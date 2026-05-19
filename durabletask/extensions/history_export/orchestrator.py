# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Orchestrator that drives one export job from start to completion.

Mirrors the .NET ``ExportJobOrchestrator`` design:

1.  Re-fetch the export-job entity state at the top of every loop
    iteration via :meth:`OrchestrationContext.call_entity`.  If the
    job no longer exists (deleted) or is no longer ACTIVE (externally
    marked failed/completed), the orchestrator exits cleanly without
    issuing any further signals.
2.  Ask ``list_terminal_instances`` for one page.
3.  Fan out ``export_instance_history`` across the page, respecting
    the configured ``max_parallel_exports`` cap, with a per-activity
    retry policy.
4.  If any individual export still failed after its retries, retry
    the *whole page* up to ``MAX_BATCH_RETRY_ATTEMPTS`` times with
    exponential backoff.
5.  Signal the entity with ``commit_checkpoint`` carrying the page
    totals.  On persistent batch failure, the signal also carries the
    failure list and ``mark_failed_on_batch=True``.
6.  In :attr:`ExportMode.BATCH`, break out of the loop when there is
    no next page.  In :attr:`ExportMode.CONTINUOUS`, sleep for
    ``CONTINUOUS_IDLE_DELAY`` on empty pages and continue tailing
    forever (until an external stop is observed via step 1).
7.  Continue-as-new every ``CONTINUE_AS_NEW_FREQUENCY`` pages to
    keep the orchestrator history bounded.
8.  In BATCH mode only: on a clean exit, signal ``mark_completed``.
    In CONTINUOUS mode, the orchestrator does not mark the job
    completed — the job lifecycle is owned by the caller.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, List, Mapping, Optional

from durabletask import task

from durabletask.extensions.history_export._constants import (
    ENTITY_NAME,
    ORCHESTRATOR_NAME,
)
from durabletask.extensions.history_export._logging import logger
from durabletask.extensions.history_export.activities import (
    EXPORT_INSTANCE_HISTORY_ACTIVITY,
    LIST_TERMINAL_INSTANCES_ACTIVITY,
    build_list_activity_input,
)
from durabletask.extensions.history_export.models import (
    ExportJobConfiguration,
    ExportJobStatus,
    ExportMode,
)


__all__ = [
    "CONTINUE_AS_NEW_FREQUENCY",
    "CONTINUOUS_IDLE_DELAY",
    "EXPORT_ACTIVITY_RETRY_POLICY",
    "MAX_BATCH_RETRY_ATTEMPTS",
    "ORCHESTRATOR_NAME",
    "export_job_orchestrator",
    "register",
]


# Per-activity retry policy applied to ``export_instance_history``.
# Mirrors the .NET defaults (3 attempts, 15s/30s/60s backoff).
EXPORT_ACTIVITY_RETRY_POLICY = task.RetryPolicy(
    first_retry_interval=timedelta(seconds=15),
    max_number_of_attempts=3,
    backoff_coefficient=2.0,
    max_retry_interval=timedelta(seconds=60),
)

# Number of *page cycles* the orchestrator processes before issuing
# continue-as-new to bound its own history.
CONTINUE_AS_NEW_FREQUENCY = 5

# Number of times to retry a whole batch (a page worth of exports) if
# any individual export ultimately fails.
MAX_BATCH_RETRY_ATTEMPTS = 3

# Default sleep between empty pages in CONTINUOUS mode.
CONTINUOUS_IDLE_DELAY = timedelta(minutes=1)

# Default backoff between batch retries.  Tests override this via
# :data:`_BATCH_RETRY_BACKOFF_OVERRIDE` to keep runtimes short.
_DEFAULT_BATCH_RETRY_FIRST = timedelta(seconds=60)
_DEFAULT_BATCH_RETRY_MAX = timedelta(seconds=300)

# Test seams: monkey-patch to small values to keep test runs fast.
_BATCH_RETRY_BACKOFF_OVERRIDE: Optional[timedelta] = None
_CONTINUOUS_IDLE_DELAY_OVERRIDE: Optional[timedelta] = None


def _batch_retry_delay(attempt: int) -> timedelta:
    if _BATCH_RETRY_BACKOFF_OVERRIDE is not None:
        return _BATCH_RETRY_BACKOFF_OVERRIDE
    seconds = min(
        int(_DEFAULT_BATCH_RETRY_FIRST.total_seconds() * (2 ** (attempt - 1))),
        int(_DEFAULT_BATCH_RETRY_MAX.total_seconds()),
    )
    return timedelta(seconds=seconds)


def _continuous_idle_delay() -> timedelta:
    return _CONTINUOUS_IDLE_DELAY_OVERRIDE or CONTINUOUS_IDLE_DELAY


def export_job_orchestrator(ctx: task.OrchestrationContext, input: Mapping[str, Any]):
    """Drive a single export job through the page → fan-out → checkpoint loop.

    Input schema::

        {
            "job_id":           str,
            "config":           ExportJobConfiguration._to_dict(),
            "checkpoint":       ExportCheckpoint._to_dict() (optional),
            "processed_cycles": int (optional, used for continue-as-new),
        }
    """
    job_id = input["job_id"]
    config = ExportJobConfiguration._from_dict(input["config"])
    initial_checkpoint = input.get("checkpoint") or {"last_instance_key": None}
    processed_cycles = int(input.get("processed_cycles", 0))

    entity_id = task.EntityInstanceId(ENTITY_NAME, job_id)
    runtime_status_names = [s.name for s in config.filter.effective_runtime_status()]
    continuation_token = initial_checkpoint.get("last_instance_key")

    totals = {"scanned": 0, "exported": 0, "failed": 0}

    try:
        while True:
            processed_cycles += 1
            if processed_cycles > CONTINUE_AS_NEW_FREQUENCY:
                ctx.continue_as_new({
                    "job_id": job_id,
                    "config": input["config"],
                    "checkpoint": {"last_instance_key": continuation_token},
                    "processed_cycles": 0,
                })
                return None

            # Step 1: re-check the entity's view of the world.  This
            # lets external state changes (delete, mark_failed) cancel
            # the orchestrator without us having to drain a backlog.
            current_state = yield ctx.call_entity(entity_id, "get")
            if current_state is None:
                logger.info(
                    "Export job %r entity has been deleted; exiting orchestrator",
                    job_id,
                )
                return {"job_id": job_id, "status": "Cancelled", "totals": totals}
            if current_state.get("status") != ExportJobStatus.ACTIVE.value:
                logger.info(
                    "Export job %r entity status is %s; exiting orchestrator",
                    job_id, current_state.get("status"),
                )
                return {
                    "job_id": job_id,
                    "status": current_state.get("status"),
                    "totals": totals,
                }

            list_input = build_list_activity_input(
                runtime_status_names=runtime_status_names,
                completed_time_from=config.filter.completed_time_from,
                completed_time_to=config.filter.completed_time_to,
                page_size=config.max_instances_per_batch,
                continuation_token=continuation_token,
            )
            page = yield ctx.call_activity(
                LIST_TERMINAL_INSTANCES_ACTIVITY, input=list_input
            )

            instance_ids = page.get("instance_ids") or []
            scanned_delta = len(instance_ids)
            exported_delta = 0
            failed_delta = 0
            batch_failures: List[dict] = []

            if instance_ids:
                batch_succeeded = False
                results: List[dict] = []
                for attempt in range(1, MAX_BATCH_RETRY_ATTEMPTS + 1):
                    results = yield from _run_page(
                        ctx,
                        instance_ids=instance_ids,
                        config=config,
                        max_parallel=config.max_parallel_exports,
                    )
                    failed_results = [r for r in results if not r.get("success")]
                    if not failed_results:
                        batch_succeeded = True
                        break
                    if attempt < MAX_BATCH_RETRY_ATTEMPTS:
                        delay = _batch_retry_delay(attempt)
                        yield ctx.create_timer(ctx.current_utc_datetime + delay)

                exported_delta = sum(1 for r in results if r.get("success"))
                failed_delta = sum(1 for r in results if not r.get("success"))
                batch_failures = [
                    {
                        "instance_id": r["instance_id"],
                        "reason": r.get("error") or "Unknown error",
                        "attempt_count": MAX_BATCH_RETRY_ATTEMPTS,
                        "last_attempt": ctx.current_utc_datetime.isoformat(),
                    }
                    for r in results
                    if not r.get("success")
                ]

                if not batch_succeeded:
                    ctx.signal_entity(
                        entity_id,
                        "commit_checkpoint",
                        {
                            "scanned_delta": 0,
                            "exported_delta": 0,
                            "failed_delta": failed_delta,
                            "failures": batch_failures,
                            "mark_failed_on_batch": True,
                        },
                    )
                    totals["scanned"] += scanned_delta
                    totals["exported"] += exported_delta
                    totals["failed"] += failed_delta
                    raise RuntimeError(
                        f"Export job '{job_id}' batch failed after "
                        f"{MAX_BATCH_RETRY_ATTEMPTS} attempts; "
                        f"{failed_delta} instances could not be exported."
                    )

            next_token = page.get("continuation_token")
            ctx.signal_entity(
                entity_id,
                "commit_checkpoint",
                {
                    "scanned_delta": scanned_delta,
                    "exported_delta": exported_delta,
                    "failed_delta": failed_delta,
                    "last_instance_key": next_token,
                },
            )

            totals["scanned"] += scanned_delta
            totals["exported"] += exported_delta
            totals["failed"] += failed_delta

            if not next_token:
                if config.mode is ExportMode.CONTINUOUS:
                    # Tail mode: sleep, then loop back and re-check.
                    yield ctx.create_timer(
                        ctx.current_utc_datetime + _continuous_idle_delay()
                    )
                    continuation_token = None
                    continue
                break

            continuation_token = next_token

        # Reaching here means BATCH mode finished its window cleanly.
        ctx.signal_entity(entity_id, "mark_completed")
        return {"job_id": job_id, "status": "Completed", "totals": totals}

    except Exception as ex:  # noqa: BLE001 - reported back via mark_failed
        ctx.signal_entity(
            entity_id,
            "mark_failed",
            {"reason": f"{type(ex).__name__}: {ex}"},
        )
        raise


def _run_page(ctx, *, instance_ids, config, max_parallel):
    """Fan out export activities for a single page, bounded by *max_parallel*."""
    destination = config.destination._to_dict()
    fmt = config.format._to_dict()

    results: List[dict] = []
    for start in range(0, len(instance_ids), max_parallel):
        chunk = instance_ids[start:start + max_parallel]
        chunk_tasks = [
            ctx.call_activity(
                EXPORT_INSTANCE_HISTORY_ACTIVITY,
                input={
                    "instance_id": instance_id,
                    "format": fmt,
                    "destination": destination,
                },
                retry_policy=EXPORT_ACTIVITY_RETRY_POLICY,
            )
            for instance_id in chunk
        ]
        chunk_results = yield task.when_all(chunk_tasks)
        results.extend(chunk_results)
    return results


def register(worker_instance) -> None:
    """Convenience helper to register the orchestrator on *worker*."""
    worker_instance.add_orchestrator(export_job_orchestrator)
