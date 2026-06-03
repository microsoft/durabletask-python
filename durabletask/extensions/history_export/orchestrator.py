# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Orchestrator that drives one export job from start to completion.

Mirrors the .NET ``ExportJobOrchestrator`` design:

1.  Resolve the job configuration from the orchestrator input.
2.  Loop over pages of terminal instance IDs:
    a.  Ask ``list_terminal_instances`` for one page.
    b.  Fan out ``export_instance_history`` across the page,
        respecting the configured ``max_parallel_exports`` cap, with
        a per-activity retry policy.
    c.  If any individual export still failed after its retries,
        retry the *whole page* once after a backoff timer.
    d.  Signal the entity with ``commit_checkpoint`` carrying the
        page totals.  On persistent batch failure, the signal also
        carries the failure list and ``mark_failed_on_batch=True``.
3.  Continue-as-new every ``CONTINUE_AS_NEW_FREQUENCY`` pages to keep
    the orchestrator history bounded.
4.  Signal ``mark_completed`` (or ``mark_failed`` on any uncaught
    exception) and return a summary.

The orchestrator never reads the entity's state back during normal
operation — except for the lightweight ``call_entity("get")`` at the
top of every loop iteration which lets external delete / mark_failed
signals cancel the orchestrator cleanly.  This keeps the orchestrator
history small and avoids round-trip latency.
"""

from __future__ import annotations

from collections.abc import Generator, Mapping
from datetime import timedelta
from typing import Any, TypedDict, cast

from durabletask import task
from durabletask import worker as worker_module

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
from durabletask.extensions.history_export.entity import ExportJobEntity
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
_BATCH_RETRY_BACKOFF_OVERRIDE: timedelta | None = None
_CONTINUOUS_IDLE_DELAY_OVERRIDE: timedelta | None = None


class _ExportActivityResult(TypedDict):
    """Shape of the dict returned by ``export_instance_history``."""

    instance_id: str
    success: bool
    error: str | None


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


def export_job_orchestrator(
    ctx: task.OrchestrationContext, input: Mapping[str, Any],
) -> Generator[Any, Any, Any]:
    """Drive a single export job through the page → fan-out → checkpoint loop.

    Input schema::

        {
            "job_id":           str,
            "config":           ExportJobConfiguration.to_dict(),
            "checkpoint":       ExportCheckpoint.to_dict() (optional),
            "processed_cycles": int (optional, used for continue-as-new),
        }
    """
    job_id = str(input["job_id"])
    config_input = input["config"]
    if not isinstance(config_input, Mapping):
        raise TypeError("config input must be a mapping")
    config_mapping = cast("Mapping[str, Any]", config_input)
    config = ExportJobConfiguration.from_dict(config_mapping)
    initial_checkpoint = input.get("checkpoint") or {"last_instance_key": None}
    processed_cycles = int(input.get("processed_cycles", 0))

    entity_id = task.EntityInstanceId(ENTITY_NAME, job_id)
    runtime_status_names = [s.name for s in config.filter.effective_runtime_status()]
    continuation_token: str | None = initial_checkpoint.get("last_instance_key")

    totals: dict[str, int] = {"scanned": 0, "exported": 0, "failed": 0}

    try:
        while True:
            processed_cycles += 1
            if processed_cycles > CONTINUE_AS_NEW_FREQUENCY:
                ctx.continue_as_new({
                    "job_id": job_id,
                    "config": dict(config_mapping),
                    "checkpoint": {"last_instance_key": continuation_token},
                    "processed_cycles": 0,
                })
                return None

            # Step 1: re-check the entity's view of the world.  This
            # lets external state changes (delete, mark_failed) cancel
            # the orchestrator without us having to drain a backlog.
            current_state: dict[str, Any] | None = (
                yield ctx.call_entity(entity_id, ExportJobEntity.OP_GET)
            )
            if current_state is None:
                logger.info(
                    "Export job %r entity has been deleted; exiting orchestrator",
                    job_id,
                )
                return {"job_id": job_id, "status": "Cancelled", "totals": totals}
            current_status = current_state.get("status")
            if current_status != ExportJobStatus.ACTIVE.value:
                logger.info(
                    "Export job %r entity status is %s; exiting orchestrator",
                    job_id, current_status,
                )
                return {
                    "job_id": job_id,
                    "status": current_status,
                    "totals": totals,
                }

            list_input = build_list_activity_input(
                runtime_status_names=runtime_status_names,
                completed_time_from=config.filter.completed_time_from,
                completed_time_to=config.filter.completed_time_to,
                page_size=config.max_instances_per_batch,
                continuation_token=continuation_token,
            )
            page: dict[str, Any] = yield ctx.call_activity(
                LIST_TERMINAL_INSTANCES_ACTIVITY, input=list_input
            )

            raw_ids: list[Any] = list(page.get("instance_ids") or [])
            instance_ids: list[str] = [str(x) for x in raw_ids]
            scanned_delta = len(instance_ids)
            exported_delta = 0
            failed_delta = 0
            batch_failures: list[dict[str, Any]] = []

            # Empty page handling matches the .NET ExportJobOrchestrator:
            # CONTINUOUS sleeps and re-polls, BATCH exits cleanly even
            # if the backend returned a non-null continuation token.
            # This guards against backends that legally return an empty
            # page with a token (the orchestrator would otherwise spin
            # forever in BATCH mode emitting no-op commit_checkpoints).
            if not instance_ids:
                if config.mode is ExportMode.CONTINUOUS:
                    yield ctx.create_timer(
                        ctx.current_utc_datetime + _continuous_idle_delay()
                    )
                    continuation_token = None
                    continue
                ctx.signal_entity(
                    entity_id,
                    ExportJobEntity.OP_COMMIT_CHECKPOINT,
                    {
                        "scanned_delta": 0,
                        "exported_delta": 0,
                        "failed_delta": 0,
                        "last_instance_key": None,
                    },
                )
                break

            # The page has at least one instance: fan out exports.
            batch_succeeded = False
            results: list[_ExportActivityResult] = []
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
                    ExportJobEntity.OP_COMMIT_CHECKPOINT,
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
                # The entity already transitioned to FAILED via
                # the commit_checkpoint signal above; returning
                # cleanly avoids the outer ``except`` issuing a
                # second mark_failed signal which the transitions
                # matrix would reject (the entity is no longer
                # ACTIVE).  Surfacing the cause is the caller's
                # responsibility via :meth:`ExportHistoryClient.get_job`,
                # whose ``last_error`` carries the failure summary.
                logger.warning(
                    "Export job %r marked FAILED after %d batch attempts; "
                    "%d instances failed to export",
                    job_id, MAX_BATCH_RETRY_ATTEMPTS, failed_delta,
                )
                return {
                    "job_id": job_id,
                    "status": ExportJobStatus.FAILED.value,
                    "totals": totals,
                }

            next_token_raw = page.get("continuation_token")
            next_token: str | None = (
                str(next_token_raw) if next_token_raw is not None else None
            )
            ctx.signal_entity(
                entity_id,
                ExportJobEntity.OP_COMMIT_CHECKPOINT,
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
        ctx.signal_entity(entity_id, ExportJobEntity.OP_MARK_COMPLETED)
        return {"job_id": job_id, "status": "Completed", "totals": totals}

    except Exception as ex:  # noqa: BLE001 - reported back via mark_failed
        ctx.signal_entity(
            entity_id,
            ExportJobEntity.OP_MARK_FAILED,
            {"reason": f"{type(ex).__name__}: {ex}"},
        )
        raise


def _run_page(
    ctx: task.OrchestrationContext,
    *,
    instance_ids: list[str],
    config: ExportJobConfiguration,
    max_parallel: int,
) -> Generator[Any, Any, list[_ExportActivityResult]]:
    """Fan out export activities for a single page, bounded by *max_parallel*."""
    destination = config.destination.to_dict()
    fmt = config.format.to_dict()

    results: list[_ExportActivityResult] = []
    for start in range(0, len(instance_ids), max_parallel):
        chunk = instance_ids[start:start + max_parallel]
        chunk_tasks: list[task.Task[_ExportActivityResult]] = [
            cast(
                "task.Task[_ExportActivityResult]",
                ctx.call_activity(
                    EXPORT_INSTANCE_HISTORY_ACTIVITY,
                    input={
                        "instance_id": instance_id,
                        "format": fmt,
                        "destination": destination,
                    },
                    retry_policy=EXPORT_ACTIVITY_RETRY_POLICY,
                ),
            )
            for instance_id in chunk
        ]
        chunk_results: list[_ExportActivityResult] = yield task.when_all(chunk_tasks)
        results.extend(chunk_results)
    return results


def register(worker_instance: worker_module.TaskHubGrpcWorker) -> None:
    """Convenience helper to register the orchestrator on *worker*."""
    worker_instance.add_orchestrator(export_job_orchestrator)
