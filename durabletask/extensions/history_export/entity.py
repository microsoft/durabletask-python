# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Durable entity that owns the state of a single export job.

The entity persists its state through the SDK's default JSON encoder.
To avoid embedding Python type metadata in the persisted payload (a
known deserialization-attack vector), the on-disk shape is owned by
:class:`~durabletask.extensions.history_export.models.ExportJobState`,
a versioned dataclass whose ``to_dict`` / ``from_dict`` methods
produce and consume pure JSON primitives keyed by literal field names
plus an explicit ``schema_version``.

Operations
----------
``create``
    Initialise a fresh export job, or revive a terminal job, by
    persisting :attr:`ExportJobStatus.ACTIVE` and scheduling the
    driving orchestrator inline (with a deterministic instance ID
    derived from the job ID).  Refuses to overwrite an active job
    (raises :class:`ExportJobInvalidTransitionError`).  Mirrors the
    .NET ``ExportJob.Create`` flow, so a single signal is enough to
    launch a job.
``get``
    Returns the persisted state dict, or ``None`` if the entity has
    not been created (or has been deleted).
``commit_checkpoint``
    Applies an incremental update after a single export page.  When
    ``mark_failed_on_batch`` is true *and* ``failures`` is non-empty,
    transitions the job to :class:`ExportJobStatus.FAILED` and
    records the failure summary as ``last_error``.
``mark_completed``
    Transitions the job to :class:`ExportJobStatus.COMPLETED`.
``mark_failed``
    Transitions the job to :class:`ExportJobStatus.FAILED` and
    records ``payload["reason"]`` as the terminal error.
``delete``
    Clears all entity state.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, cast

from durabletask import entities
from durabletask import worker as worker_module

from durabletask.extensions.history_export._constants import (
    ENTITY_NAME,
    ORCHESTRATOR_INSTANCE_ID_PREFIX,
    ORCHESTRATOR_NAME,
    orchestrator_instance_id_for,
)
from durabletask.extensions.history_export._internal import dt_from_iso
from durabletask.extensions.history_export._logging import logger
from durabletask.extensions.history_export.models import (
    ExportFailure,
    ExportJobConfiguration,
    ExportJobState,
    ExportJobStatus,
)
from durabletask.extensions.history_export.transitions import (
    assert_valid_transition,
)


__all__ = [
    "ENTITY_NAME",
    "ORCHESTRATOR_INSTANCE_ID_PREFIX",
    "ExportJobEntity",
    "orchestrator_instance_id_for",
    "register",
]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _summarize_failures(failures: list[ExportFailure], *, limit: int = 10) -> str:
    if not failures:
        return ""
    head = "; ".join(f"{f.instance_id}: {f.reason}" for f in failures[:limit])
    if len(failures) > limit:
        head += f"; ... and {len(failures) - limit} more failures"
    return head


class ExportJobEntity(entities.DurableEntity):
    """Durable entity that owns the lifecycle state of one export job."""

    # ----- operation names ------------------------------------------
    #
    # Single source of truth for the wire-level entity operation
    # names.  Clients, the orchestrator, and the transitions matrix
    # all import these so a typo in any one call site is impossible.
    # Mirrors the .NET ``nameof(this.Create)`` pattern.

    OP_CREATE = "create"
    OP_GET = "get"
    OP_COMMIT_CHECKPOINT = "commit_checkpoint"
    OP_MARK_COMPLETED = "mark_completed"
    OP_MARK_FAILED = "mark_failed"
    OP_DELETE = "delete"

    # ----- state helpers --------------------------------------------

    def _load(self) -> ExportJobState | None:
        raw = self.get_state()
        if raw is None:
            return None
        if not isinstance(raw, dict):
            raise TypeError(
                f"Unexpected entity state type {type(raw).__name__!r}; expected dict"
            )
        return ExportJobState.from_dict(cast("dict[str, Any]", raw))

    def _save(self, state: ExportJobState) -> dict[str, Any]:
        state.last_modified_at = _utcnow()
        persisted = state.to_dict()
        self.set_state(persisted)
        return persisted

    def _current_status(self) -> ExportJobStatus | None:
        state = self._load()
        return state.status if state is not None else None

    def _job_id(self) -> str:
        return self.entity_context.entity_id.key

    # ----- operations ------------------------------------------------

    def create(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        job_id = self._job_id()
        current = self._current_status()
        assert_valid_transition(
            self.OP_CREATE, current, ExportJobStatus.ACTIVE, job_id=job_id,
        )

        config_dict = payload.get("config")
        if config_dict is None:
            raise ValueError("create payload requires 'config'")
        if not isinstance(config_dict, Mapping) or not config_dict:
            raise ValueError(
                "create payload 'config' must be a non-empty mapping"
            )
        config = ExportJobConfiguration.from_dict(
            cast("Mapping[str, Any]", config_dict),
        )

        created_at_raw = payload.get("created_at")
        created_at = dt_from_iso(created_at_raw) if created_at_raw else _utcnow()
        assert created_at is not None

        # Reviving a terminal job (COMPLETED / FAILED) constructs a
        # *fresh* ExportJobState here.  That intentionally resets
        # every progress field — ``scanned_instances``,
        # ``exported_instances``, ``failed_instances``,
        # ``checkpoint.last_instance_key``, ``last_checkpoint_time``,
        # ``last_error``, and the accumulated ``failures`` list.
        # Matches the .NET ``ExportJob.Create`` revive semantics so a
        # re-created job starts from a clean slate.
        state = ExportJobState(
            status=ExportJobStatus.ACTIVE,
            config=config,
            created_at=created_at,
            last_modified_at=created_at,
        )

        # The entity itself schedules the driving orchestrator inline,
        # so a single ``create`` signal is enough to launch a job.
        # Mirrors the .NET ``ExportJob.Create`` -> ``StartExportOrchestration``
        # flow and avoids the client having to send a second ``run``
        # signal (and the failure modes that come with it).
        instance_id = orchestrator_instance_id_for(job_id)
        try:
            self.entity_context.schedule_new_orchestration(
                ORCHESTRATOR_NAME,
                input={"job_id": job_id, "config": state.config.to_dict()},
                instance_id=instance_id,
            )
            state.orchestrator_instance_id = instance_id
            logger.info(
                "Created export job %r and scheduled orchestrator %s with "
                "instance ID %s",
                job_id, ORCHESTRATOR_NAME, instance_id,
                extra={"job_id": job_id, "operation": "create"},
            )
        except Exception as ex:  # noqa: BLE001
            # Mirror the .NET pattern: record the failure on persisted
            # state and return, rather than re-raising.  Re-raising
            # inside an entity operation can cause some entity
            # backends to discard the in-flight state mutations,
            # leaving the job with no error recorded.
            state.status = ExportJobStatus.FAILED
            state.last_error = (
                f"Failed to schedule orchestrator: {type(ex).__name__}: {ex}"
            )
            logger.exception(
                "Failed to schedule orchestrator for export job %r", job_id,
                extra={"job_id": job_id, "operation": "create"},
            )
        return self._save(state)

    def get(self, _: Any = None) -> dict[str, Any] | None:
        state = self._load()
        return state.to_dict() if state is not None else None

    def commit_checkpoint(self, payload: Mapping[str, Any]) -> dict[str, Any] | None:
        state = self._load()
        if state is None:
            raise ValueError("Cannot commit_checkpoint on uninitialized export job")
        job_id = self._job_id()

        # commit_checkpoint may transition ACTIVE -> ACTIVE (no-op) or
        # ACTIVE -> FAILED (when the orchestrator signals a persistent
        # batch failure).  The transitions matrix covers both.
        scanned_delta = int(payload.get("scanned_delta", 0))
        exported_delta = int(payload.get("exported_delta", 0))
        failed_delta = int(payload.get("failed_delta", 0))
        if scanned_delta < 0 or exported_delta < 0 or failed_delta < 0:
            raise ValueError("checkpoint deltas must be non-negative")

        failures_data: list[Mapping[str, Any]] = list(payload.get("failures") or [])
        new_failures = [ExportFailure.from_dict(f) for f in failures_data]
        will_fail = bool(payload.get("mark_failed_on_batch")) and bool(new_failures)
        target = ExportJobStatus.FAILED if will_fail else ExportJobStatus.ACTIVE
        assert_valid_transition(
            self.OP_COMMIT_CHECKPOINT, state.status, target, job_id=job_id,
        )

        state.scanned_instances += scanned_delta
        state.exported_instances += exported_delta
        state.failed_instances += failed_delta

        if "last_instance_key" in payload:
            state.checkpoint.last_instance_key = payload.get("last_instance_key")

        checkpoint_time_raw = payload.get("checkpoint_time")
        checkpoint_time = (
            dt_from_iso(checkpoint_time_raw) if checkpoint_time_raw else _utcnow()
        )
        state.last_checkpoint_time = checkpoint_time

        if new_failures:
            state.failures.extend(new_failures)

        if will_fail:
            state.status = ExportJobStatus.FAILED
            summary = _summarize_failures(new_failures)
            state.last_error = (
                f"Batch export failed after retries. Failures: {summary}"
                if summary
                else "Batch export failed after retries."
            )
            logger.warning(
                "Export job %r marked FAILED after batch retries (%d failures)",
                job_id, len(new_failures),
                extra={"job_id": job_id, "operation": "commit_checkpoint"},
            )

        return self._save(state)

    def mark_completed(self, _: Any = None) -> dict[str, Any] | None:
        state = self._load()
        if state is None:
            raise ValueError("Cannot mark_completed on uninitialized export job")
        job_id = self._job_id()
        assert_valid_transition(
            self.OP_MARK_COMPLETED, state.status, ExportJobStatus.COMPLETED,
            job_id=job_id,
        )
        state.status = ExportJobStatus.COMPLETED
        state.last_error = None
        logger.info(
            "Export job %r marked COMPLETED", job_id,
            extra={"job_id": job_id, "operation": "mark_completed"},
        )
        return self._save(state)

    def mark_failed(
        self, payload: Mapping[str, Any] | None = None
    ) -> dict[str, Any] | None:
        state = self._load()
        if state is None:
            raise ValueError("Cannot mark_failed on uninitialized export job")
        job_id = self._job_id()
        assert_valid_transition(
            self.OP_MARK_FAILED, state.status, ExportJobStatus.FAILED, job_id=job_id,
        )
        reason = ""
        if payload is not None:
            reason = str(payload.get("reason", ""))
        state.status = ExportJobStatus.FAILED
        state.last_error = reason or None
        logger.info(
            "Export job %r marked FAILED: %s", job_id, reason or "(no reason)",
            extra={"job_id": job_id, "operation": "mark_failed"},
        )
        return self._save(state)

    def delete(self, _: Any = None) -> None:  # type: ignore[override]
        # The base class's delete() calls set_state(None) which is
        # exactly what we want for export-job cleanup.  ``delete`` is
        # always valid regardless of current status.
        job_id = self._job_id()
        logger.info(
            "Export job %r deleted", job_id,
            extra={"job_id": job_id, "operation": "delete"},
        )
        super().delete()


def register(
    worker_instance: worker_module.TaskHubGrpcWorker, *, name: str = ENTITY_NAME,
) -> None:
    """Convenience helper to register :class:`ExportJobEntity` on *worker*."""
    worker_instance.add_entity(ExportJobEntity, name=name)
