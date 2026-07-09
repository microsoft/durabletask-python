# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for :class:`ExportJobEntity` against the in-memory backend.

Tests share a single backend + worker per module to avoid paying the
worker start/shutdown cost on every test.  Each test uses a unique
job ID so there is no cross-test interference.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, Optional

import pytest

from durabletask import client, entities, task, worker
from durabletask.extensions.history_export import (
    ExportDestination,
    ExportJobCreationOptions,
    ExportJobStatus,
    ExportMode,
    orchestrator_instance_id_for,
)
from durabletask.extensions.history_export.entity import (
    ENTITY_NAME,
    ExportJobEntity,
)
from durabletask.testing import create_test_backend

from ._test_helpers import wait_until
from tests.durabletask._port_utils import find_free_port

PORT = find_free_port()
HOST = f"localhost:{PORT}"

_WINDOW_START = datetime(2025, 1, 1, tzinfo=timezone.utc)
_WINDOW_END = datetime(2025, 1, 2, tzinfo=timezone.utc)


def _no_op_orchestrator(ctx: task.OrchestrationContext, _input):
    # The entity's ``create`` op schedules an orchestrator named
    # ``export_job_orchestrator``.  These tests focus on entity
    # behaviour, so register a no-op stub under that canonical name.
    return None


@pytest.fixture(scope="module")
def backend():
    b = create_test_backend(port=PORT)
    yield b
    b.stop()
    b.reset()


@pytest.fixture(scope="module")
def w(backend):
    def export_job_orchestrator(ctx: task.OrchestrationContext, _input):
        return None
    w_ = worker.TaskHubGrpcWorker(host_address=HOST)
    w_.add_entity(ExportJobEntity, name=ENTITY_NAME)
    w_.add_orchestrator(export_job_orchestrator)
    w_.start()
    yield w_
    w_.stop()


@pytest.fixture(scope="module")
def c(w):
    return client.TaskHubGrpcClient(host_address=HOST)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _create_payload() -> dict:
    cfg = ExportJobCreationOptions(
        mode=ExportMode.BATCH,
        completed_time_from=_WINDOW_START,
        completed_time_to=_WINDOW_END,
        destination=ExportDestination(container="exports", prefix="run-1"),
    ).to_configuration()
    return {"config": cfg.to_dict()}


def _state_dict(metadata) -> dict:
    state = metadata.get_typed_state()
    assert isinstance(state, dict)
    return state


def _wait_for_state(
    c: client.TaskHubGrpcClient,
    entity_id: entities.EntityInstanceId,
    predicate: Callable[[dict], bool],
    *,
    description: str,
    timeout: float = 5.0,
) -> dict:
    """Poll the entity until its state satisfies *predicate*."""
    def _check() -> Optional[dict]:
        meta = c.get_entity(entity_id, include_state=True)
        if meta is None:
            return None
        state = meta.get_typed_state()
        if not isinstance(state, dict):
            return None
        return state if predicate(state) else None

    return wait_until(_check, timeout=timeout, description=description)


def _wait_for_status(
    c: client.TaskHubGrpcClient,
    entity_id: entities.EntityInstanceId,
    expected: ExportJobStatus,
    *,
    timeout: float = 5.0,
) -> dict:
    return _wait_for_state(
        c,
        entity_id,
        lambda s: s.get("status") == expected.value,
        description=f"entity {entity_id} to reach status {expected.value}",
        timeout=timeout,
    )


# ---------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------


def test_create_persists_active_status_and_schedules_orchestrator(c) -> None:
    entity_id = entities.EntityInstanceId(ENTITY_NAME, "job-1")
    c.signal_entity(entity_id, "create", input=_create_payload())

    state = _wait_for_status(c, entity_id, ExportJobStatus.ACTIVE)
    assert state["schema_version"] == "1.0"
    assert state["status"] == ExportJobStatus.ACTIVE.value
    # ``create`` schedules the driving orchestrator inline and records
    # its deterministic instance ID.
    assert state["orchestrator_instance_id"] == orchestrator_instance_id_for("job-1")
    assert state["config"]["destination"]["container"] == "exports"
    assert state["failures"] == []


def test_create_on_active_job_is_rejected_and_state_unchanged(c) -> None:
    entity_id = entities.EntityInstanceId(ENTITY_NAME, "job-1c")
    c.signal_entity(entity_id, "create", input=_create_payload())
    _wait_for_status(c, entity_id, ExportJobStatus.ACTIVE)

    c.signal_entity(entity_id, "commit_checkpoint", input={"scanned_delta": 7})
    _wait_for_state(
        c, entity_id,
        lambda s: s.get("scanned_instances") == 7,
        description="scanned_instances to reach 7",
    )

    # A second create on an ACTIVE job is rejected by the transitions
    # matrix.  The signal is one-way so we don't see the exception
    # client-side, but the state remains ACTIVE with progress intact.
    c.signal_entity(entity_id, "create", input=_create_payload())
    state = _wait_for_state(
        c, entity_id,
        lambda s: (
            s.get("status") == ExportJobStatus.ACTIVE.value
            and s.get("scanned_instances") == 7
        ),
        description="state to remain ACTIVE with scanned_instances=7",
    )
    assert state["scanned_instances"] == 7


def test_create_after_failure_revives_to_active(c) -> None:
    """Reviving a terminal job rewinds every progress field.

    Matches the .NET ``ExportJob.Create`` revive semantics: counters,
    checkpoint, ``last_checkpoint_time``, ``last_error``, and the
    accumulated ``failures`` list are all reset to a clean slate, and
    the orchestrator is re-scheduled inline.
    """
    entity_id = entities.EntityInstanceId(ENTITY_NAME, "job-1d")
    c.signal_entity(entity_id, "create", input=_create_payload())
    _wait_for_status(c, entity_id, ExportJobStatus.ACTIVE)

    # Apply enough progress and a failure so revival has something to
    # actually reset.
    c.signal_entity(
        entity_id,
        "commit_checkpoint",
        input={
            "scanned_delta": 12,
            "exported_delta": 9,
            "failed_delta": 3,
            "last_instance_key": "ts|inst-12",
            "failures": [
                {
                    "instance_id": "inst-z",
                    "reason": "timeout",
                    "attempt_count": 3,
                    "last_attempt": "2026-01-01T00:00:00+00:00",
                },
            ],
        },
    )
    pre_revive = _wait_for_state(
        c, entity_id,
        lambda s: s.get("scanned_instances") == 12,
        description="progress to land before revival",
    )
    assert pre_revive["checkpoint"]["last_instance_key"] == "ts|inst-12"
    assert pre_revive["last_checkpoint_time"] is not None
    assert len(pre_revive["failures"]) == 1

    c.signal_entity(entity_id, "mark_failed", input={"reason": "boom"})
    failed = _wait_for_status(c, entity_id, ExportJobStatus.FAILED)
    assert failed["last_error"] == "boom"

    # Revive: every progress field should reset and orchestrator
    # instance ID should be re-derived.
    c.signal_entity(entity_id, "create", input=_create_payload())
    revived = _wait_for_state(
        c, entity_id,
        lambda s: (
            s.get("status") == ExportJobStatus.ACTIVE.value
            and s.get("scanned_instances") == 0
        ),
        description="revived state to land",
    )
    assert revived["scanned_instances"] == 0
    assert revived["exported_instances"] == 0
    assert revived["failed_instances"] == 0
    assert revived["checkpoint"]["last_instance_key"] is None
    assert revived["last_checkpoint_time"] is None
    assert revived["last_error"] is None
    assert revived["failures"] == []
    assert revived["orchestrator_instance_id"] == orchestrator_instance_id_for(
        "job-1d"
    )


def test_commit_checkpoint_requires_active_status(c) -> None:
    entity_id = entities.EntityInstanceId(ENTITY_NAME, "job-2")
    c.signal_entity(entity_id, "create", input=_create_payload())
    _wait_for_status(c, entity_id, ExportJobStatus.ACTIVE)

    c.signal_entity(
        entity_id,
        "commit_checkpoint",
        input={
            "scanned_delta": 10,
            "exported_delta": 8,
            "failed_delta": 2,
            "last_instance_key": "ts|inst-9",
        },
    )
    c.signal_entity(
        entity_id,
        "commit_checkpoint",
        input={"scanned_delta": 5, "exported_delta": 5},
    )

    state = _wait_for_state(
        c, entity_id,
        lambda s: s.get("scanned_instances") == 15,
        description="scanned_instances to reach 15",
    )
    assert state["status"] == ExportJobStatus.ACTIVE.value
    assert state["scanned_instances"] == 15
    assert state["exported_instances"] == 13
    assert state["failed_instances"] == 2
    assert state["checkpoint"]["last_instance_key"] == "ts|inst-9"


def test_commit_checkpoint_records_failures_and_marks_failed(c) -> None:
    entity_id = entities.EntityInstanceId(ENTITY_NAME, "job-2b")
    c.signal_entity(entity_id, "create", input=_create_payload())
    _wait_for_status(c, entity_id, ExportJobStatus.ACTIVE)

    c.signal_entity(
        entity_id,
        "commit_checkpoint",
        input={
            "scanned_delta": 0,
            "exported_delta": 0,
            "failed_delta": 2,
            "failures": [
                {
                    "instance_id": "inst-a",
                    "reason": "timeout",
                    "attempt_count": 3,
                    "last_attempt": "2026-01-01T00:00:00+00:00",
                },
                {
                    "instance_id": "inst-b",
                    "reason": "boom",
                    "attempt_count": 3,
                    "last_attempt": "2026-01-01T00:00:00+00:00",
                },
            ],
            "mark_failed_on_batch": True,
        },
    )

    state = _wait_for_status(c, entity_id, ExportJobStatus.FAILED)
    assert state["failed_instances"] == 2
    assert len(state["failures"]) == 2
    assert "inst-a: timeout" in state["last_error"]


def test_mark_completed_sets_status(c) -> None:
    entity_id = entities.EntityInstanceId(ENTITY_NAME, "job-3")
    c.signal_entity(entity_id, "create", input=_create_payload())
    c.signal_entity(entity_id, "mark_completed")
    state = _wait_for_status(c, entity_id, ExportJobStatus.COMPLETED)
    assert state["last_error"] is None


def test_mark_failed_records_reason(c) -> None:
    entity_id = entities.EntityInstanceId(ENTITY_NAME, "job-4")
    c.signal_entity(entity_id, "create", input=_create_payload())
    _wait_for_status(c, entity_id, ExportJobStatus.ACTIVE)
    c.signal_entity(entity_id, "mark_failed", input={"reason": "boom"})
    state = _wait_for_status(c, entity_id, ExportJobStatus.FAILED)
    assert state["last_error"] == "boom"
