# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for :func:`export_job_orchestrator`.

Tests share a single backend per module to keep total runtime low.
The "context bound" tests use their own worker because they need a
fresh registration; the happy-path and cancellation tests share a
module-scoped worker.
"""

from __future__ import annotations

import gzip
import json
import threading
from datetime import datetime, timedelta, timezone

import pytest

from durabletask import client, task, worker
from durabletask.extensions.history_export import (
    ExportDestination,
    ExportFormat,
    ExportFormatKind,
    ExportHistoryClient,
    ExportJobCreationOptions,
    ExportJobStatus,
    ExportMode,
)
from durabletask.extensions.history_export.activities import clear_context
from durabletask.extensions.history_export import orchestrator as orch_mod
from durabletask.testing import create_test_backend

from tests.durabletask.extensions.history_export._test_helpers import wait_until


PORT = 50262
HOST = f"localhost:{PORT}"


class _InMemoryWriter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.blobs: dict[str, dict] = {}

    def write(self, *, instance_id, blob_name, payload, content_type, content_encoding):
        with self._lock:
            self.blobs[blob_name] = {
                "instance_id": instance_id,
                "payload": payload,
                "content_type": content_type,
                "content_encoding": content_encoding,
            }


def _echo(ctx: task.OrchestrationContext, input):
    return input


@pytest.fixture(scope="module", autouse=True)
def _retry_overrides():
    # Tighten retry/idle timings so tests don't sleep for minutes.
    orch_mod._BATCH_RETRY_BACKOFF_OVERRIDE = timedelta(milliseconds=100)
    orch_mod._CONTINUOUS_IDLE_DELAY_OVERRIDE = timedelta(milliseconds=200)
    try:
        yield
    finally:
        orch_mod._BATCH_RETRY_BACKOFF_OVERRIDE = None
        orch_mod._CONTINUOUS_IDLE_DELAY_OVERRIDE = None


@pytest.fixture(scope="module")
def backend():
    b = create_test_backend(port=PORT)
    yield b
    b.stop()
    b.reset()


@pytest.fixture(scope="module")
def writer() -> _InMemoryWriter:
    return _InMemoryWriter()


@pytest.fixture(scope="module")
def dt_client(backend):
    return client.TaskHubGrpcClient(host_address=HOST)


@pytest.fixture(scope="module")
def export_client(dt_client, writer):
    return ExportHistoryClient(dt_client, writer)


@pytest.fixture(scope="module")
def w(backend, export_client):
    w_ = worker.TaskHubGrpcWorker(host_address=HOST)
    w_.add_orchestrator(_echo)
    export_client.register_worker(w_)
    w_.start()
    yield w_
    w_.stop()


@pytest.fixture(scope="module")
def seeded_ids(dt_client, w):
    ids: list[str] = []
    for v in ["a", "b", "c", "d", "e"]:
        sid = dt_client.schedule_new_orchestration(_echo, input=v)
        state = dt_client.wait_for_orchestration_completion(sid, timeout=30)
        assert state and state.runtime_status == client.OrchestrationStatus.COMPLETED
        ids.append(sid)
    return ids


# ---------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------


def test_orchestrator_exports_all_terminal_instances_and_marks_completed(
    dt_client, export_client, writer, seeded_ids,
):
    now = datetime.now(timezone.utc)
    desc = export_client.create_job(
        ExportJobCreationOptions(
            mode=ExportMode.BATCH,
            completed_time_from=now - timedelta(hours=1),
            completed_time_to=now + timedelta(hours=1),
            destination=ExportDestination(container="exports", prefix="run-1"),
            format=ExportFormat(kind=ExportFormatKind.JSONL_GZIP),
            max_instances_per_batch=2,
        )
    )
    final = export_client.wait_for_job(desc.job_id, timeout=30, poll_interval=0.1)

    assert final.status == ExportJobStatus.COMPLETED
    assert final.exported_instances >= len(seeded_ids)
    assert final.failed_instances == 0
    assert final.failures == []

    written = {b["instance_id"] for b in writer.blobs.values()}
    for sid in seeded_ids:
        assert sid in written
    for name, entry in writer.blobs.items():
        if not name.startswith("run-1/"):
            continue
        assert name.endswith(".jsonl.gz")
        raw = gzip.decompress(entry["payload"]).decode("utf-8")
        header = json.loads(raw.strip().split("\n")[0])
        assert header["kind"] == "metadata"
        assert header["metadata"]["instance_id"] == entry["instance_id"]
        assert header["metadata"]["runtime_status"] == "COMPLETED"


def test_orchestrator_exits_when_entity_is_deleted_mid_run(
    dt_client, export_client,
):
    """Continuous-mode jobs stop when the entity is deleted externally."""
    now = datetime.now(timezone.utc)
    desc = export_client.create_job(
        ExportJobCreationOptions(
            mode=ExportMode.CONTINUOUS,
            completed_time_from=now - timedelta(hours=1),
            destination=ExportDestination(container="exports"),
            format=ExportFormat(kind=ExportFormatKind.JSON),
            max_instances_per_batch=10,
        )
    )

    # Wait for the entity to be ACTIVE (orchestrator running its loop).
    wait_until(
        lambda: (export_client.get_job(desc.job_id) or None)
        and export_client.get_job(desc.job_id).status == ExportJobStatus.ACTIVE,
        description="job to reach ACTIVE",
        timeout=5.0,
    )

    # External delete: the orchestrator's next mid-loop entity get
    # observes None and exits gracefully.
    export_client.delete_job(desc.job_id)

    run_state = dt_client.wait_for_orchestration_completion(
        desc.orchestrator_instance_id, timeout=10, fetch_payloads=True,
    )
    assert run_state is not None
    assert run_state.runtime_status == client.OrchestrationStatus.COMPLETED
    output = json.loads(run_state.serialized_output or "null")
    assert output["status"] == "Cancelled"


def test_orchestrator_records_failure_when_no_context_bound(
    dt_client, export_client,
):
    """An orchestrator that cannot reach its activity context fails the job."""
    # The shared module worker has a bound context.  Clear it so the
    # next orchestrator's activities raise, then restore it afterwards
    # so subsequent tests are unaffected.
    from durabletask.extensions.history_export.activities import (
        HistoryExportContext,
        bind_context,
    )
    clear_context()
    try:
        now = datetime.now(timezone.utc)
        desc = export_client.create_job(
            ExportJobCreationOptions(
                mode=ExportMode.BATCH,
                completed_time_from=now - timedelta(hours=1),
                completed_time_to=now + timedelta(hours=1),
                destination=ExportDestination(container="exports"),
                format=ExportFormat(kind=ExportFormatKind.JSON),
                max_instances_per_batch=10,
            )
        )
        final = export_client.wait_for_job(desc.job_id, timeout=15, poll_interval=0.1)
        assert final.status == ExportJobStatus.FAILED
        assert final.last_error is not None
    finally:
        # Re-arm the context for any subsequent tests.
        bind_context(
            HistoryExportContext(
                client=dt_client, writer=export_client.writer,
            )
        )
