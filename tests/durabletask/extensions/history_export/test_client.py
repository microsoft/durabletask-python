# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for :class:`ExportHistoryClient`.

Tests share a single backend + worker per module so the worker is
started and shut down only once per test file.  Each test uses a
unique job ID to avoid cross-test interference.
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
    ExportHistoryJobClient,
    ExportJobCreationOptions,
    ExportJobNotFoundError,
    ExportJobQuery,
    ExportJobStatus,
    ExportMode,
    orchestrator_instance_id_for,
)
from durabletask.extensions.history_export.activities import clear_context
from durabletask.testing import create_test_backend

from tests.durabletask.extensions.history_export._test_helpers import wait_until


PORT = 50263
HOST = f"localhost:{PORT}"


class _InMemoryWriter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.blobs: dict[str, dict] = {}

    def write(self, *, instance_id, container, blob_name, payload, content_type, content_encoding):
        with self._lock:
            self.blobs[blob_name] = {
                "instance_id": instance_id,
                "payload": payload,
                "content_type": content_type,
                "content_encoding": content_encoding,
            }


def _echo(ctx: task.OrchestrationContext, input):
    return input


@pytest.fixture(scope="module")
def backend():
    b = create_test_backend(port=PORT)
    yield b
    b.stop()
    b.reset()
    clear_context()


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
def seeded_terminal_instances(dt_client, w):
    """Three seeded terminal orchestrations shared across tests."""
    ids: list[str] = []
    for v in ["x", "y", "z"]:
        sid = dt_client.schedule_new_orchestration(_echo, input=v)
        state = dt_client.wait_for_orchestration_completion(sid, timeout=30)
        assert state and state.runtime_status == client.OrchestrationStatus.COMPLETED
        ids.append(sid)
    return ids


# ---------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------


def test_create_get_and_wait_for_job_end_to_end(
    dt_client, export_client, writer, seeded_terminal_instances,
):
    now = datetime.now(timezone.utc)
    desc = export_client.create_job(
        ExportJobCreationOptions(
            mode=ExportMode.BATCH,
            completed_time_from=now - timedelta(hours=1),
            completed_time_to=now + timedelta(hours=1),
            destination=ExportDestination(container="exports", prefix="run-G"),
            format=ExportFormat(kind=ExportFormatKind.JSONL_GZIP),
            max_instances_per_batch=2,
        )
    )

    assert desc.job_id
    assert desc.status == ExportJobStatus.PENDING
    assert desc.config is not None
    assert desc.orchestrator_instance_id == f"export-job-{desc.job_id}"

    final = export_client.wait_for_job(desc.job_id, timeout=30, poll_interval=0.1)

    assert final.status == ExportJobStatus.COMPLETED
    assert final.exported_instances >= len(seeded_terminal_instances)
    assert final.failed_instances == 0
    assert final.last_error is None

    matching_blobs = [
        (name, entry) for name, entry in writer.blobs.items()
        if name.startswith("run-G/")
    ]
    assert len(matching_blobs) >= len(seeded_terminal_instances)
    for name, entry in matching_blobs:
        assert name.endswith(".jsonl.gz")
        raw = gzip.decompress(entry["payload"]).decode("utf-8")
        first = json.loads(raw.strip().split("\n")[0])
        assert first["kind"] == "metadata"


def test_get_job_returns_none_for_unknown_id(export_client):
    assert export_client.get_job("does-not-exist") is None


def test_wait_for_job_raises_lookup_when_job_never_exists(export_client):
    with pytest.raises(ExportJobNotFoundError):
        export_client.wait_for_job("never-created", timeout=0.5, poll_interval=0.1)
    with pytest.raises(LookupError):
        export_client.wait_for_job("never-created", timeout=0.5, poll_interval=0.1)


def test_wait_for_job_times_out_when_status_stays_active(
    dt_client, writer, monkeypatch,
):
    local_client = ExportHistoryClient(dt_client, writer)
    fake_desc = ExportJobCreationOptions(
        mode=ExportMode.BATCH,
        completed_time_from=datetime.now(timezone.utc) - timedelta(hours=1),
        completed_time_to=datetime.now(timezone.utc) + timedelta(hours=1),
        destination=ExportDestination(container="c"),
        format=ExportFormat(kind=ExportFormatKind.JSON),
    ).to_configuration()

    from durabletask.extensions.history_export.models import ExportJobDescription

    def fake_get_job(self, job_id):
        return ExportJobDescription(
            job_id=job_id,
            status=ExportJobStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            last_modified_at=datetime.now(timezone.utc),
            config=fake_desc,
            orchestrator_instance_id="orch-1",
            scanned_instances=0,
            exported_instances=0,
            failed_instances=0,
            last_error=None,
            checkpoint=None,
            last_checkpoint_time=None,
        )

    monkeypatch.setattr(ExportHistoryClient, "get_job", fake_get_job)

    with pytest.raises(TimeoutError):
        local_client.wait_for_job("stuck-job", timeout=0.3, poll_interval=0.05)


def test_delete_job_clears_entity_state(dt_client, export_client):
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
    assert final.status == ExportJobStatus.COMPLETED

    export_client.delete_job(desc.job_id)
    wait_until(
        lambda: export_client.get_job(desc.job_id) is None,
        description="job to disappear after delete",
        timeout=5.0,
    )


def test_list_jobs_returns_created_jobs_and_supports_status_filter(export_client):
    now = datetime.now(timezone.utc)
    completed_ids: list[str] = []
    for _ in range(3):
        d = export_client.create_job(
            ExportJobCreationOptions(
                mode=ExportMode.BATCH,
                completed_time_from=now - timedelta(hours=1),
                completed_time_to=now + timedelta(hours=1),
                destination=ExportDestination(container="exports"),
                format=ExportFormat(kind=ExportFormatKind.JSON),
                max_instances_per_batch=10,
            )
        )
        export_client.wait_for_job(d.job_id, timeout=15, poll_interval=0.1)
        completed_ids.append(d.job_id)

    all_jobs = list(export_client.list_jobs())
    seen = {j.job_id for j in all_jobs}
    for jid in completed_ids:
        assert jid in seen

    completed_only = list(
        export_client.list_jobs(
            ExportJobQuery(status=[ExportJobStatus.COMPLETED])
        )
    )
    assert {j.job_id for j in completed_only} >= set(completed_ids)
    assert all(j.status == ExportJobStatus.COMPLETED for j in completed_only)

    failed_only = list(
        export_client.list_jobs(
            ExportJobQuery(status=[ExportJobStatus.FAILED])
        )
    )
    assert all(j.status == ExportJobStatus.FAILED for j in failed_only)


def test_export_history_job_client_round_trip(export_client):
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
    job_client = export_client.get_job_client(desc.job_id)
    assert isinstance(job_client, ExportHistoryJobClient)
    assert job_client.job_id == desc.job_id
    assert job_client.orchestrator_instance_id == orchestrator_instance_id_for(
        desc.job_id
    )

    final = job_client.wait(timeout=15, poll_interval=0.1)
    assert final.status == ExportJobStatus.COMPLETED

    snap = job_client.describe()
    assert snap is not None
    assert snap.status == ExportJobStatus.COMPLETED

    job_client.delete()
    wait_until(
        lambda: job_client.describe() is None,
        description="job to disappear after delete",
        timeout=5.0,
    )


def test_export_history_job_client_rejects_empty_job_id(export_client):
    with pytest.raises(ValueError):
        export_client.get_job_client("")
