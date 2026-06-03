# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for history-export activities against the in-memory backend.

Tests share a single backend + worker per module to avoid paying the
worker start/shutdown cost on every test.  The bound activity context
is module-global, so individual tests swap their own
:class:`HistoryExportContext` (writer + client) in via ``bind_context``
at the top of the test, and restore a clean slate via ``clear_context``
on teardown.
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
)
from durabletask.extensions.history_export.activities import (
    EXPORT_INSTANCE_HISTORY_ACTIVITY,
    LIST_TERMINAL_INSTANCES_ACTIVITY,
    HistoryExportContext,
    bind_context,
    clear_context,
    register as register_activities,
)
from durabletask.testing import create_test_backend


PORT = 50261
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


def _echo_orchestrator(ctx: task.OrchestrationContext, input):
    return input


def _list_then_export(ctx: task.OrchestrationContext, input):
    """Test orchestrator: list one page, then export each returned id."""
    page = yield ctx.call_activity(LIST_TERMINAL_INSTANCES_ACTIVITY, input=input["list"])
    results = []
    for instance_id in page["instance_ids"]:
        export_input = {
            "instance_id": instance_id,
            "format": input["format"],
            "destination": input["destination"],
        }
        result = yield ctx.call_activity(EXPORT_INSTANCE_HISTORY_ACTIVITY, input=export_input)
        results.append(result)
    return {"page": page, "results": results}


@pytest.fixture(scope="module")
def backend():
    b = create_test_backend(port=PORT)
    yield b
    b.stop()
    b.reset()


@pytest.fixture(scope="module")
def w(backend):
    w_ = worker.TaskHubGrpcWorker(host_address=HOST)
    w_.add_orchestrator(_echo_orchestrator)
    w_.add_orchestrator(_list_then_export)
    register_activities(w_)
    w_.start()
    yield w_
    w_.stop()


@pytest.fixture(scope="module")
def c(w):
    return client.TaskHubGrpcClient(host_address=HOST)


@pytest.fixture(autouse=True)
def _isolate_context():
    """Each test must explicitly bind its own context."""
    clear_context()
    yield
    clear_context()


@pytest.fixture(scope="module")
def seeded_ids(c, w):
    """Three completed orchestrations shared across tests."""
    ids: list[str] = []
    for value in ["a", "b", "c"]:
        sid = c.schedule_new_orchestration(_echo_orchestrator, input=value)
        state = c.wait_for_orchestration_completion(sid, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        ids.append(sid)
    return ids


# ---------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------


def test_activities_list_and_export_to_in_memory_writer(c, seeded_ids):
    writer = _InMemoryWriter()
    bind_context(HistoryExportContext(client=c, writer=writer))

    now = datetime.now(timezone.utc)
    fmt = ExportFormat(kind=ExportFormatKind.JSONL_GZIP)
    dest = ExportDestination(container="exports", prefix="run-1")
    orch_input = {
        "list": {
            "runtime_status": ["COMPLETED"],
            "completed_time_from": (now - timedelta(hours=1)).isoformat(),
            "completed_time_to": (now + timedelta(hours=1)).isoformat(),
            "page_size": 50,
            "continuation_token": None,
        },
        "format": fmt.to_dict(),
        "destination": dest.to_dict(),
    }
    run_id = c.schedule_new_orchestration(_list_then_export, input=orch_input)
    state = c.wait_for_orchestration_completion(
        run_id, timeout=30, fetch_payloads=True
    )

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED, state.failure_details
    output = json.loads(state.serialized_output or "null")
    listed_ids = output["page"]["instance_ids"]
    assert set(seeded_ids).issubset(set(listed_ids))
    assert all(r["success"] for r in output["results"]), output["results"]

    blob_names = {b["instance_id"]: name for name, b in writer.blobs.items()}
    for sid in seeded_ids:
        assert sid in blob_names
        name = blob_names[sid]
        assert name.startswith("run-1/")
        assert name.endswith(".jsonl.gz")
        entry = writer.blobs[name]
        assert entry["content_type"] == "application/x-ndjson"
        assert entry["content_encoding"] == "gzip"
        raw = gzip.decompress(entry["payload"]).decode("utf-8")
        lines = raw.strip().split("\n")
        assert len(lines) >= 2  # metadata + at least one event
        meta = json.loads(lines[0])
        assert meta["instance_id"] == sid


def test_export_activity_reports_failure_when_writer_raises(c, seeded_ids):
    class FailingWriter:
        def write(self, **_):
            raise RuntimeError("disk full")

    bind_context(HistoryExportContext(client=c, writer=FailingWriter()))

    now = datetime.now(timezone.utc)
    fmt = ExportFormat(kind=ExportFormatKind.JSON)
    dest = ExportDestination(container="exports")
    orch_input = {
        "list": {
            "runtime_status": ["COMPLETED"],
            "completed_time_from": (now - timedelta(hours=1)).isoformat(),
            "completed_time_to": (now + timedelta(hours=1)).isoformat(),
            "page_size": 50,
            "continuation_token": None,
        },
        "format": fmt.to_dict(),
        "destination": dest.to_dict(),
    }
    run_id = c.schedule_new_orchestration(_list_then_export, input=orch_input)
    state = c.wait_for_orchestration_completion(
        run_id, timeout=30, fetch_payloads=True
    )

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED, state.failure_details
    output = json.loads(state.serialized_output or "null")
    failures = [r for r in output["results"] if not r["success"]]
    assert failures, "expected at least one failure"
    assert all("disk full" in r["error"] for r in failures)


def test_activities_require_bound_context(c):
    # Do NOT bind a context.  The activities should raise.
    now = datetime.now(timezone.utc)
    fmt = ExportFormat(kind=ExportFormatKind.JSON)
    dest = ExportDestination(container="exports")
    orch_input = {
        "list": {
            "runtime_status": ["COMPLETED"],
            "completed_time_from": (now - timedelta(hours=1)).isoformat(),
            "completed_time_to": (now + timedelta(hours=1)).isoformat(),
            "page_size": 50,
            "continuation_token": None,
        },
        "format": fmt.to_dict(),
        "destination": dest.to_dict(),
    }
    run_id = c.schedule_new_orchestration(_list_then_export, input=orch_input)
    state = c.wait_for_orchestration_completion(
        run_id, timeout=30, fetch_payloads=True
    )

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.FAILED
    assert state.failure_details is not None
    assert "without a bound context" in (state.failure_details.message or "")
