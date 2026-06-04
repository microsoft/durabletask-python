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

    def write(self, *, instance_id, container, blob_name, payload, content_type, content_encoding, metadata=None):
        with self._lock:
            self.blobs[blob_name] = {
                "instance_id": instance_id,
                "payload": payload,
                "content_type": content_type,
                "content_encoding": content_encoding,
                "metadata": dict(metadata) if metadata else None,
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
        # N-9: activity must pass {"instance_id": ...} blob metadata
        # to the writer so downstream consumers can scan a container
        # without parsing each blob body.
        assert entry["metadata"] == {"instance_id": sid}
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


# ---------------------------------------------------------------------
# Unit tests for the N-2 guard
# ---------------------------------------------------------------------
#
# The activity body refuses to write a blob when the target instance
# either no longer exists (e.g. purged between list and export) or has
# re-entered a non-terminal state.  Exercising this via the full
# orchestrator would require fabricating a race against the in-memory
# backend; calling the activity body directly with a stub client lets
# us cover the guard deterministically.


class _StubGetStateClient:
    """Minimal stand-in for ``TaskHubGrpcClient`` covering N-2 paths."""

    def __init__(self, *, state):
        self._state = state
        self.history_calls = 0

    def get_orchestration_state(self, instance_id, *, fetch_payloads=False):
        del instance_id, fetch_payloads
        return self._state

    def get_orchestration_history(self, instance_id):
        del instance_id
        self.history_calls += 1
        return []


class _CountingWriter:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def write(self, **kwargs):
        self.calls.append(kwargs)


def _basic_activity_input() -> dict:
    return {
        "instance_id": "inst-x",
        "format": ExportFormat(kind=ExportFormatKind.JSON).to_dict(),
        "destination": ExportDestination(container="exports").to_dict(),
    }


def test_export_activity_skips_when_instance_no_longer_exists():
    """N-2: instance purged between list and export -> failure without write."""
    from durabletask.extensions.history_export.activities import (
        export_instance_history,
    )

    stub_client = _StubGetStateClient(state=None)
    writer = _CountingWriter()
    bind_context(HistoryExportContext(client=stub_client, writer=writer))

    result = export_instance_history(None, _basic_activity_input())

    assert result["success"] is False
    assert "no longer exists" in result["error"]
    assert writer.calls == []
    assert stub_client.history_calls == 0


def test_export_activity_skips_when_instance_is_not_terminal():
    """N-2: instance has re-entered a running state -> failure without write."""
    from durabletask.extensions.history_export.activities import (
        export_instance_history,
    )

    class _State:
        runtime_status = client.OrchestrationStatus.RUNNING

    stub_client = _StubGetStateClient(state=_State())
    writer = _CountingWriter()
    bind_context(HistoryExportContext(client=stub_client, writer=writer))

    result = export_instance_history(None, _basic_activity_input())

    assert result["success"] is False
    assert "no longer terminal" in result["error"]
    assert "RUNNING" in result["error"]


# ---------------------------------------------------------------------
# Unit tests for the N-8 blob-naming scheme
# ---------------------------------------------------------------------


def test_blob_name_matches_dotnet_hash_scheme():
    """N-8: blob name is lowercase-hex sha256 of '{:O}|{instance_id}'.

    Pins the exact .NET-aligned scheme so any future drift (timestamp
    format, hash function, casing) breaks loudly.  The expected hash
    is computed by hand from the same inputs the activity would use.
    """
    import hashlib

    from durabletask.extensions.history_export.activities import (
        _blob_name_for,
        _dotnet_o_format,
    )

    last_updated = datetime(2026, 6, 4, 17, 9, 9, 420990, tzinfo=timezone.utc)
    instance_id = "inst-1"
    fmt = ExportFormat(kind=ExportFormatKind.JSON)

    # The seven-digit fractional-seconds format is what .NET emits for
    # the same instant.
    assert _dotnet_o_format(last_updated) == "2026-06-04T17:09:09.4209900+00:00"

    expected_hash = hashlib.sha256(
        f"{_dotnet_o_format(last_updated)}|{instance_id}".encode("utf-8")
    ).hexdigest()

    assert _blob_name_for(
        instance_id=instance_id,
        last_updated_at=last_updated,
        prefix=None,
        fmt=fmt,
    ) == f"{expected_hash}.json"

    assert _blob_name_for(
        instance_id=instance_id,
        last_updated_at=last_updated,
        prefix="exports/run-1/",
        fmt=ExportFormat(kind=ExportFormatKind.JSONL_GZIP),
    ) == f"exports/run-1/{expected_hash}.jsonl.gz"


def test_blob_name_isolates_instance_ids_that_differ_only_by_slash():
    """N-8: instance IDs containing '/' no longer collide.

    The old scheme used ``instance_id.replace(\"/\", \"_\")`` which
    collapsed ``v1/x`` and ``v1_x`` to the same blob name.  Hashing
    isolates them.
    """
    from durabletask.extensions.history_export.activities import _blob_name_for

    last_updated = datetime(2026, 6, 4, 17, 9, 9, 420990, tzinfo=timezone.utc)
    fmt = ExportFormat(kind=ExportFormatKind.JSON)

    name_a = _blob_name_for(
        instance_id="v1/x", last_updated_at=last_updated, prefix=None, fmt=fmt,
    )
    name_b = _blob_name_for(
        instance_id="v1_x", last_updated_at=last_updated, prefix=None, fmt=fmt,
    )
    assert name_a != name_b


def test_blob_name_changes_when_instance_terminal_timestamp_changes():
    """N-8: re-export at a different terminal time lands at a new blob."""
    from durabletask.extensions.history_export.activities import _blob_name_for

    fmt = ExportFormat(kind=ExportFormatKind.JSON)
    instance_id = "inst-x"

    earlier = datetime(2026, 6, 4, 17, 0, 0, 0, tzinfo=timezone.utc)
    later = datetime(2026, 6, 4, 18, 0, 0, 0, tzinfo=timezone.utc)

    name_earlier = _blob_name_for(
        instance_id=instance_id, last_updated_at=earlier, prefix=None, fmt=fmt,
    )
    name_later = _blob_name_for(
        instance_id=instance_id, last_updated_at=later, prefix=None, fmt=fmt,
    )
    assert name_earlier != name_later
