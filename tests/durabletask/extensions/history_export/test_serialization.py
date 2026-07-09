# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for history-export serialization helpers."""

from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone

import pytest

from durabletask import client as client_module
from durabletask import history, task
from durabletask.extensions.history_export import ExportFormat, ExportFormatKind
from durabletask.extensions.history_export.serialization import (
    content_encoding_for,
    content_type_for,
    event_to_dict,
    file_extension_for,
    orchestration_state_to_dict,
    serialize_history,
)


def _sample_events() -> list[history.HistoryEvent]:
    ts = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    return [
        history.OrchestratorStartedEvent(event_id=-1, timestamp=ts),
        history.ExecutionStartedEvent(
            event_id=-1,
            timestamp=ts,
            name="MyOrch",
            input='"hello"',
        ),
        history.TaskScheduledEvent(
            event_id=1,
            timestamp=ts,
            name="MyActivity",
            input='42',
        ),
        history.TaskCompletedEvent(
            event_id=-1,
            timestamp=ts,
            task_scheduled_id=1,
            result='43',
        ),
        history.ExecutionCompletedEvent(
            event_id=-1,
            timestamp=ts,
            orchestration_status=1,
            result='"done"',
        ),
    ]


class TestEventToDict:
    def test_includes_type_discriminator(self) -> None:
        e = history.OrchestratorStartedEvent(
            event_id=0,
            timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        d = event_to_dict(e)
        assert d["event_type"] == "OrchestratorStartedEvent"
        assert d["event_id"] == 0
        assert d["timestamp"].startswith("2025-01-01")


class TestSerializeJson:
    def test_envelope_fields(self) -> None:
        fmt = ExportFormat(kind=ExportFormatKind.JSON)
        out = serialize_history(_sample_events(), instance_id="inst-1", fmt=fmt)
        doc = json.loads(out)
        assert doc["schema_version"] == "1.0"
        assert doc["instance_id"] == "inst-1"
        assert isinstance(doc["events"], list)
        assert len(doc["events"]) == 5
        assert doc["events"][0]["event_type"] == "OrchestratorStartedEvent"
        assert doc["events"][-1]["event_type"] == "ExecutionCompletedEvent"

    def test_deterministic(self) -> None:
        fmt = ExportFormat(kind=ExportFormatKind.JSON)
        events = _sample_events()
        a = serialize_history(events, instance_id="x", fmt=fmt)
        b = serialize_history(events, instance_id="x", fmt=fmt)
        assert a == b


class TestSerializeJsonlGzip:
    def test_decodes_to_metadata_plus_events(self) -> None:
        fmt = ExportFormat(kind=ExportFormatKind.JSONL_GZIP)
        out = serialize_history(_sample_events(), instance_id="inst-2", fmt=fmt)
        raw = gzip.decompress(out).decode("utf-8")
        lines = raw.strip().split("\n")
        assert len(lines) == 1 + 5  # metadata + events
        metadata = json.loads(lines[0])
        assert metadata == {
            "instance_id": "inst-2",
            "kind": "metadata",
            "schema_version": "1.0",
        }
        first_event = json.loads(lines[1])
        assert first_event["event_type"] == "OrchestratorStartedEvent"

    def test_deterministic(self) -> None:
        fmt = ExportFormat(kind=ExportFormatKind.JSONL_GZIP)
        events = _sample_events()
        a = serialize_history(events, instance_id="x", fmt=fmt)
        b = serialize_history(events, instance_id="x", fmt=fmt)
        assert a == b


class TestHelpers:
    def test_content_type(self) -> None:
        assert content_type_for(ExportFormat(kind=ExportFormatKind.JSON)) == "application/json"
        assert content_type_for(ExportFormat(kind=ExportFormatKind.JSONL_GZIP)) == "application/x-ndjson"

    def test_content_encoding(self) -> None:
        assert content_encoding_for(ExportFormat(kind=ExportFormatKind.JSON)) is None
        assert content_encoding_for(ExportFormat(kind=ExportFormatKind.JSONL_GZIP)) == "gzip"

    def test_file_extension(self) -> None:
        assert file_extension_for(ExportFormat(kind=ExportFormatKind.JSON)) == ".json"
        assert file_extension_for(ExportFormat(kind=ExportFormatKind.JSONL_GZIP)) == ".jsonl.gz"

    def test_unknown_kind_rejected(self) -> None:
        fmt = ExportFormat(kind=ExportFormatKind.JSON)
        fmt.kind = "bogus"  # type: ignore[assignment]
        with pytest.raises(ValueError):
            serialize_history([], instance_id="x", fmt=fmt)


class TestMetadataEmbedding:
    def _state(self) -> client_module.OrchestrationState:
        ts = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        return client_module.OrchestrationState(
            instance_id="inst-meta",
            name="MyOrch",
            runtime_status=client_module.OrchestrationStatus.COMPLETED,
            created_at=ts,
            last_updated_at=ts,
            serialized_input='"hello"',
            serialized_output='"done"',
            serialized_custom_status=None,
            failure_details=None,
        )

    def test_state_to_dict_has_no_python_type_metadata(self) -> None:
        d = orchestration_state_to_dict(self._state())
        assert "__class__" not in d and "__type__" not in d
        assert d["runtime_status"] == "COMPLETED"
        assert d["name"] == "MyOrch"
        assert d["serialized_input"] == '"hello"'
        assert d["serialized_output"] == '"done"'
        assert d["failure_details"] is None

    def test_state_with_failure_details(self) -> None:
        st = self._state()
        st.failure_details = task.FailureDetails(
            message="boom", error_type="RuntimeError", stack_trace="trace",
        )
        d = orchestration_state_to_dict(st)
        assert d["failure_details"] == {
            "message": "boom",
            "error_type": "RuntimeError",
            "stack_trace": "trace",
        }

    def test_metadata_embedded_in_json(self) -> None:
        fmt = ExportFormat(kind=ExportFormatKind.JSON)
        md = orchestration_state_to_dict(self._state())
        out = serialize_history([], instance_id="inst-meta", fmt=fmt, metadata=md)
        doc = json.loads(out)
        assert doc["metadata"]["instance_id"] == "inst-meta"
        assert doc["metadata"]["runtime_status"] == "COMPLETED"

    def test_metadata_embedded_in_jsonl_header(self) -> None:
        fmt = ExportFormat(kind=ExportFormatKind.JSONL_GZIP)
        md = orchestration_state_to_dict(self._state())
        out = serialize_history([], instance_id="inst-meta", fmt=fmt, metadata=md)
        raw = gzip.decompress(out).decode("utf-8")
        header = json.loads(raw.strip().split("\n")[0])
        assert header["kind"] == "metadata"
        assert header["metadata"]["instance_id"] == "inst-meta"
