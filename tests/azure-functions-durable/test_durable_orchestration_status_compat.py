# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the ``DurableOrchestrationStatus`` compat wrapper.

Focuses on the JSON (de)serialization surface used by the v1 client shims:
``from_json`` reconstruction, ``to_json`` emission of raw payloads, and the
falsy/empty behaviour for a non-existent instance.
"""

from datetime import datetime, timezone
from types import SimpleNamespace

import azure.durable_functions as df
from azure.durable_functions.internal.compat.durable_orchestration_status import (
    DurableOrchestrationStatus,
)


def _sample_json():
    return {
        "name": "orch",
        "instanceId": "abc",
        "createdTime": "2026-01-01T00:00:00.000000Z",
        "lastUpdatedTime": "2026-01-02T00:00:00.000000Z",
        "input": {"in": 1},
        "output": {"out": 2},
        "customStatus": "cs",
        "runtimeStatus": "Running",
    }


# ---------------------------------------------------------------------------
# from_json
# ---------------------------------------------------------------------------

def test_from_json_reconstructs_attribute_surface():
    status = DurableOrchestrationStatus.from_json(_sample_json())
    assert bool(status) is True
    assert status.name == "orch"
    assert status.instance_id == "abc"
    assert status.created_time == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert status.last_updated_time == datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert status.input_ == {"in": 1}
    assert status.output == {"out": 2}
    assert status.custom_status == "cs"
    assert status.runtime_status == df.OrchestrationRuntimeStatus.Running


def test_from_json_accepts_json_string():
    import json
    status = DurableOrchestrationStatus.from_json(json.dumps(_sample_json()))
    assert status.instance_id == "abc"
    assert status.runtime_status == df.OrchestrationRuntimeStatus.Running


def test_from_json_without_runtime_status_returns_none():
    # When the source JSON omits runtimeStatus, the wrapped state has no status;
    # runtime_status must return None rather than raising.
    status = DurableOrchestrationStatus.from_json({"instanceId": "abc"})
    assert status.instance_id == "abc"
    assert status.runtime_status is None
    assert "runtimeStatus" not in status.to_json()


def test_from_json_to_json_round_trip():
    original = _sample_json()
    restored = DurableOrchestrationStatus.from_json(original).to_json()
    assert restored == original


# ---------------------------------------------------------------------------
# Empty / non-existent instance
# ---------------------------------------------------------------------------

def test_empty_status_is_falsy_with_none_attributes():
    status = DurableOrchestrationStatus(None)
    assert bool(status) is False
    assert status.name is None
    assert status.instance_id is None
    assert status.created_time is None
    assert status.last_updated_time is None
    assert status.input_ is None
    assert status.output is None
    assert status.custom_status is None
    assert status.runtime_status is None
    assert status.orchestration_state is None
    assert status.to_json() == {}


def test_history_events_round_trip():
    source = _sample_json()
    source["historyEvents"] = [
        {
            "EventType": "ExecutionCompleted",
            "Result": {"done": True},
        },
    ]
    status = DurableOrchestrationStatus.from_json(source)
    assert status.history == source["historyEvents"]
    assert status.to_json() == source


def test_legacy_history_field_is_accepted():
    status = DurableOrchestrationStatus.from_json({
        **_sample_json(),
        "history": [{"EventType": "ExecutionStarted"}],
    })
    assert status.history == [{"EventType": "ExecutionStarted"}]
    assert status.to_json()["historyEvents"] == status.history


# ---------------------------------------------------------------------------
# _raw_payload
# ---------------------------------------------------------------------------

def test_raw_payload_parses_json():
    assert DurableOrchestrationStatus._raw_payload('{"a": 1}') == {"a": 1}


def test_raw_payload_returns_original_string_when_not_json():
    assert DurableOrchestrationStatus._raw_payload("not json") == "not json"


def test_raw_payload_none_returns_none():
    assert DurableOrchestrationStatus._raw_payload(None) is None


# ---------------------------------------------------------------------------
# Payload properties tolerate non-JSON (failed instances)
# ---------------------------------------------------------------------------

def test_payload_properties_parse_json_when_valid():
    state = SimpleNamespace(
        serialized_input='{"in": 1}',
        serialized_output='{"out": 2}',
        serialized_custom_status='"cs"',
    )
    status = DurableOrchestrationStatus(state)
    assert status.input_ == {"in": 1}
    assert status.output == {"out": 2}
    assert status.custom_status == "cs"


def test_payload_properties_return_raw_string_on_failed_instance():
    # A failed orchestration's serialized_output is a plain error string, not
    # JSON. Reading .output/.input_/.custom_status must fall back to the raw
    # string (matching to_json) instead of raising JSONDecodeError.
    state = SimpleNamespace(
        serialized_input="not json input",
        serialized_output="Exception: boom",
        serialized_custom_status="raw status",
    )
    status = DurableOrchestrationStatus(state)
    assert status.input_ == "not json input"
    assert status.output == "Exception: boom"
    assert status.custom_status == "raw status"
