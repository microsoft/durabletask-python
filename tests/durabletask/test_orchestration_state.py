# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for typed accessors on client.OrchestrationState."""

import json
from dataclasses import dataclass
from datetime import datetime, timezone

from durabletask.client import OrchestrationState, OrchestrationStatus


@dataclass
class Result:
    message: str
    count: int


class Money:
    def __init__(self, amount: int):
        self.amount = amount

    def to_json(self) -> dict:
        return {"amount": self.amount}

    @classmethod
    def from_json(cls, data: dict) -> "Money":
        return cls(data["amount"])


def _state(serialized_input=None, serialized_output=None, serialized_custom_status=None) -> OrchestrationState:
    now = datetime.now(timezone.utc)
    return OrchestrationState(
        instance_id="abc123",
        name="test",
        runtime_status=OrchestrationStatus.COMPLETED,
        created_at=now,
        last_updated_at=now,
        serialized_input=serialized_input,
        serialized_output=serialized_output,
        serialized_custom_status=serialized_custom_status,
        failure_details=None,
    )


# ----- get_output -----


def test_get_output_none_returns_none():
    assert _state(serialized_output=None).get_output() is None
    assert _state(serialized_output=None).get_output(Result) is None


def test_get_output_raw_without_type():
    state = _state(serialized_output=json.dumps({"message": "hi", "count": 2}))
    assert state.get_output() == {"message": "hi", "count": 2}


def test_get_output_coerced_to_dataclass():
    state = _state(serialized_output=json.dumps({"message": "hi", "count": 2}))
    result = state.get_output(Result)
    assert isinstance(result, Result)
    assert result == Result("hi", 2)


def test_get_output_uses_from_json_hook():
    state = _state(serialized_output=json.dumps({"amount": 50}))
    result = state.get_output(Money)
    assert isinstance(result, Money)
    assert result.amount == 50


def test_get_output_primitive():
    assert _state(serialized_output=json.dumps(42)).get_output(int) == 42


# ----- get_input -----


def test_get_input_none_returns_none():
    assert _state(serialized_input=None).get_input(Result) is None


def test_get_input_coerced_to_dataclass():
    state = _state(serialized_input=json.dumps({"message": "in", "count": 1}))
    result = state.get_input(Result)
    assert isinstance(result, Result)
    assert result == Result("in", 1)


# ----- get_custom_status -----


def test_get_custom_status_none_returns_none():
    assert _state(serialized_custom_status=None).get_custom_status(Result) is None


def test_get_custom_status_raw_without_type():
    state = _state(serialized_custom_status=json.dumps({"phase": "step1"}))
    assert state.get_custom_status() == {"phase": "step1"}


def test_get_custom_status_coerced_to_dataclass():
    state = _state(serialized_custom_status=json.dumps({"message": "s", "count": 9}))
    result = state.get_custom_status(Result)
    assert isinstance(result, Result)
    assert result == Result("s", 9)
