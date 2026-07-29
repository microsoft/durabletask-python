# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for host-free durable entity execution."""

from datetime import datetime, timezone
from typing import Any

import pytest

import azure.durable_functions as df
from azure.durable_functions.testing import (
    EntitySignalAction,
    OrchestrationStartAction,
    execute_entity,
)
from durabletask.entities import DurableEntity, EntityContext, EntityInstanceId


class CustomPayload:
    def __init__(self, value: int):
        self.value = value

    def to_json(self) -> dict[str, int]:
        return {"value": self.value}

    @classmethod
    def from_json(cls, value: dict[str, int]) -> "CustomPayload":
        return cls(value["value"])

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, CustomPayload)
            and self.value == other.value
        )


def test_execute_v1_entity_returns_result_and_state():
    def counter(context: df.DurableEntityContext) -> None:
        current = context.get_state(initializer=lambda: 0)
        current += context.get_input()
        context.set_state(current)
        context.set_result(current)

    outcome = execute_entity(
        counter, "add", input=5, state=10, entity_key="counter-1")

    assert outcome.get_result() == 15
    assert outcome.get_state() == 15
    assert outcome.actions == ()


def test_execute_native_function_exposes_identity():
    def probe(context: EntityContext, input: Any = None) -> dict[str, str]:
        return {
            "entity": context.entity_id.entity,
            "key": context.entity_id.key,
            "operation": context.operation,
            "input": input,
        }

    outcome = execute_entity(
        probe,
        "describe",
        input="value",
        entity_name="CustomProbe",
        entity_key="probe-1",
    )

    assert outcome.get_result() == {
        "entity": "customprobe",
        "key": "probe-1",
        "operation": "describe",
        "input": "value",
    }
    assert outcome.get_state() is None


def test_execute_class_entity_supports_state_and_inherited_delete():
    class Counter(DurableEntity):
        def add(self, amount: Any = None) -> int:
            value = self.get_state(int, 0) + amount
            self.set_state(value)
            return value

    added = execute_entity(Counter, "add", input=3, state=4)
    deleted = execute_entity(Counter, "delete", state=added.get_state())

    assert added.get_result() == 7
    assert added.get_state() == 7
    assert deleted.get_result() is None
    assert deleted.get_state() is None


def test_execute_class_entity_calls_optional_input_method_without_argument():
    class Counter(DurableEntity):
        def reset(self) -> str:
            self.set_state(0)
            return "reset"

    outcome = execute_entity(Counter, "reset", state=5)

    assert outcome.get_result() == "reset"
    assert outcome.get_state() == 0


def test_execute_entity_returns_typed_actions():
    signal_time = datetime(2030, 1, 2, 3, 4, tzinfo=timezone.utc)

    class Relay(DurableEntity):
        def dispatch(self, input: dict[str, Any]) -> str:
            self.signal_entity(
                EntityInstanceId("Counter", input["key"]),
                "add",
                input["amount"],
                signal_time=signal_time,
            )
            return self.schedule_new_orchestration(
                "process",
                input={"source": input["key"]},
                instance_id="orchestration-1",
            )

    outcome = execute_entity(
        Relay, "dispatch", input={"key": "target", "amount": 2})

    assert outcome.get_result() == "orchestration-1"
    signal = outcome.actions[0]
    assert isinstance(signal, EntitySignalAction)
    assert signal.entity_id == EntityInstanceId("counter", "target")
    assert signal.operation == "add"
    assert signal.get_input() == 2
    assert signal.scheduled_time == signal_time

    start = outcome.actions[1]
    assert isinstance(start, OrchestrationStartAction)
    assert start.name == "process"
    assert start.instance_id == "orchestration-1"
    assert start.get_input() == {"source": "target"}


def test_execute_entity_preserves_custom_payloads_in_strict_mode(
        monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "1")

    class PayloadEntity(DurableEntity):
        def process(self, input: CustomPayload) -> CustomPayload:
            state = CustomPayload(input.value + 1)
            self.set_state(state)
            self.signal_entity(
                EntityInstanceId("target", "one"),
                "accept",
                CustomPayload(input.value + 2),
            )
            self.schedule_new_orchestration(
                "process-payload",
                input=CustomPayload(input.value + 3),
                instance_id="orchestration-1",
            )
            return CustomPayload(input.value + 4)

    outcome = execute_entity(
        PayloadEntity, "process", input=CustomPayload(1))

    assert outcome.get_result(expected_type=CustomPayload) == CustomPayload(5)
    assert outcome.get_state(expected_type=CustomPayload) == CustomPayload(2)
    assert outcome.actions[0].get_input(
        expected_type=CustomPayload) == CustomPayload(3)
    assert outcome.actions[1].get_input(
        expected_type=CustomPayload) == CustomPayload(4)


def test_execute_entity_snapshots_mutable_state_and_actions_in_strict_mode(
        monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "1")

    class MutatingEntity(DurableEntity):
        def mutate(self) -> CustomPayload:
            state = CustomPayload(1)
            signal_input = CustomPayload(2)
            orchestration_input = CustomPayload(3)

            self.set_state(state)
            self.signal_entity(
                EntityInstanceId("target", "one"),
                "accept",
                signal_input,
            )
            self.schedule_new_orchestration(
                "process-payload",
                input=orchestration_input,
                instance_id="orchestration-1",
            )

            state.value = 10
            signal_input.value = 20
            orchestration_input.value = 30
            return state

    outcome = execute_entity(MutatingEntity, "mutate")

    assert outcome.get_result(
        expected_type=CustomPayload) == CustomPayload(10)
    assert outcome.get_state(
        expected_type=CustomPayload) == CustomPayload(1)
    assert outcome.actions[0].get_input(
        expected_type=CustomPayload) == CustomPayload(2)
    assert outcome.actions[1].get_input(
        expected_type=CustomPayload) == CustomPayload(3)


def test_execute_entity_returns_wire_shape_without_expected_type_in_strict_mode(
        monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "1")

    class TupleEntity(DurableEntity):
        def process(self) -> tuple[int, int]:
            value = (1, 2)
            self.set_state(value)
            self.signal_entity(
                EntityInstanceId("target", "one"), "accept", value)
            self.schedule_new_orchestration(
                "process-tuple", input=value, instance_id="orchestration-1")
            return value

    outcome = execute_entity(TupleEntity, "process")

    assert outcome.get_result() == [1, 2]
    assert outcome.get_state() == [1, 2]
    assert outcome.actions[0].get_input() == [1, 2]
    assert outcome.actions[1].get_input() == [1, 2]


def test_execute_entity_supports_decorated_function_handle():
    app = df.DFApp()

    @app.entity_trigger(  # pyright: ignore[reportArgumentType]
        context_name="context")
    def accumulator(context: df.DurableEntityContext) -> None:
        value = context.get_state(initializer=lambda: 0)
        context.set_state(value + context.get_input())
        context.set_result(value + context.get_input())

    entity_function = accumulator.build().get_user_function().entity_function  # pyright: ignore[reportFunctionMemberAccess]
    outcome = execute_entity(entity_function, "add", input=4, state=6)

    assert outcome.get_result() == 10
    assert outcome.get_state() == 10


def test_execute_entity_propagates_operation_failure():
    class FailingEntity(DurableEntity):
        def fail(self) -> None:
            self.set_state("not persisted")
            raise RuntimeError("operation failed")

    with pytest.raises(RuntimeError, match="operation failed"):
        execute_entity(FailingEntity, "fail", state="original")


def test_execute_entity_rejects_missing_class_operation():
    class Counter(DurableEntity):
        pass

    with pytest.raises(
            AttributeError,
            match="does not have operation 'missing'"):
        execute_entity(Counter, "missing")
