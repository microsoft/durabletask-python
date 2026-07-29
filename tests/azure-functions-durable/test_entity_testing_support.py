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

    assert outcome.result == 15
    assert outcome.state == 15
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

    assert outcome.result == {
        "entity": "customprobe",
        "key": "probe-1",
        "operation": "describe",
        "input": "value",
    }
    assert outcome.state is None


def test_execute_class_entity_supports_state_and_inherited_delete():
    class Counter(DurableEntity):
        def add(self, amount: Any = None) -> int:
            value = self.get_state(int, 0) + amount
            self.set_state(value)
            return value

    added = execute_entity(Counter, "add", input=3, state=4)
    deleted = execute_entity(Counter, "delete", state=added.state)

    assert added.result == 7
    assert added.state == 7
    assert deleted.result is None
    assert deleted.state is None


def test_execute_class_entity_calls_optional_input_method_without_argument():
    class Counter(DurableEntity):
        def reset(self) -> str:
            self.set_state(0)
            return "reset"

    outcome = execute_entity(Counter, "reset", state=5)

    assert outcome.result == "reset"
    assert outcome.state == 0


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

    assert outcome.result == "orchestration-1"
    assert outcome.actions == (
        EntitySignalAction(
            entity_id=EntityInstanceId("counter", "target"),
            operation="add",
            input=2,
            scheduled_time=signal_time,
        ),
        OrchestrationStartAction(
            name="process",
            instance_id="orchestration-1",
            input={"source": "target"},
        ),
    )


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

    assert outcome.result == CustomPayload(5)
    assert outcome.state == CustomPayload(2)
    assert outcome.actions == (
        EntitySignalAction(
            entity_id=EntityInstanceId("target", "one"),
            operation="accept",
            input=CustomPayload(3),
        ),
        OrchestrationStartAction(
            name="process-payload",
            instance_id="orchestration-1",
            input=CustomPayload(4),
        ),
    )


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

    assert outcome.result == CustomPayload(10)
    assert outcome.state == CustomPayload(1)
    assert outcome.actions == (
        EntitySignalAction(
            entity_id=EntityInstanceId("target", "one"),
            operation="accept",
            input=CustomPayload(2),
        ),
        OrchestrationStartAction(
            name="process-payload",
            instance_id="orchestration-1",
            input=CustomPayload(3),
        ),
    )


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

    assert outcome.result == 10
    assert outcome.state == 10


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
