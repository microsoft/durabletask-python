# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the _EntityExecutor class in durabletask.worker."""
import logging

from durabletask import entities
from durabletask.internal.entity_state_shim import StateShim
from durabletask.worker import _EntityExecutor, _Registry


def _make_executor(*entity_args) -> _EntityExecutor:
    """Helper to create an _EntityExecutor with registered entities."""
    registry = _Registry()
    for entity in entity_args:
        registry.add_entity(entity)
    return _EntityExecutor(registry, logging.getLogger("test"))


def _execute(executor, entity_name, operation, encoded_input=None):
    """Helper to execute an entity operation."""
    entity_id = entities.EntityInstanceId(entity_name, "test-key")
    state = StateShim(None)
    return executor.execute("test-orchestration", entity_id, operation, state, encoded_input)


class TestClassBasedEntityMethodDispatch:
    """Tests for class-based entity method dispatch in _EntityExecutor."""

    def test_method_with_no_input_parameter(self):
        """Methods that don't accept input should work without _=None."""
        class Counter(entities.DurableEntity):
            def get(self):
                return self.get_state(int, 0)

        executor = _make_executor(Counter)
        result = _execute(executor, "Counter", "get")
        assert result == "0"

    def test_method_with_input_parameter(self):
        """Methods that accept input should receive entity_input."""
        class Counter(entities.DurableEntity):
            def set(self, value: int):
                self.set_state(value)

        executor = _make_executor(Counter)
        result = _execute(executor, "Counter", "set", "42")
        assert result is None

    def test_method_with_input_returns_value(self):
        """Methods that accept input and return a value."""
        class Counter(entities.DurableEntity):
            def add(self, value: int):
                current = self.get_state(int, 0)
                new_value = current + value
                self.set_state(new_value)
                return new_value

        executor = _make_executor(Counter)
        result = _execute(executor, "Counter", "add", "5")
        assert result == "5"

    def test_mix_of_methods_with_and_without_input(self):
        """An entity with both input and no-input methods should work."""
        class Counter(entities.DurableEntity):
            def set(self, value: int):
                self.set_state(value)

            def get(self):
                return self.get_state(int, 0)

        executor = _make_executor(Counter)
        entity_id = entities.EntityInstanceId("Counter", "test-key")

        # set requires input
        state = StateShim(None)
        executor.execute("test-orch", entity_id, "set", state, "10")
        state.commit()

        # get does not require input — reuse state to simulate persistence
        result = executor.execute("test-orch", entity_id, "get", state, None)
        assert result == "10"

    def test_method_with_optional_parameter_uses_default(self):
        """Methods with default parameters should use defaults when no input is provided."""
        class Counter(entities.DurableEntity):
            def add(self, value: int = 1):
                current = self.get_state(int, 0)
                new_value = current + value
                self.set_state(new_value)
                return new_value

        executor = _make_executor(Counter)

        # No input provided — should use default value of 1
        result = _execute(executor, "Counter", "add")
        assert result == "1"

    def test_method_with_optional_parameter_uses_provided_input(self):
        """Methods with default parameters should use provided input when given."""
        class Counter(entities.DurableEntity):
            def add(self, value: int = 1):
                current = self.get_state(int, 0)
                new_value = current + value
                self.set_state(new_value)
                return new_value

        executor = _make_executor(Counter)

        # Input provided — should use it instead of default
        result = _execute(executor, "Counter", "add", "5")
        assert result == "5"


class TestFunctionBasedEntityDispatch:
    """Tests for function-based entity dispatch in _EntityExecutor."""

    def test_function_entity_receives_context_and_input(self):
        """Function-based entities always receive (ctx, input)."""
        def counter(ctx: entities.EntityContext, input):
            if ctx.operation == "get":
                return ctx.get_state(int, 0)
            elif ctx.operation == "set":
                ctx.set_state(input)

        executor = _make_executor(counter)
        entity_id = entities.EntityInstanceId("counter", "test-key")
        state = StateShim(None)

        executor.execute("test-orch", entity_id, "set", state, "42")
        state.commit()

        result = executor.execute("test-orch", entity_id, "get", state, None)
        assert result == "42"


class TestStateShimCoercion:
    """Tests for StateShim.get_state type coercion via the data converter."""

    def test_get_state_none_returns_default(self):
        state = StateShim(None)
        assert state.get_state(int, 0) == 0

    def test_get_state_none_without_default_returns_none(self):
        state = StateShim(None)
        assert state.get_state(int) is None

    def test_get_state_passes_through_matching_type(self):
        state = StateShim(5)
        assert state.get_state(int) == 5

    def test_get_state_constructor_coercion(self):
        state = StateShim("5")
        assert state.get_state(int) == 5

    def test_get_state_coerces_dataclass(self):
        from dataclasses import dataclass

        @dataclass
        class Counter:
            value: int

        # State is stored as a plain dict (as it would be after from_json).
        state = StateShim({"value": 7})
        result = state.get_state(Counter)
        assert isinstance(result, Counter)
        assert result.value == 7

    def test_get_state_uses_from_json_hook(self):
        class Wrapped:
            def __init__(self, n: int):
                self.n = n

            @classmethod
            def from_json(cls, data):
                return cls(data["n"])

        state = StateShim({"n": 3})
        result = state.get_state(Wrapped)
        assert isinstance(result, Wrapped)
        assert result.n == 3

    def test_get_state_invalid_coercion_raises(self):
        # An explicit intended_type that the state cannot be coerced to raises,
        # restoring the pre-existing strict contract for entity state access.
        import pytest

        state = StateShim("not-an-int")
        with pytest.raises(TypeError):
            state.get_state(int)
