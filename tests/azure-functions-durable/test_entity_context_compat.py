# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from unittest.mock import MagicMock

from durabletask.entities import DurableEntity

from azure.durable_functions.internal.compat.entity_context import (
    DurableEntityContext,
    wrap_entity,
)


def _adapter(operation_input=None):
    fake_ctx = MagicMock()
    fake_ctx.entity_id.entity = "counter"
    fake_ctx.entity_id.key = "k1"
    fake_ctx.operation = "add"
    return DurableEntityContext(fake_ctx, operation_input), fake_ctx


# ---------------------------------------------------------------------------
# Adapter delegation
# ---------------------------------------------------------------------------

def test_identity_properties():
    adapter, _ = _adapter()
    assert adapter.entity_name == "counter"
    assert adapter.entity_key == "k1"
    assert adapter.operation_name == "add"
    assert adapter.is_newly_constructed is False


def test_get_input_returns_stored_input():
    adapter, _ = _adapter(5)
    assert adapter.get_input() == 5


def test_get_state_returns_existing_state_without_calling_initializer():
    adapter, fake = _adapter()
    # Existing state present: core get_state ignores the default.
    fake.get_state.side_effect = lambda intended_type, default: 42
    init = MagicMock()
    result = adapter.get_state(initializer=init, expected_type=int)
    assert result == 42
    # The initializer must not run when state already exists.
    init.assert_not_called()


def test_get_state_uses_initializer_only_when_no_state():
    adapter, fake = _adapter()
    # No state: core get_state returns the default the adapter passes.
    fake.get_state.side_effect = lambda intended_type, default: default
    result = adapter.get_state(initializer=lambda: 7, expected_type=int)
    assert result == 7


def test_get_state_without_initializer_returns_none_when_no_state():
    adapter, fake = _adapter()
    fake.get_state.side_effect = lambda intended_type, default: default
    assert adapter.get_state() is None


def test_set_state_delegates():
    adapter, fake = _adapter()
    adapter.set_state({"count": 3})
    fake.set_state.assert_called_once_with({"count": 3})


def test_destruct_on_exit_clears_state():
    adapter, fake = _adapter()
    adapter.destruct_on_exit()
    fake.set_state.assert_called_once_with(None)


# ---------------------------------------------------------------------------
# wrap_entity
# ---------------------------------------------------------------------------

def test_wrap_passes_through_two_arg_entity():
    def entity(ctx, inp):
        return None
    assert wrap_entity(entity) is entity


def test_wrap_passes_through_class_based_entity():
    class Counter(DurableEntity):
        def add(self, amount):
            return amount
    assert wrap_entity(Counter) is Counter


def test_wrap_adapts_one_arg_entity_with_set_result():
    seen = {}

    def counter_entity(context):
        seen["op"] = context.operation_name
        seen["input"] = context.get_input()
        current = context.get_state(initializer=lambda: 0)
        context.set_state(current + context.get_input())
        context.set_result(current + context.get_input())

    wrapped = wrap_entity(counter_entity)
    assert wrapped is not counter_entity

    fake_ctx = MagicMock()
    fake_ctx.entity_id.entity = "counter"
    fake_ctx.entity_id.key = "k1"
    fake_ctx.operation = "add"
    fake_ctx.get_state.return_value = 10

    result = wrapped(fake_ctx, 5)
    assert result == 15
    assert seen["op"] == "add"
    assert seen["input"] == 5
    fake_ctx.set_state.assert_called_once_with(15)


def test_wrap_adapts_one_arg_entity_falls_back_to_return_value():
    def entity(context):
        return "returned"

    wrapped = wrap_entity(entity)
    fake_ctx = MagicMock()
    assert wrapped(fake_ctx, None) == "returned"


def test_wrap_uses_explicit_none_result_over_return_value():
    # v1 treats an explicit set_result(None) as a valid result; it must take
    # precedence over the function's return value rather than being treated as
    # "unset".
    def entity(context):
        context.set_result(None)
        return "should-not-be-used"

    wrapped = wrap_entity(entity)
    fake_ctx = MagicMock()
    assert wrapped(fake_ctx, None) is None


def test_wrap_preserves_entity_name():
    def my_entity(context):
        return None
    assert wrap_entity(my_entity).__name__ == "my_entity"


def test_wrap_preserves_durable_entity_name():
    def entity_fn(context):
        return None
    entity_fn.__durable_entity_name__ = "CustomName"
    wrapped = wrap_entity(entity_fn)
    assert wrapped.__durable_entity_name__ == "CustomName"
