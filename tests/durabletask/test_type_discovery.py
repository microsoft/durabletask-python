# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for annotation-based input type discovery and inbound coercion."""

import inspect
import json
import logging
from dataclasses import dataclass
from typing import Any, Optional
from unittest.mock import patch

from durabletask import entities, task, worker
from durabletask.internal import type_discovery
from durabletask.internal.entity_state_shim import StateShim
from durabletask.serialization import JsonDataConverter

TEST_LOGGER = logging.getLogger("tests")


# ----- fixtures -----


@dataclass
class Order:
    item: str
    quantity: int


class Money:
    def __init__(self, amount: int):
        self.amount = amount

    def to_json(self) -> dict[str, Any]:
        return {"amount": self.amount}

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "Money":
        return cls(data["amount"])


# ----- DataConverter.can_reconstruct -----


class TestIsReconstructable:
    """Reconstructability is now a DataConverter responsibility; the default
    JsonDataConverter recognizes dataclasses and from_json-capable types."""

    @property
    def conv(self) -> JsonDataConverter:
        return JsonDataConverter()

    def test_dataclass_is_reconstructable(self):
        assert self.conv.can_reconstruct(Order) is True

    def test_from_json_type_is_reconstructable(self):
        assert self.conv.can_reconstruct(Money) is True

    def test_builtins_are_not_reconstructable(self):
        assert self.conv.can_reconstruct(int) is False
        assert self.conv.can_reconstruct(str) is False
        assert self.conv.can_reconstruct(dict) is False

    def test_optional_dataclass_is_reconstructable(self):
        assert self.conv.can_reconstruct(Optional[Order]) is True

    def test_list_of_dataclass_is_reconstructable(self):
        assert self.conv.can_reconstruct(list[Order]) is True

    def test_list_of_builtin_is_not_reconstructable(self):
        assert self.conv.can_reconstruct(list[int]) is False


class TestCustomConverterReconstructable:
    """A custom converter can extend reconstructability to its own types, and the
    default's Optional/list recursion consults the override for element types."""

    def test_override_recognizes_custom_type_and_recurses(self):
        class Widget:
            pass

        class WidgetConverter(JsonDataConverter):
            def can_reconstruct(self, target_type: Any) -> bool:
                if isinstance(target_type, type) and issubclass(target_type, Widget):
                    return True
                return super().can_reconstruct(target_type)

        conv = WidgetConverter()
        assert conv.can_reconstruct(Widget) is True
        # The base Optional/list recursion goes through ``self``, so the override
        # is consulted for the element type.
        assert conv.can_reconstruct(list[Widget]) is True
        assert conv.can_reconstruct(Optional[Widget]) is True
        # Builtins remain excluded.
        assert conv.can_reconstruct(int) is False

    def test_discovery_uses_supplied_converter(self):
        class Widget:
            pass

        class WidgetConverter(JsonDataConverter):
            def can_reconstruct(self, target_type: Any) -> bool:
                if isinstance(target_type, type) and issubclass(target_type, Widget):
                    return True
                return super().can_reconstruct(target_type)

        def act(ctx, w: Widget):
            ...

        # The default converter does not recognize Widget...
        assert type_discovery.activity_input_type(act) is None
        # ...but the custom converter does, so discovery surfaces the type.
        assert type_discovery.activity_input_type(act, WidgetConverter()) is Widget


class TestInputTypeDiscovery:
    def test_orchestrator_input_type_dataclass(self):
        def orch(ctx, order: Order):
            ...
        assert type_discovery.orchestrator_input_type(orch) is Order

    def test_activity_input_type_dataclass(self):
        def act(ctx, order: Order):
            ...
        assert type_discovery.activity_input_type(act) is Order

    def test_input_type_builtin_returns_none(self):
        def act(ctx, value: int):
            ...
        assert type_discovery.activity_input_type(act) is None

    def test_input_type_unannotated_returns_none(self):
        def act(ctx, value):
            ...
        assert type_discovery.activity_input_type(act) is None

    def test_input_type_no_input_param_returns_none(self):
        def orch(ctx):
            ...
        assert type_discovery.orchestrator_input_type(orch) is None

    def test_postponed_annotation_resolves(self):
        # Annotation provided as a string (PEP 563 style) still resolves because
        # Order is importable in this module's globals.
        def act(ctx, order: "Order"):
            ...
        assert type_discovery.activity_input_type(act) is Order

    def test_function_entity_input_type(self):
        def counter(ctx, order: Order):
            ...
        assert type_discovery.entity_input_type(counter, "any_op") is Order

    def test_class_entity_input_type_per_operation(self):
        class Store(entities.DurableEntity):
            def add(self, order: Order):
                ...

            def clear(self):
                ...

        assert type_discovery.entity_input_type(Store, "add") is Order
        # Operation with no input parameter.
        assert type_discovery.entity_input_type(Store, "clear") is None
        # Unknown operation.
        assert type_discovery.entity_input_type(Store, "missing") is None


class TestActivityOutputTypeDiscovery:
    def test_dataclass_return_annotation(self):
        def act(ctx, _) -> Order:
            ...
        assert type_discovery.activity_output_type(act) is Order

    def test_from_json_return_annotation(self):
        def act(ctx, _) -> Money:
            ...
        assert type_discovery.activity_output_type(act) is Money

    def test_builtin_return_annotation_returns_none(self):
        def act(ctx, _) -> int:
            ...
        assert type_discovery.activity_output_type(act) is None

    def test_unannotated_return_returns_none(self):
        def act(ctx, _):
            ...
        assert type_discovery.activity_output_type(act) is None

    def test_optional_dataclass_return(self):
        def act(ctx, _) -> Optional[Order]:
            ...
        assert type_discovery.activity_output_type(act) is Optional[Order]

    def test_postponed_return_annotation_resolves(self):
        def act(ctx, _) -> "Order":
            ...
        assert type_discovery.activity_output_type(act) is Order

    def test_string_name_returns_none(self):
        assert type_discovery.activity_output_type("some_activity_name") is None


# ----- signature-structure caching -----


class _CountingConverter(JsonDataConverter):
    """Records every ``can_reconstruct`` call so per-call evaluation is visible."""

    def __init__(self):
        self.seen: list[Any] = []

    def can_reconstruct(self, target_type: Any) -> bool:
        self.seen.append(target_type)
        return super().can_reconstruct(target_type)


class TestSignatureCaching:
    """The signature *structure* is cached per callable, but the converter's
    reconstructability decision is still made on every call."""

    def test_signature_inspected_once_per_function(self):
        def act(ctx, order: Order) -> Money:
            ...

        real_signature = inspect.signature
        calls: list[Any] = []

        def counting_signature(obj, *args, **kwargs):
            calls.append(obj)
            return real_signature(obj, *args, **kwargs)

        with patch.object(inspect, "signature", counting_signature):
            assert type_discovery.activity_input_type(act) is Order
            after_first = len(calls)
            for _ in range(5):
                assert type_discovery.activity_input_type(act) is Order
            # The input and output helpers share one cached structure, so the
            # return annotation costs no additional inspection either.
            assert type_discovery.activity_output_type(act) is Money
            assert type_discovery.orchestrator_input_type(act) is Order

        assert after_first >= 1
        assert len(calls) == after_first, "signature() was re-inspected for a cached function"

    def test_entity_operation_signature_inspected_once(self):
        class Store(entities.DurableEntity):
            def add(self, order: Order):
                ...

        real_signature = inspect.signature
        calls: list[Any] = []

        def counting_signature(obj, *args, **kwargs):
            calls.append(obj)
            return real_signature(obj, *args, **kwargs)

        with patch.object(inspect, "signature", counting_signature):
            assert type_discovery.entity_input_type(Store, "add") is Order
            after_first = len(calls)
            for _ in range(5):
                assert type_discovery.entity_input_type(Store, "add") is Order

        assert after_first >= 1
        assert len(calls) == after_first

    def test_converter_decision_is_made_per_call(self):
        class Widget:
            pass

        class WidgetConverter(JsonDataConverter):
            def can_reconstruct(self, target_type: Any) -> bool:
                if isinstance(target_type, type) and issubclass(target_type, Widget):
                    return True
                return super().can_reconstruct(target_type)

        def act(ctx, w: Widget) -> Widget:
            ...

        # Prime the cache with the converter that *does* recognize Widget, then
        # switch back: the cached structure must not bake in the first answer.
        assert type_discovery.activity_input_type(act, WidgetConverter()) is Widget
        assert type_discovery.activity_input_type(act) is None
        assert type_discovery.activity_input_type(act, WidgetConverter()) is Widget

        assert type_discovery.activity_output_type(act, WidgetConverter()) is Widget
        assert type_discovery.activity_output_type(act) is None
        assert type_discovery.activity_output_type(act, WidgetConverter()) is Widget

    def test_stateful_converter_answer_is_not_cached(self):
        class Widget:
            pass

        class ToggleConverter(JsonDataConverter):
            def __init__(self):
                self.enabled = False

            def can_reconstruct(self, target_type: Any) -> bool:
                if self.enabled and target_type is Widget:
                    return True
                return super().can_reconstruct(target_type)

        def act(ctx, w: Widget):
            ...

        converter = ToggleConverter()
        assert type_discovery.activity_input_type(act, converter) is None
        converter.enabled = True
        assert type_discovery.activity_input_type(act, converter) is Widget
        converter.enabled = False
        assert type_discovery.activity_input_type(act, converter) is None

    def test_converter_is_consulted_on_every_call(self):
        def act(ctx, order: Order) -> Order:
            ...

        converter = _CountingConverter()
        for _ in range(3):
            assert type_discovery.activity_input_type(act, converter) is Order
        assert converter.seen == [Order, Order, Order]

        converter.seen.clear()
        for _ in range(3):
            assert type_discovery.activity_output_type(act, converter) is Order
        assert converter.seen == [Order, Order, Order]

    def test_unannotated_parameters_are_not_offered_to_the_converter(self):
        def act(ctx, value, *args, keyword_only: Order = None, **kwargs):
            ...

        converter = _CountingConverter()
        assert type_discovery.activity_input_type(act, converter) is None
        # *args/**kwargs and keyword-only parameters are not positional inputs,
        # and an unannotated parameter never reaches the converter at all.
        assert converter.seen == []

    def test_unhashable_callable_still_resolves(self):
        class Callable_:
            # Defining __eq__ without __hash__ makes instances unhashable, so
            # they cannot be used as a cache key.
            def __eq__(self, other):
                return self is other

            def __call__(self, ctx, order: Order) -> Order:
                ...

        act = Callable_()
        assert type_discovery.activity_input_type(act) is Order
        assert type_discovery.activity_input_type(act) is Order
        assert type_discovery.activity_output_type(act) is Order


# ----- activity executor inbound coercion -----


def _activity_executor(fn) -> tuple[worker._ActivityExecutor, str]:
    registry = worker._Registry()
    name = registry.add_activity(fn)
    return worker._ActivityExecutor(registry, TEST_LOGGER, JsonDataConverter()), name


def test_activity_input_coerced_to_dataclass():
    seen: dict[str, Any] = {}

    def handle(ctx: task.ActivityContext, order: Order):
        seen["type"] = type(order).__name__
        seen["item"] = order.item
        return order.quantity

    executor, name = _activity_executor(handle)
    result = executor.execute("orch1", name, 1, json.dumps({"item": "widget", "quantity": 3}))
    assert seen["type"] == "Order"
    assert seen["item"] == "widget"
    assert json.loads(result) == 3


def test_activity_input_coerced_via_from_json():
    seen: dict[str, Any] = {}

    def handle(ctx: task.ActivityContext, money: Money):
        seen["type"] = type(money).__name__
        seen["amount"] = money.amount
        return money.amount

    executor, name = _activity_executor(handle)
    result = executor.execute("orch1", name, 1, json.dumps({"amount": 50}))
    assert seen["type"] == "Money"
    assert seen["amount"] == 50
    assert json.loads(result) == 50


def test_activity_builtin_input_unchanged():
    seen: dict[str, Any] = {}

    def handle(ctx: task.ActivityContext, value: int):
        seen["type"] = type(value).__name__
        return value

    executor, name = _activity_executor(handle)
    executor.execute("orch1", name, 1, json.dumps(7))
    assert seen["type"] == "int"


def test_activity_input_coercion_failure_falls_back_to_raw():
    seen: dict[str, Any] = {}

    def handle(ctx: task.ActivityContext, order: Order):
        # Payload is missing the required 'quantity' field, so coercion fails and
        # the raw dict is passed through instead of raising.
        seen["type"] = type(order).__name__
        return "ok"

    executor, name = _activity_executor(handle)
    result = executor.execute("orch1", name, 1, json.dumps({"item": "widget"}))
    assert seen["type"] == "dict"
    assert json.loads(result) == "ok"


# ----- entity executor inbound coercion -----


def test_function_entity_input_coerced_to_dataclass():
    seen: dict[str, Any] = {}

    def store(ctx: entities.EntityContext, order: Order):
        seen["type"] = type(order).__name__
        seen["item"] = order.item

    registry = worker._Registry()
    registry.add_entity(store)
    executor = worker._EntityExecutor(registry, TEST_LOGGER, JsonDataConverter())
    entity_id = entities.EntityInstanceId("store", "k1")
    state = StateShim(None, JsonDataConverter())
    executor.execute("orch1", entity_id, "save", state, json.dumps({"item": "book", "quantity": 2}))
    assert seen["type"] == "Order"
    assert seen["item"] == "book"


def test_class_entity_input_coerced_per_operation():
    seen: dict[str, Any] = {}

    class Store(entities.DurableEntity):
        def save(self, order: Order):
            seen["type"] = type(order).__name__
            seen["item"] = order.item

    registry = worker._Registry()
    registry.add_entity(Store)
    executor = worker._EntityExecutor(registry, TEST_LOGGER, JsonDataConverter())
    entity_id = entities.EntityInstanceId("store", "k1")
    state = StateShim(None, JsonDataConverter())
    executor.execute("orch1", entity_id, "save", state, json.dumps({"item": "book", "quantity": 2}))
    assert seen["type"] == "Order"
    assert seen["item"] == "book"
