# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the DataConverter abstraction and the default JsonDataConverter."""

import inspect
import json
import logging
from dataclasses import dataclass, field
from functools import wraps
from typing import Any
from unittest.mock import patch

import pytest
from durabletask import task
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.internal.entity_state_shim import StateShim
from durabletask.serialization import (
    DEFAULT_DATA_CONVERTER,
    DataConverter,
    JsonDataConverter,
)
from durabletask.internal import type_discovery
from durabletask.worker import _ActivityExecutor, _OrchestrationExecutor, _Registry


@dataclass
class Order:
    item: str
    quantity: int


@dataclass(frozen=True)
class PricedOrder:
    quantity: int
    unit_price: int
    total: int = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "total", self.quantity * self.unit_price)


@dataclass
class Shipment:
    order: PricedOrder


def test_round_trip_dataclass_with_derived_field():
    converter = JsonDataConverter()
    order = PricedOrder(3, 10)
    encoded = converter.serialize(order)
    assert json.loads(encoded) == {"quantity": 3, "unit_price": 10, "total": 30}
    assert converter.deserialize(encoded, PricedOrder) == order


def test_coerce_dataclass_recomputes_derived_field():
    converter = JsonDataConverter()
    result = converter.coerce({"quantity": 3, "unit_price": 10, "total": 999}, PricedOrder)
    assert result == PricedOrder(3, 10)
    assert result.total == 30


def test_round_trip_nested_dataclass_with_derived_field():
    converter = JsonDataConverter()
    shipment = Shipment(PricedOrder(3, 10))
    assert converter.deserialize(converter.serialize(shipment), Shipment) == shipment


@dataclass
class CounterState:
    name: str
    counter: int = field(init=False, default=0)

    def __init__(self, name: str, counter: int = 0):
        self.name = name
        self.counter = counter


@dataclass
class KeywordCounterState:
    name: str
    counter: int = field(init=False, default=0)

    def __init__(self, name: str, *, counter: int = 0):
        self.name = name
        self.counter = counter


@dataclass
class KwargsCounterState:
    name: str
    counter: int = field(init=False, default=0)

    def __init__(self, name: str, **kwargs):
        self.name = name
        self.counter = kwargs.get("counter", 0)


@pytest.mark.parametrize("state_type", [CounterState, KeywordCounterState, KwargsCounterState])
def test_non_init_field_preserved_by_custom_constructor(state_type):
    converter = JsonDataConverter()
    original = state_type("persisted", counter=42)
    encoded = converter.serialize(original)
    restored = converter.deserialize(encoded, state_type)
    assert restored == original
    assert restored.counter == 42

    state = StateShim(encoded, converter, is_serialized=True)
    restored_state = state.get_state(state_type)
    assert restored_state.counter == 42
    state.set_state(restored_state)
    assert json.loads(state.encode_state()) == {"name": "persisted", "counter": 42}


@pytest.mark.parametrize("accept_kwargs", [False, True])
def test_non_init_field_preserved_by_decorated_initializer(accept_kwargs):
    @dataclass
    class DecoratedCounterState:
        name: str
        counter: int = field(init=False, default=0)

    generated_init = DecoratedCounterState.__init__
    if accept_kwargs:
        @wraps(generated_init)
        def restore_counter(self, *args, **kwargs):
            counter = kwargs.pop("counter", 0)
            generated_init(self, *args, **kwargs)
            self.counter = counter
    else:
        @wraps(generated_init)
        def restore_counter(self, *args, counter=0, **kwargs):
            generated_init(self, *args, **kwargs)
            self.counter = counter
    DecoratedCounterState.__init__ = restore_counter

    converter = JsonDataConverter()
    original = DecoratedCounterState("persisted", counter=42)
    encoded = converter.serialize(original)
    assert converter.deserialize(encoded, DecoratedCounterState).counter == 42

    state = StateShim(encoded, converter, is_serialized=True)
    restored = state.get_state(DecoratedCounterState)
    assert restored.counter == 42
    state.set_state(restored)
    assert json.loads(state.encode_state()) == {"name": "persisted", "counter": 42}


@pytest.mark.parametrize("accept_kwargs", [False, True])
def test_non_init_custom_constructor_field_is_recursively_coerced(accept_kwargs):
    @dataclass
    class CustomShipment:
        order: Order = field(init=False)

    if accept_kwargs:
        def initializer(self, **kwargs):
            self.order = kwargs["order"]
    else:
        def initializer(self, *, order):
            self.order = order
    CustomShipment.__init__ = initializer

    converter = JsonDataConverter()
    restored = converter.deserialize('{"order": {"item": "book", "quantity": 42}}', CustomShipment)
    assert isinstance(restored, CustomShipment)
    assert restored.order == Order("book", 42)


def test_non_init_positional_only_constructor_parameter_is_omitted():
    @dataclass
    class PositionalCounter:
        counter: int = field(init=False)

        def __init__(self, counter=0, /):
            self.counter = counter

    restored = JsonDataConverter().deserialize('{"counter": 42}', PositionalCounter)
    assert isinstance(restored, PositionalCounter)
    assert restored.counter == 0


@pytest.mark.parametrize("method_type", [classmethod, staticmethod])
def test_non_init_field_preserved_by_descriptor_initializer(method_type):
    @dataclass
    class DescriptorCounter:
        counter: int = field(init=False, default=0)

    if method_type is classmethod:
        def initializer(cls, counter=0):
            cls.counter = counter
    else:
        def initializer(counter=0):
            DescriptorCounter.counter = counter
    DescriptorCounter.__init__ = method_type(initializer)

    converter = JsonDataConverter()
    encoded = converter.serialize(DescriptorCounter(counter=42))
    DescriptorCounter.counter = 0
    restored = converter.deserialize(encoded, DescriptorCounter)
    assert isinstance(restored, DescriptorCounter)
    assert restored.counter == 42


def test_non_init_field_matching_receiver_name_is_omitted():
    @dataclass
    class ReceiverCounter:
        counter: int = field(init=False)

        def __init__(counter):
            counter.counter = 7

    restored = JsonDataConverter().deserialize('{"counter": 42}', ReceiverCounter)
    assert isinstance(restored, ReceiverCounter)
    assert restored.counter == 7


def test_positional_only_receiver_name_can_be_passed_through_kwargs():
    @dataclass
    class PositionalReceiver:
        self: int = field(init=False, default=0)

        def __init__(self, /, **kwargs):
            self.self = kwargs.get("self", 0)

    converter = JsonDataConverter()
    encoded = converter.serialize(PositionalReceiver(self=42))
    restored = converter.deserialize(encoded, PositionalReceiver)
    assert isinstance(restored, PositionalReceiver)
    assert restored.self == 42


@pytest.mark.parametrize("hashable", [True, False])
def test_non_init_field_preserved_by_callable_initializer(hashable):
    @dataclass
    class CallableCounter:
        counter: int = field(init=False, default=0)

    class Initializer:
        def __call__(self, counter=0):
            CallableCounter.counter = counter

    if not hashable:
        Initializer.__hash__ = None
    CallableCounter.__init__ = Initializer()

    converter = JsonDataConverter()
    encoded = converter.serialize(CallableCounter(counter=42))
    CallableCounter.counter = 0
    restored = converter.deserialize(encoded, CallableCounter)
    assert isinstance(restored, CallableCounter)
    assert restored.counter == 42


@pytest.mark.parametrize("error_type", [TypeError, ValueError])
def test_non_init_fields_retained_when_constructor_signature_unavailable(error_type):
    @dataclass
    class UninspectableCounter:
        counter: int = field(init=False)

        def __init__(self, counter=0):
            self.counter = counter

    with patch("durabletask.serialization.inspect.signature", side_effect=error_type):
        restored = JsonDataConverter().deserialize('{"counter": 42}', UninspectableCounter)
    assert isinstance(restored, UninspectableCounter)
    assert restored.counter == 42


def test_dataclass_constructor_typeerror_is_not_retried():
    calls = []

    @dataclass
    class FailingCounter:
        counter: int = field(init=False)

        def __init__(self, counter=0):
            calls.append(counter)
            raise TypeError("failure inside user constructor")

    restored = JsonDataConverter().deserialize('{"counter": 42}', FailingCounter)
    assert restored == {"counter": 42}
    assert calls == [42]


def test_ordinary_dataclass_skips_constructor_signature_inspection():
    with patch("durabletask.serialization.inspect.signature", side_effect=AssertionError("unexpected inspection")):
        restored = JsonDataConverter().deserialize('{"item": "book", "quantity": 42}', Order)
    assert restored == Order("book", 42)


def test_constructor_signature_cached_by_initializer():
    @dataclass
    class SharedInitializer:
        counter: int = field(init=False)

        def __init__(self, counter=0):
            self.counter = counter

    @dataclass(init=False)
    class InheritedInitializer(SharedInitializer):
        pass

    converter = JsonDataConverter()
    with patch("durabletask.serialization.inspect.signature", wraps=inspect.signature) as signature:
        for cls in (SharedInitializer, InheritedInitializer, SharedInitializer):
            restored = converter.deserialize('{"counter": 42}', cls)
            assert isinstance(restored, cls)
            assert restored.counter == 42
    signature.assert_called_once_with(SharedInitializer.__init__, follow_wrapped=False)


# ----- JsonDataConverter -----


def test_serialize_none_returns_none():
    assert JsonDataConverter().serialize(None) is None


def test_serialize_dataclass_plain_json():
    assert json.loads(JsonDataConverter().serialize(Order("widget", 3))) == {
        "item": "widget",
        "quantity": 3,
    }


def test_deserialize_none_or_empty_returns_none():
    conv = JsonDataConverter()
    assert conv.deserialize(None) is None
    assert conv.deserialize("") is None
    assert conv.deserialize(None, Order) is None


def test_deserialize_without_type_returns_raw():
    conv = JsonDataConverter()
    assert conv.deserialize('{"item": "x", "quantity": 1}') == {"item": "x", "quantity": 1}


def test_deserialize_coerces_to_type():
    conv = JsonDataConverter()
    result = conv.deserialize('{"item": "x", "quantity": 1}', Order)
    assert isinstance(result, Order)
    assert result == Order("x", 1)


def test_deserialize_best_effort_falls_back_to_raw(caplog):
    conv = JsonDataConverter()
    # Missing required 'quantity' field -> coercion fails -> raw dict returned.
    with caplog.at_level(logging.DEBUG, logger="durabletask"):
        result = conv.deserialize('{"item": "x"}', Order)
    assert result == {"item": "x"}
    assert any("coerce" in r.message.lower() for r in caplog.records)


def test_round_trip_through_converter():
    conv = JsonDataConverter()
    encoded = conv.serialize(Order("book", 2))
    assert conv.deserialize(encoded, Order) == Order("book", 2)


def test_default_converter_is_json_converter():
    assert isinstance(DEFAULT_DATA_CONVERTER, JsonDataConverter)


# ----- Custom converter -----


def test_custom_converter_is_a_dataconverter_subclass():
    class UpperConverter(DataConverter):
        def serialize(self, value: Any) -> str | None:
            return None if value is None else json.dumps(str(value).upper())

        def deserialize(self, data: str | None, target_type: type | None = None) -> Any:
            return None if data is None else json.loads(data)

        def coerce(self, value: Any, target_type: type | None = None) -> Any:
            return value

    conv = UpperConverter()
    assert conv.serialize("hello") == '"HELLO"'
    assert conv.deserialize('"HELLO"') == "HELLO"
    assert conv.coerce("HELLO") == "HELLO"


# ----- set and frozenset container support -----


@dataclass(frozen=True)
class TaggedOrder:
    item: str
    tags: set[str]
    flags: frozenset[int]


def test_roundtrip_dataclass_with_sets():
    converter = JsonDataConverter(enable_generic_sets=True)
    original = TaggedOrder(item="laptop", tags={"electronics", "sale"}, flags=frozenset({1, 2}))
    serialized = converter.serialize(original)
    assert serialized is not None
    restored = converter.deserialize(serialized, TaggedOrder)
    assert isinstance(restored, TaggedOrder)
    assert restored.item == "laptop"
    assert isinstance(restored.tags, set)
    assert restored.tags == {"electronics", "sale"}
    assert isinstance(restored.flags, frozenset)
    assert restored.flags == frozenset({1, 2})


def test_can_reconstruct_set_and_frozenset():
    # By default (enable_generic_sets=False), generic sets are not reconstructed to
    # preserve backward compatibility and existing-instance replay behavior.
    default_converter = JsonDataConverter()
    assert default_converter.can_reconstruct(set[PricedOrder]) is False
    assert default_converter.can_reconstruct(frozenset[PricedOrder]) is False
    assert default_converter.can_reconstruct(set[int]) is False
    assert default_converter.can_reconstruct(set[str]) is False

    # When opted in, supported sets (including primitive elements) return True,
    # while unsupported unhashable element types return False.
    opt_in_converter = JsonDataConverter(enable_generic_sets=True)
    assert opt_in_converter.can_reconstruct(set[PricedOrder]) is True
    assert opt_in_converter.can_reconstruct(frozenset[PricedOrder]) is True
    assert opt_in_converter.can_reconstruct(set[int]) is True
    assert opt_in_converter.can_reconstruct(set[str]) is True
    assert opt_in_converter.can_reconstruct(frozenset[int]) is True
    assert opt_in_converter.can_reconstruct(set[list[str]]) is False
    assert opt_in_converter.can_reconstruct(set[dict[str, int]]) is False


def test_existing_instance_replay_compatibility_preserves_list():
    """Preserve existing-instance replay behavior for recorded arrays under set annotations.

    When generic set coercion is not explicitly enabled, historical JSON arrays
    containing duplicates and order (e.g. {"tags": ["b", "a", "b"]}) remain raw
    lists, avoiding NonDeterminismError during orchestration replay.
    """
    default_converter = JsonDataConverter()
    recorded_input = '{"item": "laptop", "tags": ["b", "a", "b"], "flags": [1, 2, 1]}'
    restored = default_converter.deserialize(recorded_input, TaggedOrder)
    assert isinstance(restored, TaggedOrder)
    assert isinstance(restored.tags, list)
    assert restored.tags == ["b", "a", "b"]
    assert len(restored.tags) == 3
    assert isinstance(restored.flags, list)
    assert restored.flags == [1, 2, 1]
    assert len(restored.flags) == 3

    # With opt-in enabled, set coercion deduplicates and converts to sets
    opt_in_converter = JsonDataConverter(enable_generic_sets=True)
    opt_in_restored = opt_in_converter.deserialize(recorded_input, TaggedOrder)
    assert isinstance(opt_in_restored, TaggedOrder)
    assert isinstance(opt_in_restored.tags, set)
    assert opt_in_restored.tags == {"a", "b"}
    assert len(opt_in_restored.tags) == 2
    assert isinstance(opt_in_restored.flags, frozenset)
    assert opt_in_restored.flags == frozenset({1, 2})
    assert len(opt_in_restored.flags) == 2


def test_default_converter_replays_recorded_arrays_under_set_annotations():
    def orchestrator(ctx: task.OrchestrationContext, data: TaggedOrder):
        yield ctx.call_activity(f"count-{len(data.tags)}")
        return {"tags": data.tags, "flags": data.flags}

    registry = _Registry()
    name = registry.add_orchestrator(orchestrator)
    history = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(
            name, "legacy-sets",
            encoded_input='{"item": "laptop", "tags": ["b", "a", "b"], "flags": [1, 2, 1]}',
        ),
        helpers.new_task_scheduled_event(1, "count-3"),
    ]
    executor = _OrchestrationExecutor(registry, logging.getLogger("test_set_replay"), JsonDataConverter())
    result = executor.execute("legacy-sets", history, [helpers.new_task_completed_event(1, "null")])
    assert len(result.actions) == 1
    completion = result.actions[0].completeOrchestration
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert completion.result.value == '{"tags": ["b", "a", "b"], "flags": [1, 2, 1]}'


def test_type_discovery_and_activity_execution_with_sets():
    """Verify that type discovery recognizes set annotations when opted in,
    enabling activities to receive sets rather than raw lists."""
    def tag_activity(ctx: Any, tags: set[str]) -> int:
        assert isinstance(tags, set)
        tags.add("processed")
        return len(tags)

    reg = _Registry()
    reg.add_activity(tag_activity)
    logger = logging.getLogger("test_type_discovery_sets")

    # With opt-in, activity input is discovered as set[str] and executed
    conv = JsonDataConverter(enable_generic_sets=True)
    assert type_discovery.activity_input_type(tag_activity, conv) == set[str]
    executor = _ActivityExecutor(reg, logger, conv)
    output = executor.execute("orch-1", "tag_activity", 1, '["item1", "item2"]')
    assert output == "3"

    # Default converter preserves input as None (raw JSON), passing list
    default_conv = JsonDataConverter()
    assert type_discovery.activity_input_type(tag_activity, default_conv) is None
