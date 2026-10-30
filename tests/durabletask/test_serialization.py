# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the private JSON codec in durabletask.serialization.

These exercise the low-level encode/decode mechanism directly. The supported,
public surface is the ``DataConverter`` abstraction (see
``test_data_converter.py``).
"""

import json
import os
import subprocess
import sys
from pathlib import Path
from collections import namedtuple
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, List, Optional, Union, get_args

import pytest
from durabletask import serialization
from durabletask.serialization import _AUTO_SERIALIZED as AUTO_SERIALIZED
from durabletask.serialization import JsonDataConverter
from durabletask.serialization import _encode_custom_object
from durabletask.serialization import _coerce_to_type as coerce_to_type
from durabletask.serialization import _from_json as from_json
from durabletask.serialization import _resolve_forward_refs as resolve_forward_refs
from durabletask.serialization import _to_json as to_json

# ----- Test fixtures -----


@dataclass
class Address:
    street: str
    city: str


@dataclass
class Person:
    name: str
    age: int
    address: Address | None = None


class Widget:
    """A custom object using the to_json/from_json convention."""

    def __init__(self, label: str, size: int):
        self.label = label
        self.size = size

    def to_json(self) -> dict:
        return {"label": self.label, "size": self.size}

    @classmethod
    def from_json(cls, data: dict) -> "Widget":
        return cls(data["label"], data["size"])

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Widget)
            and other.label == self.label
            and other.size == self.size
        )


Point = namedtuple("Point", ["x", "y"])


class StaticWidget:
    """Custom object whose to_json/from_json are static methods returning a str.

    This mirrors the ``azure-functions-durable`` sample convention where
    ``to_json(obj)`` is a ``@staticmethod`` that returns a string.
    """

    def __init__(self, name: str):
        self.name = name

    @staticmethod
    def to_json(obj: "StaticWidget") -> str:
        return obj.name

    @staticmethod
    def from_json(data: str) -> "StaticWidget":
        return StaticWidget(data)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, StaticWidget) and other.name == self.name


def test_to_json_static_hook_receives_instance():
    # type(obj).to_json(obj) must invoke the @staticmethod with the instance.
    assert to_json(StaticWidget("gizmo")) == '"gizmo"'


def test_static_hook_round_trips_with_expected_type():
    encoded = to_json(StaticWidget("gizmo"))
    result = from_json(encoded, StaticWidget)
    assert isinstance(result, StaticWidget)
    assert result == StaticWidget("gizmo")


def test_instance_to_json_hook_receives_instance():
    # The same type(obj).to_json(obj) path works for plain instance methods.
    assert json.loads(to_json(Widget("gear", 5))) == {"label": "gear", "size": 5}


# ----- to_json -----


def test_to_json_builtins_are_plain_json():
    assert to_json({"a": 1, "b": [1, 2, 3]}) == json.dumps({"a": 1, "b": [1, 2, 3]})
    assert to_json("hello") == '"hello"'
    assert to_json(42) == "42"


def test_to_json_dataclass_emits_plain_dict_without_marker():
    encoded = to_json(Address("1 Main St", "Redmond"))
    parsed = json.loads(encoded)
    assert parsed == {"street": "1 Main St", "city": "Redmond"}
    assert AUTO_SERIALIZED not in encoded


def test_to_json_nested_dataclass_has_no_marker():
    encoded = to_json(Person("Ada", 30, Address("1 Main St", "Redmond")))
    assert AUTO_SERIALIZED not in encoded
    parsed = json.loads(encoded)
    assert parsed["address"] == {"street": "1 Main St", "city": "Redmond"}


def test_to_json_simplenamespace_emits_plain_dict():
    encoded = to_json(SimpleNamespace(a=1, b="two"))
    assert AUTO_SERIALIZED not in encoded
    assert json.loads(encoded) == {"a": 1, "b": "two"}


def test_to_json_custom_object_uses_to_json_hook():
    encoded = to_json(Widget("gear", 5))
    assert json.loads(encoded) == {"label": "gear", "size": 5}


def test_to_json_namedtuple_serializes_as_array():
    # Without an expected_type the field names are not preserved on the wire.
    assert to_json(Point(1, 2)) == "[1, 2]"


def test_to_json_unserializable_raises_typeerror_with_cause():
    class NotSerializable:
        pass

    with pytest.raises(TypeError) as exc_info:
        to_json(NotSerializable())
    assert "NotSerializable" in str(exc_info.value)
    assert exc_info.value.__cause__ is not None


# ----- to_json: hook precedence and nested-hook recursion (PR #154 follow-up) -----


@dataclass
class Temperature:
    """Dataclass whose JSON shape differs from its field layout."""

    celsius: float

    def to_json(self) -> dict:
        return {"fahrenheit": self.celsius * 9 / 5 + 32}

    @classmethod
    def from_json(cls, data: dict) -> "Temperature":
        return cls((data["fahrenheit"] - 32) * 5 / 9)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Temperature) and other.celsius == self.celsius


class Money:
    """A field type that is not JSON-serializable on its own."""

    def __init__(self, cents: int):
        self.cents = cents

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Money) and other.cents == self.cents


@dataclass
class Invoice:
    """Dataclass with a non-serializable field, rescued by a to_json hook."""

    amount: Money

    def to_json(self) -> dict:
        return {"amount_cents": self.amount.cents}

    @classmethod
    def from_json(cls, data: dict) -> "Invoice":
        return cls(Money(data["amount_cents"]))

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Invoice) and other.amount == self.amount


def test_dataclass_to_json_hook_takes_precedence_over_fields():
    # The dataclass defines to_json, so its output -- not the raw fields -- wins.
    assert json.loads(to_json(Temperature(100.0))) == {"fahrenheit": 212.0}


def test_dataclass_with_non_serializable_field_uses_to_json_hook():
    # Previously the dataclass branch ran first and asdict() hit the
    # non-serializable Money field, raising even though to_json was defined.
    assert json.loads(to_json(Invoice(Money(500)))) == {"amount_cents": 500}


def test_dataclass_with_non_serializable_field_round_trips():
    encoded = to_json(Invoice(Money(500)))
    assert from_json(encoded, Invoice) == Invoice(Money(500))


def test_nested_dataclass_hook_is_honored():
    # The nested Temperature must serialize through its own to_json, not be
    # flattened to its raw fields by dataclasses.asdict.
    @dataclass
    class Reading:
        temp: Temperature

    encoded = to_json(Reading(Temperature(100.0)))
    assert json.loads(encoded) == {"temp": {"fahrenheit": 212.0}}


def test_nested_dataclass_hook_round_trips():
    @dataclass
    class Reading:
        temp: Temperature

    encoded = to_json(Reading(Temperature(37.0)))
    result = from_json(encoded, Reading)
    assert isinstance(result, Reading)
    assert result.temp == Temperature(37.0)


def test_simplenamespace_without_hook_still_emits_fields():
    # SimpleNamespace has no to_json, so it falls through to vars().
    assert json.loads(to_json(SimpleNamespace(x=1))) == {"x": 1}


# ----- from_json without expected_type (loose mode) -----


def test_from_json_returns_raw_without_expected_type():
    assert from_json('{"a": 1}') == {"a": 1}
    assert from_json("[1, 2, 3]") == [1, 2, 3]
    assert from_json("42") == 42


def test_from_json_legacy_marker_decodes_to_simplenamespace():
    legacy = json.dumps({"a": 1, "b": 2, AUTO_SERIALIZED: True})
    result = from_json(legacy)
    assert isinstance(result, SimpleNamespace)
    assert result.a == 1
    assert result.b == 2


def test_legacy_simplenamespace_reserializes_cleanly():
    legacy = json.dumps({"a": 1, AUTO_SERIALIZED: True})
    ns = from_json(legacy)
    reencoded = to_json(ns)
    assert AUTO_SERIALIZED not in reencoded
    assert json.loads(reencoded) == {"a": 1}


# ----- from_json with expected_type (type-directed) -----


def test_from_json_coerces_to_dataclass():
    encoded = to_json(Address("1 Main St", "Redmond"))
    result = from_json(encoded, Address)
    assert isinstance(result, Address)
    assert result == Address("1 Main St", "Redmond")


def test_from_json_coerces_nested_dataclass():
    encoded = to_json(Person("Ada", 30, Address("1 Main St", "Redmond")))
    result = from_json(encoded, Person)
    assert isinstance(result, Person)
    assert isinstance(result.address, Address)
    assert result.address.city == "Redmond"


def test_from_json_coerces_optional_dataclass_when_present():
    result = from_json('{"name": "Ada", "age": 30, "address": null}', Person)
    assert isinstance(result, Person)
    assert result.address is None


def test_from_json_coerces_list_of_dataclasses():
    encoded = to_json([Address("a", "b"), Address("c", "d")])
    result = from_json(encoded, list[Address])
    assert all(isinstance(item, Address) for item in result)
    assert result[1] == Address("c", "d")


def test_from_json_uses_from_json_hook():
    encoded = to_json(Widget("gear", 5))
    result = from_json(encoded, Widget)
    assert isinstance(result, Widget)
    assert result == Widget("gear", 5)


def test_from_json_primitive_passthrough_with_expected_type():
    assert from_json("42", int) == 42
    assert from_json('"hi"', str) == "hi"


def test_from_json_legacy_marker_with_expected_type_strips_and_builds():
    # A payload persisted by an older SDK version (with the marker) must still
    # decode when the caller now passes an expected_type.
    legacy = json.dumps(
        {"street": "1 Main St", "city": "Redmond", AUTO_SERIALIZED: True}
    )
    result = from_json(legacy, Address)
    assert isinstance(result, Address)
    assert result == Address("1 Main St", "Redmond")


def test_from_json_none_payload_with_expected_type():
    assert from_json("null", Address) is None


# ----- from_json container recursion (PR #154 follow-up) -----


def test_from_json_coerces_dict_of_dataclasses():
    encoded = to_json({"home": Address("1 Main St", "Redmond")})
    result = from_json(encoded, dict[str, Address])
    assert isinstance(result["home"], Address)
    assert result["home"] == Address("1 Main St", "Redmond")


def test_from_json_coerces_dict_of_from_json_types():
    encoded = to_json({"a": Widget("gear", 5), "b": Widget("cog", 7)})
    result = from_json(encoded, dict[str, Widget])
    assert result["a"] == Widget("gear", 5)
    assert result["b"] == Widget("cog", 7)


def test_from_json_dict_without_value_type_is_passthrough():
    encoded = to_json({"a": Address("1 Main St", "Redmond")})
    # Bare ``dict`` (no args) leaves values as parsed JSON.
    result = from_json(encoded, dict)
    assert result == {"a": {"street": "1 Main St", "city": "Redmond"}}


def test_from_json_coerces_fixed_length_tuple():
    encoded = to_json([Address("a", "b"), 5])
    result = from_json(encoded, tuple[Address, int])
    assert isinstance(result, tuple)
    assert result[0] == Address("a", "b")
    assert result[1] == 5


def test_from_json_coerces_homogeneous_tuple():
    encoded = to_json([Address("a", "b"), Address("c", "d")])
    result = from_json(encoded, tuple[Address, ...])
    assert isinstance(result, tuple)
    assert all(isinstance(item, Address) for item in result)
    assert result[1] == Address("c", "d")


def test_coerce_to_type_fixed_length_tuple_too_long_raises():
    # A JSON array longer than the fixed-length tuple type must fail fast rather
    # than silently dropping the trailing element(s).
    with pytest.raises(TypeError):
        coerce_to_type([1, 2, 3], tuple[int, int])


def test_coerce_to_type_fixed_length_tuple_too_short_raises():
    # A JSON array shorter than the fixed-length tuple type must fail fast rather
    # than silently producing a short tuple.
    with pytest.raises(TypeError):
        coerce_to_type([1], tuple[int, int])


# ----- coerce_to_type -----


def test_coerce_to_type_none_type_returns_value():
    value = {"a": 1}
    assert coerce_to_type(value, None) is value


def test_coerce_to_type_already_correct_type():
    addr = Address("a", "b")
    assert coerce_to_type(addr, Address) is addr


def test_coerce_to_type_invalid_conversion_raises():
    with pytest.raises(TypeError):
        coerce_to_type("not-a-number", int)


def test_coerce_optional_dataclass_coerces_member():
    result = coerce_to_type({"street": "a", "city": "b"}, Optional[Address])
    assert isinstance(result, Address)


def test_coerce_genuine_union_leaves_unmatched_value_untouched():
    @dataclass
    class A:
        x: int

    @dataclass
    class B:
        y: int

    # A dict matching neither A nor B by isinstance must be returned unchanged,
    # not force-coerced into the first union member.
    value = {"z": 1}
    assert coerce_to_type(value, Union[A, B]) == {"z": 1}


def test_coerce_dict_values_recursively():
    result = coerce_to_type({"home": {"street": "a", "city": "b"}}, dict[str, Address])
    assert isinstance(result["home"], Address)


def test_coerce_bare_any_returns_value_unchanged():
    # ``Any`` carries no type info; the parsed value is already the result.
    value = {"k": "v"}
    assert coerce_to_type(value, Any) is value


def test_coerce_optional_any_returns_value_unchanged():
    # ``Any | None`` must pass the value through rather than raising on the
    # ``isinstance(value, Any)`` check inside the union loop.
    value = {"k": "v"}
    assert coerce_to_type(value, Optional[Any]) == {"k": "v"}


def test_coerce_dataclass_with_any_field_round_trips():
    # A plain dataclass whose field is annotated ``Any | None`` must reconstruct
    # with that field left as the raw parsed JSON, not crash.
    @dataclass
    class Envelope:
        name: str
        payload: Any | None = None

    restored = from_json(to_json(Envelope("x", {"nested": [1, 2]})), Envelope)
    assert isinstance(restored, Envelope)
    assert restored.name == "x"
    assert restored.payload == {"nested": [1, 2]}


# ----- from_json converter hook (PR #154 follow-up) -----


class ConverterAwareWidget:
    """A type whose from_json hook reconstructs a nested typed value using the
    converter passed to it by the DataConverter."""

    def __init__(self, address: "Address"):
        self.address = address

    def to_json(self) -> dict:
        return {"address": self.address}

    @classmethod
    def from_json(cls, data: dict, converter) -> "ConverterAwareWidget":
        nested = converter.coerce(data["address"], Address)
        return cls(nested)


def test_from_json_hook_receives_converter_for_nested_reconstruction():
    from durabletask.serialization import JsonDataConverter

    conv = JsonDataConverter()
    encoded = conv.serialize(ConverterAwareWidget(Address("1 Main St", "Redmond")))
    result = conv.deserialize(encoded, ConverterAwareWidget)
    assert isinstance(result, ConverterAwareWidget)
    # The nested value was rebuilt into an Address by the hook via the converter.
    assert isinstance(result.address, Address)
    assert result.address.city == "Redmond"


def test_single_arg_from_json_hook_is_not_passed_converter():
    # A classic single-argument from_json must keep working unchanged.
    from durabletask.serialization import JsonDataConverter

    conv = JsonDataConverter()
    encoded = conv.serialize(Widget("gear", 5))
    result = conv.deserialize(encoded, Widget)
    assert result == Widget("gear", 5)


def test_converter_threaded_through_nested_containers():
    # A from_json hook nested inside a dataclass field's list still receives the
    # converter so it can recurse.
    from durabletask.serialization import JsonDataConverter

    @dataclass
    class Catalog:
        widgets: list[ConverterAwareWidget]

    conv = JsonDataConverter()
    encoded = conv.serialize(
        Catalog([ConverterAwareWidget(Address("a", "b"))])
    )
    result = conv.deserialize(encoded, Catalog)
    assert isinstance(result.widgets[0], ConverterAwareWidget)
    assert isinstance(result.widgets[0].address, Address)


def test_coerce_to_type_without_converter_calls_single_arg_hook():
    # Calling the private helper without a converter must not pass one, even if
    # the hook could accept it (defensive: no converter available).
    result = coerce_to_type({"label": "gear", "size": 5}, Widget)
    assert result == Widget("gear", 5)


def test_from_json_hook_unrelated_second_param_is_not_treated_as_converter():
    # A ``from_json`` whose second parameter is *not* named ``converter`` (here a
    # ``strict`` flag with a default) must not be mistaken for a converter-aware
    # hook -- otherwise the DataConverter would be bound to ``strict``.
    from durabletask.serialization import JsonDataConverter

    @dataclass
    class Flagged:
        label: str

        @classmethod
        def from_json(cls, value, strict=False):
            # If the converter were passed here, ``strict`` would be truthy.
            assert strict is False
            return cls(value["label"])

    conv = JsonDataConverter()
    result = conv.deserialize('{"label": "ok"}', Flagged)
    assert result == Flagged("ok")


# ----- forward-reference resolution (Python 3.10 get_type_hints parity) -----
#
# On Python 3.10, ``typing.get_type_hints`` does not deep-resolve forward
# references nested inside container args (e.g. the element type of
# ``list["TreeNode"]`` on a self-referential dataclass), leaving a bare string
# or ``ForwardRef``. ``_resolve_forward_refs`` restores the 3.11+ behavior so
# nested coercion still fires. These tests run on every supported version.


def test_resolve_forward_refs_resolves_string_element_type():
    # ``list["Address"]`` evaluates to a generic whose arg is the raw string
    # "Address" -- exactly what 3.10 leaves behind.
    resolved = resolve_forward_refs(List["Address"], {"Address": Address})
    assert get_args(resolved) == (Address,)


def test_resolve_forward_refs_resolves_forwardref_element_type():
    resolved = resolve_forward_refs(List["Address"], {"Address": Address})
    assert get_args(resolved) == (Address,)


def test_resolve_forward_refs_then_coerce_reconstructs_nested_dataclass():
    # The full 3.10 path: resolve the unresolved element type, then coerce the
    # contained dicts into the target dataclass.
    field_type = resolve_forward_refs(list["Address"], {"Address": Address})
    result = coerce_to_type([{"street": "a", "city": "b"}], field_type)
    assert isinstance(result[0], Address)
    assert result[0].city == "b"


def test_resolve_forward_refs_leaves_unresolvable_name_untouched():
    # An unknown name is left as a string so coercion harmlessly falls back to
    # the parsed JSON rather than raising.
    resolved = resolve_forward_refs(list["DoesNotExist"], {})
    assert get_args(resolved) == ("DoesNotExist",)


# ----- set and frozenset container serialization and coercion -----


@dataclass(frozen=True)
class FrozenAddress:
    street: str
    city: str


class SetItem:
    """Identity-hashable value exposing a custom JSON representation."""

    def __init__(self, value: Any) -> None:
        self.value = value

    def to_json(self) -> Any:
        return self.value


def test_to_json_serializes_set_and_frozenset():
    encoded_set = to_json({"b", "a"})
    assert json.loads(encoded_set) == ["a", "b"]

    encoded_frozenset = to_json(frozenset([2, 1]))
    assert json.loads(encoded_frozenset) == [1, 2]

    # Numeric sets follow lexical JSON ordering (e.g. "10" < "2")
    encoded_numeric_lexical = to_json({2, 10})
    assert json.loads(encoded_numeric_lexical) == [10, 2]

    # Verify disjoint and nested frozenset in-process serialization
    encoded_disjoint = to_json({
        frozenset({"alpha"}),
        frozenset({"beta"}),
        frozenset({"gamma"}),
    })
    assert json.loads(encoded_disjoint) == [["alpha"], ["beta"], ["gamma"]]

    encoded_nested = to_json({
        frozenset({frozenset({"b", "a"}), frozenset({"c"})}),
        frozenset({frozenset({"d"})}),
    })
    assert json.loads(encoded_nested) == [[["a", "b"], ["c"]], [["d"]]]


def test_set_serialization_non_finite_numbers():
    """Verify that sets with non-finite numbers (NaN, Infinity, -Infinity)
    serialize deterministically with lexical JSON string sorting."""
    import math

    s = {float("nan"), float("inf"), float("-inf"), 2.0, 10.0}
    encoded = to_json(s)
    parsed = json.loads(encoded)
    # Lexical order of canonical JSON strings:
    # "-Infinity", "10.0", "2.0", "Infinity", "NaN"
    assert parsed[0] == float("-inf")
    assert parsed[1] == 10.0
    assert parsed[2] == 2.0
    assert parsed[3] == float("inf")
    assert math.isnan(parsed[4])


def test_set_serialization_equal_canonical_keys_differently_ordered_dicts():
    """Verify that custom objects with identical canonical JSON but differently ordered
    dictionaries emit normalized values without order leaks."""
    class CustomDictObj:
        def __init__(self, data: dict[str, Any], tag: str):
            self.data = data
            self.tag = tag

        def to_json(self) -> dict[str, Any]:
            return self.data

        def __hash__(self) -> int:
            return hash(self.tag)

        def __eq__(self, other: object) -> bool:
            return isinstance(other, CustomDictObj) and self.tag == other.tag

    obj1 = CustomDictObj({"z": 100, "a": 1, "m": 50}, "obj1")
    obj2 = CustomDictObj({"a": 1, "m": 50, "z": 100}, "obj2")
    s = {obj1, obj2}
    encoded = to_json(s)
    parsed = json.loads(encoded)
    # Both elements must be normalized to canonical sorted key order: {"a": 1, "m": 50, "z": 100}
    assert len(parsed) == 2
    assert parsed[0] == {"a": 1, "m": 50, "z": 100}
    assert parsed[1] == {"a": 1, "m": 50, "z": 100}


@pytest.mark.parametrize("depth", [0, 1, 10])
def test_set_serialization_calls_nested_hooks_once(depth: int):
    class CountingItem:
        calls = 0

        def to_json(self):
            self.calls += 1
            return {"nested": [StaticWidget("value")]}

    item = CountingItem()
    value: Any = item
    expected: Any = {"nested": ["value"]}
    for _ in range(depth):
        value = frozenset({value})
        expected = [expected]
    assert json.loads(to_json({value})) == [expected]
    assert item.calls == 1


def test_set_serialization_does_not_parse_or_encode_singleton_sort_keys(monkeypatch: pytest.MonkeyPatch):
    dumps = json.dumps
    encoded_values: list[Any] = []

    def record_dumps(value: Any, **kwargs: Any) -> str:
        encoded_values.append(value)
        return dumps(value, **kwargs)

    def unexpected_loads(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("Set serialization must not parse intermediate JSON")

    monkeypatch.setattr(serialization.json, "loads", unexpected_loads)
    monkeypatch.setattr(serialization.json, "dumps", record_dumps)
    value = frozenset({frozenset({SetItem("x" * 100000)})})
    assert to_json(value) == dumps([["x" * 100000]])
    assert encoded_values == [value]


def test_set_serialization_sorts_each_normalized_element_once(monkeypatch: pytest.MonkeyPatch):
    dumps = json.dumps
    sort_values: list[Any] = []

    def record_dumps(value: Any, **kwargs: Any) -> str:
        if not kwargs:
            sort_values.append(value)
        return dumps(value, **kwargs)

    monkeypatch.setattr(serialization.json, "dumps", record_dumps)
    assert to_json({SetItem("b"), SetItem("a")}) == '["a", "b"]'
    assert sorted(sort_values) == ["a", "b"]


def test_set_serialization_matches_canonical_json_order():
    values = [
        None, True, False, -1, 1, 10, 1.0, 10.0, float("nan"),
        float("-inf"), float("inf"), "", '"', "\\", "\n", "\u2603",
        [], [1], [10], [1, 2], [1.0], [{}], [[]],
        {}, {"a": 1}, {"a": 10}, {"a": 1, "b": []},
        {"z": [1, {"b": 2, "a": 1}], "a": [[], {}]},
    ]
    expected = "[" + ", ".join(sorted(json.dumps(value, sort_keys=True) for value in values)) + "]"
    assert to_json({SetItem(value) for value in values}) == expected


def test_set_serialization_normalizes_json_object_names_before_ordering():
    # Numeric names become strings on the wire, just like string names.
    numeric = SetItem({2: "two", 10: "ten"})
    strings = SetItem({"2": "two", "10": "ten"})
    expected = '{"10": "ten", "2": "two"}'
    assert to_json({numeric, strings}) == f"[{expected}, {expected}]"
    assert to_json({frozenset({numeric}), frozenset({strings})}) == f"[[{expected}], [{expected}]]"


@pytest.mark.parametrize("reverse", [False, True])
def test_set_serialization_collapses_equivalent_unicode_object_names(reverse: bool):
    pairs = [("\ud83d\ude00", 1), ("\U0001f600", 2)]
    value = dict(reversed(pairs) if reverse else pairs)
    expected = '[{"\\ud83d\\ude00": 2}]'
    assert to_json({SetItem(value)}) == expected
    assert to_json({frozenset({SetItem(value)})}) == f"[{expected}]"


@pytest.mark.parametrize("name", [
    "ascii", "\u2603", "\U0001f600", "\ud83d\ude00", "\ud83d",
    "\ude00", "\ud83d\ud83d\ude00", "\ude00\ud83d", "\ud83dA\ude00",
])
def test_set_serialization_unicode_names_match_json_normalization(name: str):
    value = {name: 1}
    expected = json.dumps([json.loads(json.dumps(value, sort_keys=True))])
    assert to_json({SetItem(value)}) == expected


def test_set_serialization_orders_normalized_unicode_names():
    value = {"\ud83d\ude00": 1, "\ue000": 2}
    assert to_json({SetItem(value)}) == '[{"\\ue000": 2, "\\ud83d\\ude00": 1}]'


@pytest.mark.parametrize("value", [
    {"\u2603" + str(i): i for i in range(10000)},
    [{"a": i, "b": i + 1, "c": i + 2} for i in range(1000)],
])
def test_set_serialization_dictionary_heavy_payloads(value: Any):
    class CountingItem(SetItem):
        calls = 0

        def to_json(self) -> Any:
            self.calls += 1
            return self.value

    item = CountingItem(value)
    assert to_json({item}) == json.dumps([value], sort_keys=True)
    assert item.calls == 1


@pytest.mark.parametrize("path", ["dict", "list", "hook"])
def test_set_serialization_tracks_dictionary_cycles_through_children(path: str):
    value: dict[str, Any] = {"a": 1}
    if path == "dict":
        value["z"] = value
    elif path == "list":
        value["z"] = [value]
    else:
        value["z"] = SetItem(value)
    with pytest.raises(TypeError, match="Circular reference detected"):
        to_json({SetItem(value)})


def test_set_serialization_normalizes_dictionary_children_once_per_occurrence():
    class CountingItem(SetItem):
        calls = 0

        def to_json(self) -> Any:
            self.calls += 1
            return self.value

    child = CountingItem({"b": 2, "a": 1})
    value = {"a": 0, "b": child, "c": child}
    assert to_json({SetItem(value)}) == '[{"a": 0, "b": {"a": 1, "b": 2}, "c": {"a": 1, "b": 2}}]'
    assert child.calls == 2


@pytest.mark.parametrize("shape", ["list", "dict", "hook", "set"])
def test_set_serialization_rejects_cycles(shape: str):
    class Item:
        def to_json(self):
            return value

    item = Item()
    value: Any
    if shape == "list":
        value = [item]
    elif shape == "dict":
        value = {"item": item}
    elif shape == "hook":
        value = item
    else:
        value = {item}
    with pytest.raises(TypeError, match="Circular reference detected"):
        to_json({item})


def test_set_serialization_allows_shared_non_circular_values():
    shared = {"b": 2, "a": 1}
    assert to_json({SetItem([shared, shared])}) == '[[{"a": 1, "b": 2}, {"a": 1, "b": 2}]]'


def test_set_serialization_preserves_native_and_custom_hook_precedence():
    class HookedTuple(tuple):
        def to_json(self):
            pytest.fail("JSON-native containers must not invoke to_json")

    class HookedFrozenSet(frozenset):
        def to_json(self):
            return "custom set"

    class Item:
        def to_json(self):
            return SimpleNamespace(
                z=HookedTuple([Temperature(100.0)]),
                a=HookedFrozenSet({1}),
            )

    assert to_json({Item()}) == '[{"a": "custom set", "z": [{"fahrenheit": 212.0}]}]'


@pytest.mark.parametrize("value", [object(), {object(): 1}, {"a": 1, 2: 3}])
def test_set_serialization_preserves_encoding_errors(value: Any):
    class Item:
        def to_json(self):
            return value

    with pytest.raises(TypeError, match="Cannot deterministically serialize set") as error:
        to_json({Item()})
    assert error.value.__cause__ is not None


def test_serialization_outside_sets_matches_legacy_json_encoder():
    value = {"z": [Temperature(100.0), {"b": 2, "a": 1}], "a": (1, 2)}
    assert to_json(value) == json.dumps(value, default=_encode_custom_object)
    assert to_json(value) == '{"z": [{"fahrenheit": 212.0}, {"b": 2, "a": 1}], "a": [1, 2]}'
    names = {"\ud83d\ude00": 1, "\U0001f600": 2}
    assert to_json(names) == json.dumps(names)


def test_set_serialization_deterministic_across_hash_seeds():
    """Verify that sets of disjoint frozensets, nested frozensets, unorderable custom objects,
    non-finite numbers, and equal canonical keys with differently ordered dictionaries
    are deterministically serialized across separate processes with differing PYTHONHASHSEED."""
    repo_root = str(Path(__file__).resolve().parents[2])
    code = """
import json
from dataclasses import dataclass
from typing import Any
from durabletask.serialization import JsonDataConverter

conv = JsonDataConverter()

# Case A: Disjoint frozensets (exposes frozenset partial-ordering defects)
case_a = conv.serialize({
    frozenset({"alpha"}),
    frozenset({"beta"}),
    frozenset({"gamma"}),
})

# Case B: Nested frozensets (multi-element, multi-level nesting)
case_b = conv.serialize({
    frozenset({frozenset({"b", "a"}), frozenset({"c"})}),
    frozenset({frozenset({"d"})}),
})

# Case C: Non-orderable custom objects (frozen dataclass without order=True)
@dataclass(frozen=True)
class Entry:
    id: str
    val: int

case_c = conv.serialize({
    Entry("delta", 4),
    Entry("alpha", 1),
    Entry("charlie", 3),
    Entry("bravo", 2),
})

# Case D: Numeric sets with lexical JSON ordering and non-finite floats
case_d = conv.serialize({float("nan"), float("inf"), float("-inf"), 2.0, 10.0})

# Case E: Equal canonical keys with differently ordered dictionaries
class DictItem:
    def __init__(self, data: dict[str, Any], tag: str):
        self.data = data
        self.tag = tag
    def to_json(self) -> dict[str, Any]:
        return self.data
    def __hash__(self) -> int:
        return hash(self.tag)
    def __eq__(self, other: object) -> bool:
        return isinstance(other, DictItem) and self.tag == other.tag

case_e = conv.serialize({
    DictItem({"z": 10, "a": 1}, "item1"),
    DictItem({"a": 1, "z": 10}, "item2"),
})

# Case F: Different Python key types representing identical JSON object names.
case_f = conv.serialize({
    DictItem({2: "two", 10: "ten"}, "numeric"),
    DictItem({"2": "two", "10": "ten"}, "strings"),
})

print(json.dumps({"a": case_a, "b": case_b, "c": case_c, "d": case_d, "e": case_e, "f": case_f}))
"""
    seeds = ["0", "1", "2", "42", "999"]
    outputs: dict[str, dict[str, str]] = {}

    for seed in seeds:
        env = os.environ.copy()
        env["PYTHONHASHSEED"] = seed
        env["PYTHONPATH"] = (
            f"{repo_root}{os.pathsep}{env['PYTHONPATH']}"
            if "PYTHONPATH" in env
            else repo_root
        )
        proc = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env=env,
        )
        assert proc.returncode == 0, (
            f"Subprocess failed for seed {seed}:\nstdout: {proc.stdout}\nstderr: {proc.stderr}"
        )
        outputs[seed] = json.loads(proc.stdout.strip())

    base = outputs[seeds[0]]
    for seed in seeds[1:]:
        assert outputs[seed] == base, (
            f"Deterministic serialization failed between seed {seeds[0]} and {seed}:\n"
            f"Seed {seeds[0]}: {base}\nSeed {seed}: {outputs[seed]}"
        )

    expected_a = '[["alpha"], ["beta"], ["gamma"]]'
    expected_b = '[[["a", "b"], ["c"]], [["d"]]]'
    expected_c = (
        '[{"id": "alpha", "val": 1}, {"id": "bravo", "val": 2}, '
        '{"id": "charlie", "val": 3}, {"id": "delta", "val": 4}]'
    )
    expected_d = '[-Infinity, 10.0, 2.0, Infinity, NaN]'
    expected_e = '[{"a": 1, "z": 10}, {"a": 1, "z": 10}]'
    expected_f = '[{"10": "ten", "2": "two"}, {"10": "ten", "2": "two"}]'

    assert base["a"] == expected_a
    assert base["b"] == expected_b
    assert base["c"] == expected_c
    assert base["d"] == expected_d
    assert base["e"] == expected_e
    assert base["f"] == expected_f


def test_deserialize_coerces_set_when_opted_in():
    converter = JsonDataConverter(enable_generic_sets=True)
    encoded = converter.serialize(["a", "b", "c"])
    assert encoded is not None
    result = converter.deserialize(encoded, set[str])
    assert isinstance(result, set)
    assert result == {"a", "b", "c"}

    encoded_int = converter.serialize([1, 2, 3])
    assert encoded_int is not None
    result_frozen = converter.deserialize(encoded_int, frozenset[int])
    assert isinstance(result_frozen, frozenset)
    assert result_frozen == frozenset({1, 2, 3})

    encoded_dc = json.dumps([
        {"street": "1 Main St", "city": "Redmond"},
        {"street": "2 1st Ave", "city": "Seattle"},
    ])
    result_dc = converter.deserialize(encoded_dc, set[FrozenAddress])
    assert isinstance(result_dc, set)
    assert all(isinstance(item, FrozenAddress) for item in result_dc)
    assert FrozenAddress("1 Main St", "Redmond") in result_dc


def test_deserialize_preserves_raw_array_when_opted_out():
    default_converter = JsonDataConverter()
    payload = '["b", "a", "b"]'
    result = default_converter.deserialize(payload, set[str])
    # Preserves list and duplicates for replay compatibility
    assert isinstance(result, list)
    assert result == ["b", "a", "b"]
