# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the private JSON codec in durabletask.serialization.

These exercise the low-level encode/decode mechanism directly. The supported,
public surface is the ``DataConverter`` abstraction (see
``test_data_converter.py``).
"""

import json
from collections import namedtuple
from dataclasses import dataclass
from types import SimpleNamespace
from typing import List, Optional, Union, get_args

import pytest

from durabletask.serialization import _AUTO_SERIALIZED as AUTO_SERIALIZED
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
    resolved = resolve_forward_refs(list["Address"], {"Address": Address})
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
