# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the JSON serialization codec in durabletask.internal.shared."""

import json
from collections import namedtuple
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from durabletask.internal import shared


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


# ----- to_json -----


def test_to_json_builtins_are_plain_json():
    assert shared.to_json({"a": 1, "b": [1, 2, 3]}) == json.dumps({"a": 1, "b": [1, 2, 3]})
    assert shared.to_json("hello") == '"hello"'
    assert shared.to_json(42) == "42"


def test_to_json_dataclass_emits_plain_dict_without_marker():
    encoded = shared.to_json(Address("1 Main St", "Redmond"))
    parsed = json.loads(encoded)
    assert parsed == {"street": "1 Main St", "city": "Redmond"}
    assert shared.AUTO_SERIALIZED not in encoded


def test_to_json_nested_dataclass_has_no_marker():
    encoded = shared.to_json(Person("Ada", 30, Address("1 Main St", "Redmond")))
    assert shared.AUTO_SERIALIZED not in encoded
    parsed = json.loads(encoded)
    assert parsed["address"] == {"street": "1 Main St", "city": "Redmond"}


def test_to_json_simplenamespace_emits_plain_dict():
    encoded = shared.to_json(SimpleNamespace(a=1, b="two"))
    assert shared.AUTO_SERIALIZED not in encoded
    assert json.loads(encoded) == {"a": 1, "b": "two"}


def test_to_json_custom_object_uses_to_json_hook():
    encoded = shared.to_json(Widget("gear", 5))
    assert json.loads(encoded) == {"label": "gear", "size": 5}


def test_to_json_namedtuple_serializes_as_array():
    # Without an expected_type the field names are not preserved on the wire.
    assert shared.to_json(Point(1, 2)) == "[1, 2]"


def test_to_json_unserializable_raises_typeerror_with_cause():
    class NotSerializable:
        pass

    with pytest.raises(TypeError) as exc_info:
        shared.to_json(NotSerializable())
    assert "NotSerializable" in str(exc_info.value)
    assert exc_info.value.__cause__ is not None


# ----- from_json without expected_type (loose mode) -----


def test_from_json_returns_raw_without_expected_type():
    assert shared.from_json('{"a": 1}') == {"a": 1}
    assert shared.from_json("[1, 2, 3]") == [1, 2, 3]
    assert shared.from_json("42") == 42


def test_from_json_legacy_marker_decodes_to_simplenamespace():
    legacy = json.dumps({"a": 1, "b": 2, shared.AUTO_SERIALIZED: True})
    result = shared.from_json(legacy)
    assert isinstance(result, SimpleNamespace)
    assert result.a == 1
    assert result.b == 2


def test_legacy_simplenamespace_reserializes_cleanly():
    legacy = json.dumps({"a": 1, shared.AUTO_SERIALIZED: True})
    ns = shared.from_json(legacy)
    reencoded = shared.to_json(ns)
    assert shared.AUTO_SERIALIZED not in reencoded
    assert json.loads(reencoded) == {"a": 1}


# ----- from_json with expected_type (type-directed) -----


def test_from_json_coerces_to_dataclass():
    encoded = shared.to_json(Address("1 Main St", "Redmond"))
    result = shared.from_json(encoded, Address)
    assert isinstance(result, Address)
    assert result == Address("1 Main St", "Redmond")


def test_from_json_coerces_nested_dataclass():
    encoded = shared.to_json(Person("Ada", 30, Address("1 Main St", "Redmond")))
    result = shared.from_json(encoded, Person)
    assert isinstance(result, Person)
    assert isinstance(result.address, Address)
    assert result.address.city == "Redmond"


def test_from_json_coerces_optional_dataclass_when_present():
    result = shared.from_json('{"name": "Ada", "age": 30, "address": null}', Person)
    assert isinstance(result, Person)
    assert result.address is None


def test_from_json_coerces_list_of_dataclasses():
    encoded = shared.to_json([Address("a", "b"), Address("c", "d")])
    result = shared.from_json(encoded, list[Address])
    assert all(isinstance(item, Address) for item in result)
    assert result[1] == Address("c", "d")


def test_from_json_uses_from_json_hook():
    encoded = shared.to_json(Widget("gear", 5))
    result = shared.from_json(encoded, Widget)
    assert isinstance(result, Widget)
    assert result == Widget("gear", 5)


def test_from_json_primitive_passthrough_with_expected_type():
    assert shared.from_json("42", int) == 42
    assert shared.from_json('"hi"', str) == "hi"


def test_from_json_legacy_marker_with_expected_type_strips_and_builds():
    # A payload persisted by an older SDK version (with the marker) must still
    # decode when the caller now passes an expected_type.
    legacy = json.dumps(
        {"street": "1 Main St", "city": "Redmond", shared.AUTO_SERIALIZED: True}
    )
    result = shared.from_json(legacy, Address)
    assert isinstance(result, Address)
    assert result == Address("1 Main St", "Redmond")


def test_from_json_none_payload_with_expected_type():
    assert shared.from_json("null", Address) is None


# ----- coerce_to_type -----


def test_coerce_to_type_none_type_returns_value():
    value = {"a": 1}
    assert shared.coerce_to_type(value, None) is value


def test_coerce_to_type_already_correct_type():
    addr = Address("a", "b")
    assert shared.coerce_to_type(addr, Address) is addr


def test_coerce_to_type_invalid_conversion_raises():
    with pytest.raises(TypeError):
        shared.coerce_to_type("not-a-number", int)
