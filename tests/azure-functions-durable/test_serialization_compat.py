# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json

import pytest

from azure.durable_functions.internal.serialization import (
    DEFAULT_FUNCTIONS_DATA_CONVERTER,
)


class Point:
    """Sample custom type using the v1 to_json / from_json convention."""

    def __init__(self, x: int, y: int):
        self.x = x
        self.y = y

    def to_json(self):
        return {"x": self.x, "y": self.y}

    @classmethod
    def from_json(cls, data):
        return cls(data["x"], data["y"])

    def __eq__(self, other):
        return isinstance(other, Point) and self.x == other.x and self.y == other.y


def test_custom_object_round_trips():
    point = Point(3, 4)
    serialized = DEFAULT_FUNCTIONS_DATA_CONVERTER.serialize(point)
    assert isinstance(serialized, str)

    restored = DEFAULT_FUNCTIONS_DATA_CONVERTER.deserialize(serialized)
    assert isinstance(restored, Point)
    assert restored == point


def test_nested_custom_object_round_trips():
    payload = {"points": [Point(1, 1), Point(2, 2)], "label": "path"}
    serialized = DEFAULT_FUNCTIONS_DATA_CONVERTER.serialize(payload)
    restored = DEFAULT_FUNCTIONS_DATA_CONVERTER.deserialize(serialized)
    assert restored["label"] == "path"
    assert restored["points"] == [Point(1, 1), Point(2, 2)]


@pytest.mark.parametrize("value", [
    {"a": 1, "b": [1, 2, 3]},
    [1, 2, 3],
    "hello",
    42,
    3.14,
    True,
])
def test_builtin_values_round_trip(value):
    serialized = DEFAULT_FUNCTIONS_DATA_CONVERTER.serialize(value)
    assert isinstance(serialized, str)
    restored = DEFAULT_FUNCTIONS_DATA_CONVERTER.deserialize(serialized)
    assert restored == value


def test_none_round_trips():
    assert DEFAULT_FUNCTIONS_DATA_CONVERTER.serialize(None) is None
    assert DEFAULT_FUNCTIONS_DATA_CONVERTER.deserialize(None) is None


def test_coerce_plain_dict_to_type():
    # get_input(expected_type=...) relies on the converter coercing a plain
    # (already-deserialized) dict into the declared type.
    coerced = DEFAULT_FUNCTIONS_DATA_CONVERTER.coerce({"x": 5, "y": 6}, Point)
    assert coerced == Point(5, 6)


def test_deserialize_reconstructs_from_json_type_from_plain_dict():
    # A payload serialized as a plain JSON object (not the custom-object
    # envelope) must still be reconstructed to a from_json-capable target type.
    # This is the path behind ``call_http``, whose built-in poll orchestrator
    # returns a plain dict that must arrive as a DurableHttpResponse.
    serialized = json.dumps({"x": 7, "y": 8})
    result = DEFAULT_FUNCTIONS_DATA_CONVERTER.deserialize(serialized, Point)
    assert isinstance(result, Point)
    assert result == Point(7, 8)
