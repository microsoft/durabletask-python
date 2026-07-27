# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

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


def test_coerce_round_trips_custom_object():
    # coerce validates through the codec (serialize + deserialize) rather than
    # permissively reconstructing an arbitrary value, so a custom object
    # round-trips to its declared type.
    coerced = DEFAULT_FUNCTIONS_DATA_CONVERTER.coerce(Point(5, 6), Point)
    assert coerced == Point(5, 6)
