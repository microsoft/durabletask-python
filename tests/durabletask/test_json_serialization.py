# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import dataclasses
from collections import namedtuple
from types import SimpleNamespace

import pytest

from durabletask.internal.shared import (
    AUTO_SERIALIZED,
    from_json,
    to_json,
)


# --- Dataclass fixtures ---

@dataclasses.dataclass
class SimpleData:
    x: int
    y: str


@dataclasses.dataclass
class InnerData:
    value: int


@dataclasses.dataclass
class OuterData:
    inner: InnerData
    label: str


@dataclasses.dataclass
class DeeplyNested:
    outer: OuterData
    flag: bool


# --- Namedtuple fixtures ---

Point = namedtuple("Point", ["x", "y"])


class TestDataclassSerialization:
    """Tests for dataclass serialization/deserialization via to_json/from_json."""

    def test_simple_dataclass_round_trip(self):
        """A simple dataclass should serialize and deserialize to a SimpleNamespace."""
        obj = SimpleData(x=1, y="hello")
        json_str = to_json(obj)
        result = from_json(json_str)

        assert isinstance(result, SimpleNamespace)
        assert result.x == 1
        assert result.y == "hello"

    def test_simple_dataclass_json_contains_auto_serialized_marker(self):
        """The JSON output should contain the AUTO_SERIALIZED marker."""
        obj = SimpleData(x=1, y="hello")
        json_str = to_json(obj)

        assert AUTO_SERIALIZED in json_str

    def test_nested_dataclass_round_trip(self):
        """Nested dataclasses should all deserialize to SimpleNamespace, not dict."""
        obj = OuterData(inner=InnerData(value=42), label="test")
        json_str = to_json(obj)
        result = from_json(json_str)

        assert isinstance(result, SimpleNamespace)
        assert isinstance(result.inner, SimpleNamespace), (
            "Inner dataclass should deserialize to SimpleNamespace, not dict"
        )
        assert result.inner.value == 42
        assert result.label == "test"

    def test_deeply_nested_dataclass_round_trip(self):
        """Deeply nested dataclasses should all deserialize to SimpleNamespace."""
        obj = DeeplyNested(
            outer=OuterData(inner=InnerData(value=7), label="deep"),
            flag=True,
        )
        json_str = to_json(obj)
        result = from_json(json_str)

        assert isinstance(result, SimpleNamespace)
        assert isinstance(result.outer, SimpleNamespace)
        assert isinstance(result.outer.inner, SimpleNamespace)
        assert result.outer.inner.value == 7
        assert result.outer.label == "deep"
        assert result.flag is True

    def test_dataclass_inside_dict(self):
        """A dataclass value inside a dict should round-trip as SimpleNamespace."""
        obj = {"data": SimpleData(x=10, y="world")}
        json_str = to_json(obj)
        result = from_json(json_str)

        assert isinstance(result, dict)
        assert isinstance(result["data"], SimpleNamespace)
        assert result["data"].x == 10
        assert result["data"].y == "world"

    def test_dataclass_inside_list(self):
        """Dataclass items inside a list should round-trip as SimpleNamespace."""
        items = [SimpleData(x=1, y="a"), SimpleData(x=2, y="b")]
        json_str = to_json(items)
        result = from_json(json_str)

        assert isinstance(result, list)
        assert len(result) == 2
        for item in result:
            assert isinstance(item, SimpleNamespace)
        assert result[0].x == 1
        assert result[1].y == "b"

    def test_array_of_nested_dataclasses(self):
        """An array of dataclasses with nested dataclass fields should fully round-trip."""
        items = [
            OuterData(inner=InnerData(value=1), label="first"),
            OuterData(inner=InnerData(value=2), label="second"),
        ]
        json_str = to_json(items)
        result = from_json(json_str)

        assert isinstance(result, list)
        assert len(result) == 2
        for item in result:
            assert isinstance(item, SimpleNamespace)
            assert isinstance(item.inner, SimpleNamespace)
        assert result[0].inner.value == 1
        assert result[0].label == "first"
        assert result[1].inner.value == 2
        assert result[1].label == "second"

    def test_nested_array_of_dataclasses(self):
        """An array nested inside another array of dataclasses should round-trip."""
        items = [
            [SimpleData(x=1, y="a"), SimpleData(x=2, y="b")],
            [SimpleData(x=3, y="c")],
        ]
        json_str = to_json(items)
        result = from_json(json_str)

        assert isinstance(result, list)
        assert len(result) == 2
        assert isinstance(result[0], list)
        assert len(result[0]) == 2
        assert isinstance(result[1], list)
        assert len(result[1]) == 1
        for sublist in result:
            for item in sublist:
                assert isinstance(item, SimpleNamespace)
        assert result[0][0].x == 1
        assert result[0][1].y == "b"
        assert result[1][0].x == 3

    def test_dict_with_nested_dataclass_values(self):
        """Dict values that are nested dataclasses should fully round-trip."""
        obj = {"item": OuterData(inner=InnerData(value=99), label="nested")}
        json_str = to_json(obj)
        result = from_json(json_str)

        assert isinstance(result, dict)
        assert isinstance(result["item"], SimpleNamespace)
        assert isinstance(result["item"].inner, SimpleNamespace)
        assert result["item"].inner.value == 99
        assert result["item"].label == "nested"

    def test_dict_with_multiple_dataclass_values(self):
        """A dict with several dataclass values should all round-trip."""
        obj = {
            "a": SimpleData(x=1, y="one"),
            "b": SimpleData(x=2, y="two"),
        }
        json_str = to_json(obj)
        result = from_json(json_str)

        assert isinstance(result, dict)
        for key in ("a", "b"):
            assert isinstance(result[key], SimpleNamespace)
        assert result["a"].x == 1
        assert result["b"].y == "two"

    def test_dict_with_array_of_dataclasses(self):
        """A dict whose value is a list of dataclasses should round-trip."""
        obj = {"items": [SimpleData(x=1, y="a"), SimpleData(x=2, y="b")]}
        json_str = to_json(obj)
        result = from_json(json_str)

        assert isinstance(result, dict)
        assert isinstance(result["items"], list)
        assert len(result["items"]) == 2
        for item in result["items"]:
            assert isinstance(item, SimpleNamespace)
        assert result["items"][0].x == 1
        assert result["items"][1].y == "b"

    def test_dict_with_array_of_nested_dataclasses(self):
        """A dict whose value is a list of nested dataclasses should fully round-trip."""
        obj = {
            "records": [
                OuterData(inner=InnerData(value=10), label="r1"),
                OuterData(inner=InnerData(value=20), label="r2"),
            ]
        }
        json_str = to_json(obj)
        result = from_json(json_str)

        assert isinstance(result, dict)
        assert isinstance(result["records"], list)
        for item in result["records"]:
            assert isinstance(item, SimpleNamespace)
            assert isinstance(item.inner, SimpleNamespace)
        assert result["records"][0].inner.value == 10
        assert result["records"][1].label == "r2"

    def test_dataclass_with_list_of_dataclass_field(self):
        """A dataclass containing a list-of-dataclass field should round-trip."""
        @dataclasses.dataclass
        class Container:
            items: list

        obj = Container(items=[InnerData(value=1), InnerData(value=2)])
        json_str = to_json(obj)
        result = from_json(json_str)

        assert isinstance(result, SimpleNamespace)
        assert isinstance(result.items, list)
        assert len(result.items) == 2
        for item in result.items:
            assert isinstance(item, SimpleNamespace)
        assert result.items[0].value == 1
        assert result.items[1].value == 2

    def test_dataclass_with_dict_of_dataclass_field(self):
        """A dataclass containing a dict-of-dataclass field should round-trip."""
        @dataclasses.dataclass
        class Mapping:
            entries: dict

        obj = Mapping(entries={"a": InnerData(value=5), "b": InnerData(value=6)})
        json_str = to_json(obj)
        result = from_json(json_str)

        assert isinstance(result, SimpleNamespace)
        assert isinstance(result.entries, dict)
        for val in result.entries.values():
            assert isinstance(val, SimpleNamespace)
        assert result.entries["a"].value == 5
        assert result.entries["b"].value == 6


class TestSimpleNamespaceSerialization:
    """Tests for SimpleNamespace serialization."""

    def test_simple_namespace_round_trip(self):
        """SimpleNamespace should serialize and deserialize correctly."""
        obj = SimpleNamespace(a=1, b="two")
        json_str = to_json(obj)
        result = from_json(json_str)

        assert isinstance(result, SimpleNamespace)
        assert result.a == 1
        assert result.b == "two"

    def test_simple_namespace_not_mutated(self):
        """Serializing a SimpleNamespace should NOT mutate the original object."""
        obj = SimpleNamespace(x=1, y=2)
        original_attrs = set(vars(obj).keys())

        to_json(obj)

        current_attrs = set(vars(obj).keys())
        assert current_attrs == original_attrs, (
            f"Original SimpleNamespace was mutated: gained {current_attrs - original_attrs}"
        )
        assert not hasattr(obj, AUTO_SERIALIZED)

    def test_nested_simple_namespace_round_trip(self):
        """Nested SimpleNamespace should deserialize as SimpleNamespace."""
        obj = SimpleNamespace(child=SimpleNamespace(val=99))
        json_str = to_json(obj)
        result = from_json(json_str)

        assert isinstance(result, SimpleNamespace)
        assert isinstance(result.child, SimpleNamespace)
        assert result.child.val == 99


class TestNamedtupleSerialization:
    """Tests for namedtuple serialization."""

    def test_namedtuple_top_level_round_trip(self):
        """A namedtuple at the top level should serialize with field names."""
        p = Point(x=3, y=4)
        json_str = to_json(p)
        result = from_json(json_str)

        assert isinstance(result, SimpleNamespace)
        assert result.x == 3
        assert result.y == 4


class TestPrimitiveSerialization:
    """Tests for primitive/basic type round-trips."""

    @pytest.mark.parametrize("value", [
        42,
        3.14,
        "hello",
        True,
        False,
        None,
        [1, 2, 3],
        {"key": "value"},
    ])
    def test_primitive_round_trip(self, value):
        """Primitive types should round-trip unchanged."""
        json_str = to_json(value)
        result = from_json(json_str)
        assert result == value


class TestDataclassNotMutated:
    """Ensure serialization does not mutate dataclass inputs."""

    def test_dataclass_not_mutated(self):
        """Serializing a dataclass should not add attributes to the original."""
        obj = SimpleData(x=1, y="test")
        to_json(obj)

        # dataclass fields should be unchanged
        assert obj.x == 1
        assert obj.y == "test"
        assert not hasattr(obj, AUTO_SERIALIZED)
