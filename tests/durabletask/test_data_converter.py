# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the DataConverter abstraction and the default JsonDataConverter."""

import json
import logging
from dataclasses import dataclass
from typing import Any

from durabletask.serialization import (
    DEFAULT_DATA_CONVERTER,
    DataConverter,
    JsonDataConverter,
)


@dataclass
class Order:
    item: str
    quantity: int


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

    conv = UpperConverter()
    assert conv.serialize("hello") == '"HELLO"'
    assert conv.deserialize('"HELLO"') == "HELLO"
