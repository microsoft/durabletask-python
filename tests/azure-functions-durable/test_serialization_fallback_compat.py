# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the legacy serialization fallback path.

When the installed ``azure-functions`` package predates the centralized
``df_dumps`` / ``df_loads`` serializers, the converter falls back to the legacy
``_serialize_custom_object`` / ``_deserialize_custom_object`` hooks. These tests
exercise the fallback callables directly (they are otherwise only selected at
import time based on the installed SDK version).
"""

import pytest

import azure.durable_functions.internal.serialization as serialization


class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def to_json(self):
        return {"x": self.x, "y": self.y}

    @classmethod
    def from_json(cls, data):
        return cls(data["x"], data["y"])

    def __eq__(self, other):
        return isinstance(other, Point) and self.x == other.x and self.y == other.y


def test_fallback_dumps_and_loads_round_trip_builtin():
    serialized = serialization._fallback_df_dumps({"a": [1, 2, 3]})
    assert serialization._fallback_df_loads(serialized) == {"a": [1, 2, 3]}


def test_fallback_dumps_and_loads_round_trip_custom_object():
    serialized = serialization._fallback_df_dumps(Point(3, 4))
    restored = serialization._fallback_df_loads(serialized)
    assert restored == Point(3, 4)


def test_fallback_loads_ignores_expected_type():
    serialized = serialization._fallback_df_dumps({"k": "v"})
    # expected_type is accepted for call-site compatibility but ignored.
    assert serialization._fallback_df_loads(serialized, expected_type=dict) == {"k": "v"}


def test_warn_fallback_is_emitted_only_once(caplog):
    # Reset the module-level guard so the warning path is exercised.
    serialization._warned = False
    try:
        with caplog.at_level("DEBUG", logger="azure.functions.DurableFunctions"):
            serialization._warn_fallback_once()
            serialization._warn_fallback_once()
        fallback_records = [
            r for r in caplog.records if "centralized" in r.getMessage()]
        assert len(fallback_records) == 1
    finally:
        serialization._warned = True


def test_fallback_raises_clearly_when_legacy_hooks_unavailable(monkeypatch):
    # If the private azure-functions custom-object hooks are missing (renamed or
    # removed upstream), the fallback path raises a clear RuntimeError rather
    # than a cryptic failure.
    monkeypatch.setattr(serialization, "_serialize_custom_object", None)
    monkeypatch.setattr(serialization, "_deserialize_custom_object", None)
    with pytest.raises(RuntimeError, match="Durable serialization API"):
        serialization._fallback_df_dumps({"a": 1})
    with pytest.raises(RuntimeError, match="Durable serialization API"):
        serialization._fallback_df_loads("{}")
