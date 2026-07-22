# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the durabletask-native activity adapter (``wrap_activity``)."""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from typing import Any

import pytest

from azure.durable_functions.internal.compat.activity import wrap_activity


def test_one_param_activity_passes_through_unchanged():
    def act(x):
        return x

    assert wrap_activity(act, "x") is act


def test_two_param_activity_is_adapted_to_single_input():
    def act(ctx, payload):
        return (ctx, payload)

    adapted = wrap_activity(act, "payload")

    assert adapted is not act
    assert list(inspect.signature(adapted).parameters) == ["payload"]
    assert adapted.__name__ == "act"
    # A placeholder context is supplied; the input is passed through.
    ctx, payload = adapted("value")
    assert payload == "value"
    # Reading the placeholder context raises a clear error rather than an
    # opaque AttributeError on None.
    with pytest.raises(NotImplementedError, match="ActivityContext is not available"):
        _ = ctx.orchestration_id


def test_adapter_invokes_original_positionally_regardless_of_param_name():
    # The original's second parameter name differs from input_name; the adapter
    # still binds correctly because it calls the original positionally.
    def act(ctx, original_name):
        return original_name

    adapted = wrap_activity(act, "input")
    assert list(inspect.signature(adapted).parameters) == ["input"]
    assert adapted("hi") == "hi"


def test_adapter_sanitizes_parameterized_generic_annotations():
    def act(ctx, payload: Mapping[str, Any]) -> dict[str, Any]:
        return dict(payload)

    adapted = wrap_activity(act, "payload")
    # Parameterized generics (rejected by the worker indexer) are reduced to
    # concrete builtins.
    assert adapted.__annotations__ == {"payload": dict, "return": dict}


def test_adapter_preserves_concrete_annotations():
    def act(ctx, name: str) -> str:
        return name

    adapted = wrap_activity(act, "name")
    assert adapted.__annotations__ == {"name": str, "return": str}


def test_adapter_without_annotations_has_none():
    def act(ctx, payload):
        return payload

    adapted = wrap_activity(act, "payload")
    assert adapted.__annotations__ == {}


def test_adapter_rejects_invalid_input_name():
    def act(ctx, payload):
        return payload

    with pytest.raises(ValueError, match="valid Python identifier"):
        wrap_activity(act, "not an identifier")
    with pytest.raises(ValueError, match="valid Python identifier"):
        wrap_activity(act, "class")  # a keyword
