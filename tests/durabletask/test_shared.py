# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the deprecated serialization shims in durabletask.internal.shared.

The JSON codec now lives in ``durabletask.serialization`` and its functions are
private; the supported surface is the pluggable ``DataConverter``. The thin
``shared.to_json`` / ``shared.from_json`` wrappers remain for backwards
compatibility but emit a ``DeprecationWarning``.
"""

import json
import warnings

import pytest

from durabletask.internal import shared


def test_shared_to_json_warns_and_serializes():
    with pytest.warns(DeprecationWarning, match="to_json"):
        result = shared.to_json({"a": 1})
    assert json.loads(result) == {"a": 1}


def test_shared_from_json_warns_and_deserializes():
    with pytest.warns(DeprecationWarning, match="from_json"):
        result = shared.from_json('{"a": 1}')
    assert result == {"a": 1}


def test_shared_from_json_still_accepts_expected_type():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        assert shared.from_json("42", int) == 42


def test_shared_auto_serialized_marker_still_exported():
    # The legacy marker constant remains importable for back-compat.
    assert shared.AUTO_SERIALIZED == "__durabletask_autoobject__"
