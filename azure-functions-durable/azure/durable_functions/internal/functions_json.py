# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import importlib
import json
from typing import Any, Callable

from durabletask.internal import shared

# ``azure.functions`` only exposes its custom-object (de)serialization helpers
# from a private module, and they are untyped. Resolve them dynamically and
# bind them to locally-typed callables so the rest of the module stays fully
# type-checked.
_df_serializers = importlib.import_module("azure.functions._durable_functions")
_serialize_custom_object: Callable[[Any], Any] = getattr(
    _df_serializers, "_serialize_custom_object")
_deserialize_custom_object: Callable[[dict[str, Any]], Any] = getattr(
    _df_serializers, "_deserialize_custom_object")


def _to_json(obj: Any) -> str:
    return json.dumps(obj, default=_serialize_custom_object)


def _from_json(json_str: str | bytes | bytearray) -> Any:
    return json.loads(json_str, object_hook=_deserialize_custom_object)


def install_custom_serialization() -> None:
    """Replace durabletask's global JSON (de)serialization helpers.

    Routes ``durabletask`` payload serialization through azure-functions'
    custom-object (de)serializers so that user types round-trip consistently
    between the Functions host and the durabletask runtime.
    """
    shared.to_json = _to_json
    shared.from_json = _from_json
