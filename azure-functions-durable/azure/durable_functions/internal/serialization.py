# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Azure Functions payload serialization for Durable Task.

Bridges durabletask's pluggable :class:`~durabletask.serialization.DataConverter`
to the azure-functions SDK's centralized ``df_dumps`` / ``df_loads`` serializers
so that payloads round-trip through the **exact** wire format the Durable
Functions host extension (and the SDK's ``ActivityTriggerConverter``) expect:
builtins as plain JSON, custom objects wrapped in the
``{"__class__", "__module__", "__data__"}`` envelope via their ``to_json`` /
``from_json`` hooks.

When the installed ``azure-functions`` package exposes ``df_dumps`` / ``df_loads``
(centralized serializers with optional type validation and strict-typing
support), they are used directly. On older releases that lack them we fall back
to the legacy ``_serialize_custom_object`` / ``_deserialize_custom_object`` hooks
-- the same behavior the SDK converter uses in those versions -- keeping both
sides symmetric. The wire format is unchanged either way.
"""

from __future__ import annotations

import importlib
import json
import logging
from typing import Any, Callable, Optional, cast

from durabletask.serialization import JsonDataConverter

logger = logging.getLogger("azure.functions.DurableFunctions")

# ``azure.functions`` only exposes its Durable serialization helpers from a
# private, untyped module. Resolve it dynamically and bind the symbols we need
# to locally-typed callables so the rest of the module stays type-checked.
_df_internal = importlib.import_module("azure.functions._durable_functions")
_serialize_custom_object: Callable[[Any], Any] = getattr(
    _df_internal, "_serialize_custom_object")
_deserialize_custom_object: Callable[[dict[str, Any]], Any] = getattr(
    _df_internal, "_deserialize_custom_object")

_FALLBACK_MESSAGE = (
    "The installed 'azure-functions' package does not provide the centralized "
    "'df_dumps' / 'df_loads' serializers. Durable Functions is falling back to "
    "the legacy serialization pipeline; the wire format is unchanged, but "
    "payload type validation (the 'expected_type' argument and strict typing "
    "mode) is unavailable. Upgrade to azure-functions>=1.26.0b4 to enable "
    "type-validated serialization."
)

_warned = False


def _warn_fallback_once() -> None:
    # Deferred to first use (debug level) rather than emitted at import time, so
    # users who never exercise the fallback path are not spammed.
    global _warned
    if not _warned:
        _warned = True
        logger.debug(_FALLBACK_MESSAGE)


def _fallback_df_dumps(value: Any) -> str:
    """Serialize ``value`` via the legacy custom-object hook."""
    _warn_fallback_once()
    return json.dumps(value, default=_serialize_custom_object)


def _fallback_df_loads(s: str, expected_type: Optional[type] = None) -> Any:
    """Deserialize ``s`` via the legacy custom-object hook.

    ``expected_type`` is accepted for call-site compatibility but ignored on
    this fallback path; type validation is only performed by the SDK's
    ``df_loads`` when it is available.
    """
    _warn_fallback_once()
    return json.loads(s, object_hook=_deserialize_custom_object)


# Prefer the SDK's centralized serializers; fall back to the legacy hooks when
# they are unavailable (older azure-functions releases).
_sdk_df_dumps = getattr(_df_internal, "df_dumps", None)
_sdk_df_loads = getattr(_df_internal, "df_loads", None)

df_dumps: Callable[[Any], str] = (
    cast("Callable[[Any], str]", _sdk_df_dumps)
    if callable(_sdk_df_dumps) else _fallback_df_dumps)
df_loads: Callable[..., Any] = (
    cast("Callable[..., Any]", _sdk_df_loads)
    if callable(_sdk_df_loads) else _fallback_df_loads)


class FunctionsDataConverter(JsonDataConverter):
    """:class:`DataConverter` that serializes via azure-functions' codec.

    Overrides only the string boundary (:meth:`serialize` / :meth:`deserialize`)
    to route through ``df_dumps`` / ``df_loads`` -- producing the
    ``{"__class__", "__module__", "__data__"}`` envelope that the Durable
    Functions host expects -- while inheriting :class:`JsonDataConverter`'s
    value-level :meth:`coerce` and reconstruction policy
    (:meth:`can_reconstruct`), which operate on already-parsed values and are
    wire-format agnostic.
    """

    def serialize(self, value: Any) -> str | None:
        if value is None:
            return None
        return df_dumps(value)

    def deserialize(self, data: str | None, target_type: type | None = None) -> Any:
        if data is None or data == "":
            return None
        return df_loads(data, target_type)


# Shared instance: the converter is stateless, so a single instance is reused
# across the per-invocation worker/client objects.
DEFAULT_FUNCTIONS_DATA_CONVERTER: FunctionsDataConverter = FunctionsDataConverter()
