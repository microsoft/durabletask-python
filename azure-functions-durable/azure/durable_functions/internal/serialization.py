# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Azure Functions payload serialization for Durable Task.

Bridges durabletask's pluggable :class:`~durabletask.serialization.DataConverter`
to the azure-functions SDK's centralized ``df_dumps`` / ``df_loads`` serializers
so that payloads round-trip through the **exact** wire format the Durable
Functions host extension (and the SDK's ``ActivityTriggerConverter``) expect.
"""

from __future__ import annotations
import inspect
from threading import Lock
from azure.functions._durable_functions import df_dumps, df_loads

from typing import Any, Callable, Protocol, TypeGuard, override

from durabletask.serialization import JsonDataConverter


class _ConfigurableJsonEncoder(Protocol):
    def __call__(self, value: Any, *, ensure_ascii: bool = True) -> str:
        ...


_serialization_lock = Lock()
_ensure_ascii: bool | None = None
_unicode_encoder: _ConfigurableJsonEncoder | None = None


def _supports_ensure_ascii(
        encoder: Callable[[Any], str]) -> TypeGuard[_ConfigurableJsonEncoder]:
    parameter = inspect.signature(encoder).parameters.get("ensure_ascii")
    return parameter is not None and parameter.kind in (
        inspect.Parameter.KEYWORD_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    )


def configure_serialization(*, ensure_ascii: object = True) -> None:
    """Select one JSON encoding policy before durable payload serialization."""
    global _ensure_ascii, _unicode_encoder
    if not isinstance(ensure_ascii, bool):
        raise TypeError("ensure_ascii must be a bool")
    with _serialization_lock:
        if _ensure_ascii is not None and _ensure_ascii != ensure_ascii:
            raise ValueError(
                "JSON serialization is already configured with "
                f"ensure_ascii={_ensure_ascii} in this process; configure "
                "serialization once before serializing durable payloads")
        if not ensure_ascii:
            encoder = df_dumps
            if not _supports_ensure_ascii(encoder):
                raise RuntimeError(
                    "ensure_ascii=False requires an azure-functions SDK whose "
                    "df_dumps supports the ensure_ascii keyword argument. "
                    "Install a compatible azure-functions SDK before opting in.")
            _unicode_encoder = encoder
        _ensure_ascii = ensure_ascii


def serialize_json(value: Any) -> str:
    """Apply the process-wide policy using the shared Functions JSON codec."""
    global _ensure_ascii
    with _serialization_lock:
        if _ensure_ascii is None:
            _ensure_ascii = True
        ensure_ascii = _ensure_ascii
        encoder = _unicode_encoder
    if ensure_ascii:
        # Older shared SDKs do not accept the optional keyword argument.
        return df_dumps(value)
    if encoder is None:
        raise RuntimeError("Unicode JSON serialization has no configured encoder")
    return encoder(value, ensure_ascii=False)


class FunctionsDataConverter(JsonDataConverter):
    """:class:`DataConverter` that serializes via azure-functions' codec.

    Overrides the string boundary (:meth:`serialize` / :meth:`deserialize`) to
    route through ``df_dumps`` / ``df_loads`` -- producing the format that the
    Durable Functions host expects.

    :meth:`coerce` and :meth:`can_reconstruct` are overridden so the codec's
    type rules apply uniformly. :meth:`coerce` round-trips the value through
    ``serialize`` / ``deserialize`` rather than the base converter's permissive
    value-level reconstruction, so a coercion is validated exactly like a wire
    payload. :meth:`can_reconstruct` returns ``True`` so type discovery always
    hands the declared type to ``deserialize`` and lets ``df_loads`` decide what
    it can reconstruct, instead of gating on the base converter's narrower
    dataclass / ``from_json`` policy.
    """

    @override
    def serialize(self, value: Any) -> str | None:
        if value is None:
            return None
        return serialize_json(value)

    @override
    def deserialize(self, data: str | None, target_type: type | None = None) -> Any:
        if data is None or data == "":
            return None
        return df_loads(data, expected_type=target_type)

    @override
    def coerce(self, value: Any, target_type: type | None = None) -> Any:
        if value is None or target_type is None:
            return value
        return self.deserialize(self.serialize(value), target_type)

    @override
    def can_reconstruct(self, target_type: Any) -> bool:
        return True


# Shared instance: the converter is stateless, so a single instance is reused
# across the per-invocation worker/client objects.
DEFAULT_FUNCTIONS_DATA_CONVERTER: FunctionsDataConverter = FunctionsDataConverter()
