# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Pluggable serialization for Durable Task payloads.

All user payloads (orchestrator/activity/entity inputs and outputs, external
event data, custom status, and entity state) flow through a
:class:`DataConverter`. The worker and client both accept a converter and share
it across every serialization boundary, so a single object controls how Python
values become JSON on the wire and how they are reconstructed on the way back.

The default :class:`JsonDataConverter` preserves the SDK's built-in behavior:
builtins serialize as plain JSON, dataclasses / ``SimpleNamespace`` instances
and objects exposing a ``to_json()`` hook serialize to plain JSON structures,
and a caller-supplied ``target_type`` drives reconstruction on the read side
(the destination type is never read from the payload).

To customize serialization -- for example to validate with pydantic, encode
custom ``datetime`` / ``Decimal`` formats, or integrate another model framework
-- implement :class:`DataConverter` and pass it to the worker and client::

    converter = MyDataConverter()
    worker = TaskHubGrpcWorker(data_converter=converter)
    client = TaskHubGrpcClient(data_converter=converter)
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from durabletask.internal import shared

logger = logging.getLogger("durabletask")


class DataConverter(ABC):
    """Strategy for serializing and deserializing Durable Task payloads.

    Implementations are used by both the worker and the client and must be
    deterministic: the same value must always serialize to the same string so
    that orchestration replay stays consistent.
    """

    @abstractmethod
    def serialize(self, value: Any) -> str | None:
        """Serialize ``value`` to a string, or ``None`` when ``value`` is ``None``."""
        ...

    @abstractmethod
    def deserialize(self, data: str | None, target_type: type | None = None) -> Any:
        """Deserialize ``data``, optionally coercing the result to ``target_type``.

        ``data`` is ``None`` (or empty) when there is no payload, in which case
        ``None`` is returned. When ``target_type`` is provided the result is
        reconstructed as that type; otherwise the raw deserialized value is
        returned. The destination type is always supplied by the caller and is
        never derived from the payload.

        Whether a failure to coerce to ``target_type`` raises or falls back to
        the raw value is an implementation choice. The default
        :class:`JsonDataConverter` is best-effort and falls back; a validating
        converter may instead raise.
        """
        ...


class JsonDataConverter(DataConverter):
    """Default :class:`DataConverter` backed by the SDK's JSON codec.

    Serialization emits plain JSON. Custom objects may opt in by exposing a
    ``to_json()`` method (called as ``type(obj).to_json(obj)``, so both instance
    methods and ``@staticmethod`` hooks work) and a ``from_json(value)``
    classmethod used during type-directed reconstruction. This matches the
    ``to_json`` / ``from_json`` convention used by ``azure-functions-durable``.

    Deserialization is **best-effort**: when a ``target_type`` is supplied and
    the payload cannot be coerced to it, the raw deserialized value is returned
    (and a debug message is logged) rather than raising. This keeps the core SDK
    permissive; a stricter, validating converter can be supplied for callers who
    want coercion failures to surface as errors.
    """

    def serialize(self, value: Any) -> str | None:
        if value is None:
            return None
        return shared.to_json(value)

    def deserialize(self, data: str | None, target_type: type | None = None) -> Any:
        if data is None or data == "":
            return None
        if target_type is None:
            return shared.from_json(data)
        try:
            return shared.from_json(data, target_type)
        except Exception as e:
            # Best-effort: fall back to the raw deserialized value rather than
            # failing the operation. Logged so the mismatch remains discoverable.
            logger.debug(
                "Could not coerce payload to '%s' (%s); returning the raw "
                "deserialized value.",
                getattr(target_type, "__name__", target_type), e,
            )
            return shared.from_json(data)


# Shared default instance used when no converter is supplied.
DEFAULT_DATA_CONVERTER: DataConverter = JsonDataConverter()
