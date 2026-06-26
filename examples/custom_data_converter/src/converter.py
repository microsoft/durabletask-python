# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Pydantic-backed :class:`DataConverter` for the Durable Task Python SDK.

This is the heart of the example: a small, self-contained converter that plugs
a third-party serialization/validation library (`pydantic
<https://docs.pydantic.dev/>`_) into the SDK's pluggable serialization seam.

How the seam works
------------------
Both the worker and the client accept a ``data_converter`` argument. The SDK
routes **every** payload boundary -- orchestrator/activity/entity inputs and
outputs, external events, and custom status -- through that single converter,
so one object controls how Python values become JSON on the wire and how they
are reconstructed on the way back.

A converter implements three methods:

* ``serialize(value)``       -- Python value  -> JSON string (or ``None``).
* ``deserialize(data, t)``   -- JSON string    -> Python value, optionally
  reconstructed as ``t`` (the type the SDK learned from a function's
  annotation, a ``return_type=`` argument, or a typed client accessor).
* ``coerce(value, t)``       -- already-parsed value -> reconstructed as ``t``
  (used where the SDK already holds a parsed value, e.g. entity state).

It may also override one hook:

* ``is_reconstructable(t)``  -- tells the SDK's inbound type-discovery that an
  *input* annotated with type ``t`` should be handed to ``deserialize`` /
  ``coerce`` (rather than passed through as raw JSON). The default recognizes
  dataclasses and ``from_json()``-capable types; override it to add your own
  (here, pydantic models).

This converter recognizes :class:`pydantic.BaseModel` subclasses and uses
pydantic for them (gaining validation, aliasing, custom field types, etc.).
For everything else -- ``str``, ``int``, ``list``, dataclasses, ... -- it
delegates to the SDK's default :class:`JsonDataConverter`, so plugging it in
costs nothing for non-pydantic payloads. This "handle my types, delegate the
rest" shape is the recommended pattern for a real custom converter.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from durabletask.serialization import DataConverter, JsonDataConverter


def _is_model_type(target_type: Any) -> bool:
    """Return True when ``target_type`` is a pydantic model class."""
    return isinstance(target_type, type) and issubclass(target_type, BaseModel)


class PydanticDataConverter(DataConverter):
    """A :class:`DataConverter` that serializes pydantic models with pydantic.

    Pydantic models are serialized via ``model_dump_json()`` and reconstructed
    (with full validation) via ``model_validate_json()`` / ``model_validate()``
    whenever the SDK supplies the model type. Every other value falls through to
    the default :class:`JsonDataConverter`.
    """

    def __init__(self) -> None:
        # Delegate non-pydantic payloads to the SDK's built-in JSON codec.
        self._fallback = JsonDataConverter()

    def serialize(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, BaseModel):
            # ``model_dump_json`` honors pydantic field serializers, aliases,
            # and custom types (e.g. ``datetime`` -> ISO 8601) automatically.
            return value.model_dump_json()
        return self._fallback.serialize(value)

    def deserialize(self, data: str | None, target_type: type | None = None) -> Any:
        if data is None or data == "":
            return None
        if _is_model_type(target_type):
            # ``model_validate_json`` parses *and validates* the payload,
            # raising ``pydantic.ValidationError`` on malformed data.
            return target_type.model_validate_json(data)  # type: ignore[union-attr]
        return self._fallback.deserialize(data, target_type)

    def coerce(self, value: Any, target_type: type | None = None) -> Any:
        if value is None:
            return None
        if _is_model_type(target_type):
            return target_type.model_validate(value)  # type: ignore[union-attr]
        return self._fallback.coerce(value, target_type)

    def is_reconstructable(self, target_type: Any) -> bool:
        # Teach the SDK's inbound type-discovery that pydantic models are
        # reconstructable, so an orchestrator/activity input annotated with a
        # model type is rebuilt (and validated) by this converter instead of
        # arriving as a plain ``dict``. Delegating to ``super()`` keeps the
        # default behavior (dataclasses, ``from_json`` types, ``Optional`` /
        # ``list`` wrappers, builtins excluded) for everything else; because the
        # base recurses through ``self.is_reconstructable``, ``list[OrderItem]``
        # and ``Optional[Order]`` are recognized too.
        if _is_model_type(target_type):
            return True
        return super().is_reconstructable(target_type)
