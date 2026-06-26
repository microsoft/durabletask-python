# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Internal JSON codec for Durable Task payloads.

This module holds the low-level serialization *mechanism* -- the JSON string
encode/decode primitives and the value-level type coercion used to reconstruct
custom objects. Serialization *policy* (the public, pluggable strategy) lives in
:mod:`durabletask.serialization`; the default ``JsonDataConverter`` is the only
production consumer of ``to_json`` / ``from_json``, while ``coerce_to_type`` is
also used directly by entity state accessors that already hold a parsed value.
"""

from __future__ import annotations

import dataclasses
import json
import types
import typing
from collections.abc import Sequence
from types import SimpleNamespace
from typing import Any, cast

# Marker formerly added to JSON payloads to flag objects for automatic
# deserialization into a SimpleNamespace. New code no longer emits this marker
# (objects are serialized as plain JSON), but the decoder still recognizes it so
# that orchestration histories produced by older SDK versions continue to replay.
AUTO_SERIALIZED = "__durabletask_autoobject__"


def to_json(obj: Any) -> str:
    """Serialize a value to a JSON string.

    Builtins serialize to plain JSON. Dataclasses, ``SimpleNamespace``
    instances, and objects exposing a ``to_json()`` method are serialized to
    plain JSON as well (without any type marker); custom objects can be
    reconstructed on the receiving side by passing ``expected_type`` to
    :func:`from_json`.
    """
    try:
        return json.dumps(obj, default=_encode_custom_object)
    except TypeError as e:
        # Preserve the original error as the cause so serialization failures are
        # easier to diagnose, while naming the offending top-level type.
        raise TypeError(
            f"Failed to serialize object of type '{type(obj).__name__}' to JSON: {e}"
        ) from e


def from_json(json_str: str | bytes | bytearray, expected_type: type | None = None) -> Any:
    """Deserialize a JSON string, optionally coercing the result to a type.

    When ``expected_type`` is ``None`` (the default) the raw parsed JSON is
    returned. For backwards compatibility, payloads carrying the legacy
    :data:`AUTO_SERIALIZED` marker are reconstructed as ``SimpleNamespace``
    instances so that in-flight orchestrations produced by older SDK versions
    continue to replay.

    When ``expected_type`` is provided, the legacy marker (if present) is
    stripped and the parsed value is coerced to ``expected_type`` -- dataclasses
    are constructed from their dict payloads, types exposing a ``from_json()``
    classmethod are reconstructed via that hook, and ``Optional``/``Union`` and
    ``list`` type hints are honored recursively. The destination type is always
    supplied by the caller; it is never read from the payload.
    """
    if expected_type is None:
        return json.loads(json_str, object_hook=_legacy_object_hook)
    raw = json.loads(json_str, object_hook=_strip_legacy_marker)
    return coerce_to_type(raw, expected_type)


def _encode_custom_object(o: Any) -> Any:
    """``default`` hook for :func:`json.dumps` that emits plain JSON.

    Called only for values the JSON encoder cannot natively serialize. Note that
    namedtuples are handled natively by the encoder (serialized as JSON arrays)
    and never reach this hook.
    """
    # Custom objects may opt in via a ``to_json`` hook. It is resolved off the
    # type and called with the instance (``type(o).to_json(o)``) so that both
    # instance methods and ``@staticmethod`` hooks work -- matching the calling
    # convention used by ``azure-functions-durable``. The hook returns a
    # JSON-serializable value (a structure or a string), not a JSON document.
    #
    # The hook is checked before the dataclass / ``SimpleNamespace`` branches so
    # a type may override the default structural encoding -- mirroring the read
    # path, where :func:`coerce_to_type` consults ``from_json`` before its
    # dataclass branch. This matters for dataclasses whose fields are not
    # JSON-native (e.g. ``timedelta`` / ``datetime``), which ``asdict`` alone
    # cannot serialize.
    to_json_hook = getattr(cast(Any, type(o)), "to_json", None)
    if callable(to_json_hook):
        return to_json_hook(o)
    if dataclasses.is_dataclass(o) and not isinstance(o, type):
        return dataclasses.asdict(o)
    if isinstance(o, SimpleNamespace):
        return vars(o)
    # This will raise a TypeError describing the unsupported type.
    raise TypeError(f"Object of type '{type(o).__name__}' is not JSON serializable")


def _legacy_object_hook(d: dict[str, Any]) -> Any:
    # If the object carries the legacy marker, deserialize it as a SimpleNamespace.
    if d.pop(AUTO_SERIALIZED, False):
        return SimpleNamespace(**d)
    return d


def _strip_legacy_marker(d: dict[str, Any]) -> dict[str, Any]:
    # Discard the legacy marker so typed coercion sees a plain dict.
    d.pop(AUTO_SERIALIZED, None)
    return d


def coerce_to_type(value: Any, expected_type: Any) -> Any:
    """Coerce an already-parsed JSON value to ``expected_type``.

    Handles ``None``/``Optional``/``Union`` and ``list`` type hints recursively,
    types exposing a ``from_json()`` classmethod, and dataclasses (including
    nested dataclass fields). The destination type is always caller-supplied and
    never derived from the payload, keeping deserialization secure.
    """
    if expected_type is None or value is None:
        return value

    origin = typing.get_origin(expected_type)
    if origin is not None:
        return _coerce_generic(value, expected_type, origin)

    if not isinstance(expected_type, type):
        # Not a concrete, instantiable type (e.g. a typing special form we don't
        # special-case) -- return the value unchanged.
        return value

    if isinstance(value, expected_type):
        return value

    from_json_hook = getattr(expected_type, "from_json", None)
    if callable(from_json_hook):
        return from_json_hook(value)

    if dataclasses.is_dataclass(expected_type) and isinstance(value, dict):
        return _build_dataclass(expected_type, cast(dict[str, Any], value))

    type_ctor = cast(Any, expected_type)
    try:
        return type_ctor(value)
    except Exception as e:
        type_name = getattr(type_ctor, "__name__", None) or str(type_ctor)
        raise TypeError(
            f"Could not coerce value of type '{type(value).__name__}' to "
            f"'{type_name}'"
        ) from e


def _coerce_generic(value: Any, expected_type: Any, origin: Any) -> Any:
    args = typing.get_args(expected_type)
    if origin is typing.Union or origin is types.UnionType:
        # If the value already matches a member type, keep it as-is.
        non_none = [a for a in args if a is not type(None)]
        for arg in non_none:
            if isinstance(arg, type) and isinstance(value, arg):
                return value
        # ``Optional[T]`` (exactly one non-None member): coerce to that member.
        # For a genuine multi-member ``Union`` where the value matched none of
        # the members, leave it untouched rather than guessing the first arg --
        # forcing a coercion there can silently mis-construct the wrong type.
        if len(non_none) == 1:
            return coerce_to_type(value, non_none[0])
        return value
    if origin in (list, Sequence) and isinstance(value, list):
        elem_type = args[0] if args else None
        return [coerce_to_type(item, elem_type) for item in cast(list[Any], value)]
    # Other generics (dict, tuple, ...) are returned as parsed JSON.
    return value


def _build_dataclass(cls: Any, data: dict[str, Any]) -> Any:
    """Construct a dataclass from its dict payload, recursing into typed fields."""
    try:
        hints = typing.get_type_hints(cls)
    except Exception:
        hints = {}
    kwargs: dict[str, Any] = {}
    for field in dataclasses.fields(cls):
        if field.name not in data:
            continue
        field_type = hints.get(field.name)
        kwargs[field.name] = coerce_to_type(data[field.name], field_type)
    return cls(**kwargs)
