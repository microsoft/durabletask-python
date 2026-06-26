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
import inspect
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


def from_json(json_str: str | bytes | bytearray, expected_type: type | None = None,
              converter: Any | None = None) -> Any:
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

    ``converter`` is the active :class:`~durabletask.serialization.DataConverter`.
    It is forwarded to ``from_json`` hooks that opt in (see :func:`coerce_to_type`)
    so they can reconstruct nested typed values via ``converter.coerce(...)``.
    """
    if expected_type is None:
        return json.loads(json_str, object_hook=_legacy_object_hook)
    raw = json.loads(json_str, object_hook=_strip_legacy_marker)
    return coerce_to_type(raw, expected_type, converter)


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
        # Shallow-convert to a dict whose *values are the original field objects*
        # (unlike ``dataclasses.asdict``, which deep-recurses and would convert a
        # nested dataclass via ``asdict`` -- bypassing that child's ``to_json``
        # hook). ``json.dumps`` then recurses into each value and re-enters this
        # hook for any nested custom object, so nested ``to_json`` hooks fire at
        # every depth (including inside lists/dicts).
        return {f.name: getattr(o, f.name) for f in dataclasses.fields(o)}
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


def coerce_to_type(value: Any, expected_type: Any, converter: Any | None = None) -> Any:
    """Coerce an already-parsed JSON value to ``expected_type``.

    Handles ``None``/``Optional``/``Union`` and ``list`` type hints recursively,
    types exposing a ``from_json()`` classmethod, and dataclasses (including
    nested dataclass fields). The destination type is always caller-supplied and
    never derived from the payload, keeping deserialization secure.

    ``converter`` is the active :class:`~durabletask.serialization.DataConverter`.
    A ``from_json`` hook may opt in to receiving it (by accepting a second
    positional parameter) and delegate nested reconstruction back to the
    converter, e.g. ``converter.coerce(child, ChildType)``. This keeps hooks free
    of manual nested deserialization and routes children through the same policy.
    """
    if expected_type is None or value is None:
        return value

    # ``Any`` imposes no constraint -- and ``isinstance(x, Any)`` raises -- so
    # short-circuit before any type inspection below.
    if expected_type is typing.Any:
        return value

    origin = typing.get_origin(expected_type)
    if origin is not None:
        return _coerce_generic(value, expected_type, origin, converter)

    if not isinstance(expected_type, type):
        # Not a concrete, instantiable type (e.g. a typing special form we don't
        # special-case) -- return the value unchanged.
        return value

    if isinstance(value, expected_type):
        return value

    from_json_hook = getattr(expected_type, "from_json", None)
    if callable(from_json_hook):
        return _invoke_from_json(from_json_hook, value, converter)

    if dataclasses.is_dataclass(expected_type) and isinstance(value, dict):
        return _build_dataclass(expected_type, cast(dict[str, Any], value), converter)

    type_ctor = cast(Any, expected_type)
    try:
        return type_ctor(value)
    except Exception as e:
        type_name = getattr(type_ctor, "__name__", None) or str(type_ctor)
        raise TypeError(
            f"Could not coerce value of type '{type(value).__name__}' to "
            f"'{type_name}'"
        ) from e


def _invoke_from_json(from_json_hook: Any, value: Any, converter: Any | None) -> Any:
    """Invoke a ``from_json`` hook, passing the converter if the hook accepts it.

    Hooks may be declared as ``from_json(cls, value)`` (the original contract) or
    ``from_json(cls, value, converter)`` to opt into managed nested
    reconstruction. Arity is detected from the bound hook's signature, so the
    extra parameter is fully backwards compatible. When a converter-aware hook is
    found but no converter was threaded (e.g. a direct ``json_codec`` call), the
    shared default converter is resolved lazily so the hook always receives one.
    """
    wants_converter = False
    try:
        params = [
            p for p in inspect.signature(from_json_hook).parameters.values()
            if p.kind in (inspect.Parameter.POSITIONAL_ONLY,
                          inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        wants_converter = len(params) >= 2
    except (TypeError, ValueError):
        wants_converter = False

    if wants_converter:
        if converter is None:
            converter = _default_converter()
        return from_json_hook(value, converter)
    return from_json_hook(value)


def _default_converter() -> Any:
    # Lazy import to avoid a load-time cycle: ``serialization`` imports this
    # module at import time, but by the time a hook actually runs both modules
    # are fully initialized.
    from durabletask.serialization import DEFAULT_DATA_CONVERTER
    return DEFAULT_DATA_CONVERTER


def _coerce_generic(value: Any, expected_type: Any, origin: Any, converter: Any | None = None) -> Any:
    args = typing.get_args(expected_type)
    if origin is typing.Union or origin is types.UnionType:
        # If the value already matches a member type, keep it as-is.
        non_none = [a for a in args if a is not type(None)]
        for arg in non_none:
            # An ``Any`` member imposes no constraint (and ``isinstance(x, Any)``
            # raises), so the value is acceptable as-is.
            if arg is typing.Any:
                return value
            if isinstance(arg, type) and isinstance(value, arg):
                return value
        # ``Optional[T]`` (exactly one non-None member): coerce to that member.
        # For a genuine multi-member ``Union`` where the value matched none of
        # the members, leave it untouched rather than guessing the first arg --
        # forcing a coercion there can silently mis-construct the wrong type.
        if len(non_none) == 1:
            return coerce_to_type(value, non_none[0], converter)
        return value
    if origin in (list, Sequence) and isinstance(value, list):
        elem_type = args[0] if args else None
        return [coerce_to_type(item, elem_type, converter) for item in cast(list[Any], value)]
    # Other generics (dict, tuple, ...) are returned as parsed JSON.
    return value


def _build_dataclass(cls: Any, data: dict[str, Any], converter: Any | None = None) -> Any:
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
        kwargs[field.name] = coerce_to_type(data[field.name], field_type, converter)
    return cls(**kwargs)
