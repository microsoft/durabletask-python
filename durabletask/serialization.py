# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Pluggable serialization for Durable Task payloads.

All user payloads (orchestrator/activity/entity inputs and outputs, external
event data, custom status, and entity state) flow through a
:class:`DataConverter`. The worker and client both accept a converter and share
it across every serialization boundary, so a single object controls how Python
values become JSON on the wire and how they are reconstructed on the way back.

The default :class:`JsonDataConverter` preserves the SDK's built-in behavior:
builtins serialize as plain JSON; objects exposing a ``to_json()`` hook, then
dataclasses, then ``SimpleNamespace`` instances serialize to plain JSON
structures; and a caller-supplied ``target_type`` drives reconstruction on the
read side (the destination type is never read from the payload).

To customize serialization -- for example to validate with pydantic, encode
custom ``datetime`` / ``Decimal`` formats, or integrate another model framework
-- implement :class:`DataConverter` and pass it to the worker and client::

    converter = MyDataConverter()
    worker = TaskHubGrpcWorker(data_converter=converter)
    client = TaskHubGrpcClient(data_converter=converter)

This module is the single home for both serialization *policy* (the public,
pluggable :class:`DataConverter` strategy) and the low-level JSON codec
*mechanism* (the private ``_to_json`` / ``_from_json`` / ``_coerce_to_type``
helpers). The mechanism is intentionally private: the supported, stable surface
is the :class:`DataConverter` abstraction.
"""

from __future__ import annotations

import dataclasses
import enum
import functools
import inspect
import json
import logging
import sys
import types
import typing
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from types import SimpleNamespace
from typing import Any, cast

logger = logging.getLogger("durabletask")

# Marker formerly added to JSON payloads to flag objects for automatic
# deserialization into a SimpleNamespace. New code no longer emits this marker
# (objects are serialized as plain JSON), but the decoder still recognizes it so
# that orchestration histories produced by older SDK versions continue to
# replay. Internal detail; re-exported from ``durabletask.internal.shared`` only
# for backwards compatibility.
_AUTO_SERIALIZED = "__durabletask_autoobject__"


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

    @abstractmethod
    def coerce(self, value: Any, target_type: type | None = None) -> Any:
        """Coerce an **already-deserialized** ``value`` to ``target_type``.

        Unlike :meth:`deserialize`, the input is a live Python value rather than
        a serialized string. Used where the SDK holds a parsed value (for
        example, durable entity state during a batch) and needs to reconstruct
        the caller's requested type without re-serializing. When ``target_type``
        is ``None`` the value is returned unchanged. The same coercion policy
        (strict vs. best-effort) that an implementation applies in
        :meth:`deserialize` should apply here.
        """
        ...

    def can_reconstruct(self, target_type: Any) -> bool:
        """Return True if this converter can rebuild ``target_type`` from a payload.

        Inbound type-discovery calls this to decide whether a function's
        annotated *input* type (or an activity's *return* annotation) should be
        passed to :meth:`deserialize` / :meth:`coerce`. When it returns ``False``
        the SDK passes the raw deserialized payload through unchanged -- this
        gate is what stops the SDK from invoking reconstruction on a type the
        converter does not actually handle.

        The base implementation is conservative and returns ``False``: a
        converter makes no reconstruction claims unless it opts in.
        :class:`JsonDataConverter` overrides this to recognize the types its
        codec can rebuild (dataclasses and ``from_json()``-capable types, plus
        ``Optional`` / ``list`` / ``Sequence`` hints wrapping them). Override
        this in a custom converter to teach the SDK about its own types (for
        example ``pydantic.BaseModel`` subclasses) so that inputs annotated with
        them are reconstructed instead of arriving as raw JSON.
        """
        return False


class JsonDataConverter(DataConverter):
    """Default :class:`DataConverter` backed by the SDK's JSON codec.

    Serialization emits plain JSON. Custom objects may opt in by exposing a
    ``to_json()`` method (called as ``type(obj).to_json(obj)``, so both instance
    methods and ``@staticmethod`` hooks work) and a ``from_json(value)``
    classmethod used during type-directed reconstruction. This matches the
    ``to_json`` / ``from_json`` convention used by ``azure-functions-durable``.

    The ``to_json`` hook takes precedence over the built-in dataclass /
    ``SimpleNamespace`` handling, and nested values are encoded recursively, so
    a dataclass (or any object) with a ``to_json`` hook -- including one nested
    inside another dataclass, ``list``, or ``dict`` -- round-trips through its
    own hooks.

    Deserialization (and value-level :meth:`coerce`) is **best-effort**: when a
    ``target_type`` is supplied and the value cannot be coerced to it, the raw
    value is returned (and a debug message is logged) rather than raising. This
    keeps the core SDK permissive; a stricter, validating converter can be
    supplied for callers who want coercion failures to surface as errors.

    > [!NOTE]
    > Type-directed reconstruction recurses through dataclass fields,
    > ``list``/``Sequence``, ``dict``/``Mapping`` values, ``tuple`` elements,
    > and ``Optional`` / ``Union`` hints. A type that exposes a custom
    > ``from_json()`` classmethod is responsible for reconstructing its own
    > nested values. To help, the converter passes itself to ``from_json`` when
    > the hook declares a second parameter -- ``from_json(cls, value, converter)``
    > -- so the hook can call ``converter.coerce(nested_value, NestedType)`` (or
    > ``converter.deserialize(...)``) to reconstruct nested values the built-in
    > recursion does not cover. The SDK never infers nested types from the
    > payload; the destination type is always supplied by the caller.
    """

    def serialize(self, value: Any) -> str | None:
        if value is None:
            return None
        return _to_json(value)

    def deserialize(self, data: str | None, target_type: type | None = None) -> Any:
        if data is None or data == "":
            return None
        if target_type is None:
            return _from_json(data)
        try:
            return _from_json(data, target_type, converter=self)
        except Exception as e:
            # Best-effort: fall back to the raw deserialized value rather than
            # failing the operation. Logged so the mismatch remains discoverable.
            self._log_coercion_fallback(target_type, e)
            return _from_json(data)

    def coerce(self, value: Any, target_type: type | None = None) -> Any:
        if target_type is None or value is None:
            return value
        try:
            return _coerce_to_type(value, target_type, converter=self)
        except Exception as e:
            self._log_coercion_fallback(target_type, e)
            return value

    def can_reconstruct(self, target_type: Any) -> bool:
        return _can_reconstruct(self, target_type)

    @staticmethod
    def _log_coercion_fallback(target_type: type, error: Exception) -> None:
        logger.debug(
            "Could not coerce payload to '%s' (%s); returning the raw "
            "deserialized value.",
            getattr(target_type, "__name__", target_type), error,
        )


# Shared default instance used when no converter is supplied.
DEFAULT_DATA_CONVERTER: DataConverter = JsonDataConverter()


# ---------------------------------------------------------------------------
# Private JSON codec mechanism.
#
# These are the low-level encode/decode primitives and the value-level type
# coercion used to reconstruct custom objects. They are deliberately private:
# the supported, pluggable surface is :class:`DataConverter`. ``_coerce_to_type``
# is also used internally by entity state accessors that already hold a parsed
# value.
# ---------------------------------------------------------------------------


def _can_reconstruct(converter: DataConverter, target_type: Any) -> bool:
    """:class:`JsonDataConverter`'s reconstruction policy.

    Recognizes dataclasses and ``from_json()``-capable types, plus ``Optional``
    / ``list`` / ``Sequence`` hints wrapping them; builtins and unknown
    annotations are excluded. Recurses through ``converter.can_reconstruct``
    (not itself) so a :class:`JsonDataConverter` subclass that overrides
    ``can_reconstruct`` still participates in the element-type checks of
    ``Optional`` / ``list`` hints.
    """
    origin = typing.get_origin(target_type)
    if origin is not None:
        args = typing.get_args(target_type)
        if origin is typing.Union or origin is types.UnionType:
            return any(
                converter.can_reconstruct(a) for a in args if a is not type(None)
            )
        if origin in (list, Sequence):
            return any(converter.can_reconstruct(a) for a in args)
        return False
    if not isinstance(target_type, type):
        return False
    if dataclasses.is_dataclass(target_type):
        return True
    return callable(getattr(cast(Any, target_type), "from_json", None))


def _to_json(obj: Any) -> str:
    """Serialize a value to a JSON string.

    Builtins serialize to plain JSON. Objects exposing a ``to_json()`` method,
    dataclasses, and ``SimpleNamespace`` instances are serialized to plain JSON
    as well (without any type marker), recursively. Custom objects can be
    reconstructed on the receiving side by passing ``expected_type`` to
    :func:`_from_json`.
    """
    try:
        return json.dumps(obj, default=_encode_custom_object)
    except TypeError as e:
        # Preserve the original error as the cause so serialization failures are
        # easier to diagnose, while naming the offending top-level type.
        raise TypeError(
            f"Failed to serialize object of type '{type(obj).__name__}' to JSON: {e}"
        ) from e


def _from_json(
    json_str: str | bytes | bytearray,
    expected_type: type | None = None,
    converter: DataConverter | None = None,
) -> Any:
    """Deserialize a JSON string, optionally coercing the result to a type.

    When ``expected_type`` is ``None`` (the default) the raw parsed JSON is
    returned. For backwards compatibility, payloads carrying the legacy
    :data:`_AUTO_SERIALIZED` marker are reconstructed as ``SimpleNamespace``
    instances so that in-flight orchestrations produced by older SDK versions
    continue to replay.

    When ``expected_type`` is provided, the legacy marker (if present) is
    stripped and the parsed value is coerced to ``expected_type`` -- dataclasses
    are constructed from their dict payloads, types exposing a ``from_json()``
    classmethod are reconstructed via that hook, and ``Optional``/``Union``,
    ``list``, ``dict``, and ``tuple`` type hints are honored recursively. The
    destination type is always supplied by the caller; it is never read from the
    payload.

    ``converter`` is threaded through to ``from_json`` hooks that declare a
    second parameter, letting them recursively reconstruct nested values.
    """
    if expected_type is None:
        return json.loads(json_str, object_hook=_legacy_object_hook)
    raw = json.loads(json_str, object_hook=_strip_legacy_marker)
    return _coerce_to_type(raw, expected_type, converter)


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
    # The hook is checked *before* the dataclass / ``SimpleNamespace`` branches
    # so a dataclass (or namespace) that needs custom handling -- for example
    # one with a field that is not JSON-serializable on its own -- can opt in.
    # Resolving off the type (rather than the instance) avoids mistaking a data
    # attribute named ``to_json`` for a hook, and mirrors the decode side, which
    # prefers ``from_json`` over the dataclass branch.
    to_json_hook = getattr(cast(Any, type(o)), "to_json", None)
    if callable(to_json_hook):
        return to_json_hook(o)
    if isinstance(o, enum.Enum):
        # Emit the member's underlying value (a primitive). Reconstruction is
        # type-directed: passing the enum type to ``deserialize`` rebuilds the
        # member via ``EnumType(value)``. ``IntEnum`` / ``IntFlag`` members are
        # ints and serialize natively without reaching this hook; this branch
        # covers string- and other-valued enums so they round-trip too. Emitting
        # only the value (never the member name or type) keeps the wire format a
        # plain primitive and avoids leaking the Python type into the payload.
        return o.value
    if dataclasses.is_dataclass(o) and not isinstance(o, type):
        # Return a *shallow* mapping of the dataclass's fields rather than
        # ``dataclasses.asdict``. asdict recursively converts nested dataclasses
        # to plain dicts (bypassing their ``to_json`` hooks) and deep-copies
        # every leaf value. Emitting the live field values lets ``json.dumps``
        # recurse so each nested value is encoded through this same hook,
        # honoring nested ``to_json`` hooks and avoiding the deep copy.
        return {f.name: getattr(o, f.name) for f in dataclasses.fields(o)}
    if isinstance(o, SimpleNamespace):
        return vars(o)
    # This will raise a TypeError describing the unsupported type.
    raise TypeError(f"Object of type '{type(o).__name__}' is not JSON serializable")


def _legacy_object_hook(d: dict[str, Any]) -> Any:
    # If the object carries the legacy marker, deserialize it as a SimpleNamespace.
    if d.pop(_AUTO_SERIALIZED, False):
        return SimpleNamespace(**d)
    return d


def _strip_legacy_marker(d: dict[str, Any]) -> dict[str, Any]:
    # Discard the legacy marker so typed coercion sees a plain dict.
    d.pop(_AUTO_SERIALIZED, None)
    return d


def _coerce_to_type(value: Any, expected_type: Any, converter: DataConverter | None = None) -> Any:
    """Coerce an already-parsed JSON value to ``expected_type``.

    Handles ``None``/``Optional``/``Union``, ``list``, ``dict``, and ``tuple``
    type hints recursively, types exposing a ``from_json()`` classmethod, and
    dataclasses (including nested dataclass fields). The destination type is
    always caller-supplied and never derived from the payload, keeping
    deserialization secure.

    ``converter`` is passed to ``from_json`` hooks that declare a second
    parameter so they can recursively reconstruct nested values.
    """
    if expected_type is None or value is None:
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


def _invoke_from_json(hook: Any, value: Any, converter: DataConverter | None) -> Any:
    """Call a ``from_json`` hook, passing the converter when the hook accepts it.

    A hook declared as ``from_json(cls, value)`` (the common, single-argument
    convention) is called with just the value. A hook declared as
    ``from_json(cls, value, converter)`` additionally receives the active
    :class:`DataConverter`, letting it reconstruct nested values recursively via
    ``converter.coerce(...)`` / ``converter.deserialize(...)``.

    > [!NOTE]
    > ``from_json`` must be a ``@classmethod`` or ``@staticmethod`` -- the hook
    > is resolved off the *type* (no instance exists yet during reconstruction).
    > A plain instance method would have ``self`` consume the value and is
    > unsupported regardless of the converter-arity detection below.
    """
    if converter is not None and _hook_accepts_converter(hook):
        return hook(value, converter)
    return hook(value)


@functools.lru_cache(maxsize=2048)
def _hook_accepts_converter(hook: Any) -> bool:
    """Return True if a bound ``from_json`` hook can accept a second argument.

    The hook is inspected as accessed off the type (``cls``/``self`` already
    bound), so a classmethod ``from_json(cls, value, converter)`` presents as
    ``(value, converter)``. Results are cached because reconstruction runs on
    hot paths; bound classmethods hash equal across attribute accesses, so the
    cache stays effective and bounded.
    """
    try:
        sig = inspect.signature(hook)
    except (TypeError, ValueError):
        return False
    positional = 0
    for param in sig.parameters.values():
        if param.kind in (inspect.Parameter.POSITIONAL_ONLY,
                          inspect.Parameter.POSITIONAL_OR_KEYWORD):
            positional += 1
        elif param.kind is inspect.Parameter.VAR_POSITIONAL:
            return True
    return positional >= 2


def _coerce_generic(value: Any, expected_type: Any, origin: Any,
                    converter: DataConverter | None = None) -> Any:
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
            return _coerce_to_type(value, non_none[0], converter)
        return value
    if origin in (list, Sequence) and isinstance(value, list):
        elem_type = args[0] if args else None
        return [_coerce_to_type(item, elem_type, converter) for item in cast(list[Any], value)]
    if origin in (dict, Mapping) and isinstance(value, dict):
        # JSON object keys are always strings, so only the values can carry a
        # reconstructable type. Keys are passed through unchanged.
        mapping = cast("dict[Any, Any]", value)
        val_type = args[1] if len(args) == 2 else None
        if val_type is None:
            return mapping
        return {k: _coerce_to_type(v, val_type, converter) for k, v in mapping.items()}
    if origin is tuple and isinstance(value, list):
        # Tuples serialize to JSON arrays, so the parsed value is a list.
        if not args:
            return tuple(cast(list[Any], value))
        # Homogeneous ``tuple[T, ...]``.
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(_coerce_to_type(item, args[0], converter) for item in cast(list[Any], value))
        # Fixed-length ``tuple[T1, T2, ...]`` -- coerce element-wise by position.
        arr = cast(list[Any], value)
        if len(arr) != len(args):
            raise TypeError(
                f"Could not coerce JSON array of length {len(arr)} to "
                f"tuple of length {len(args)}"
            )
        return tuple(_coerce_to_type(item, t, converter) for item, t in zip(arr, args))
    # Other generics are returned as parsed JSON.
    return value


def _build_dataclass(cls: Any, data: dict[str, Any],
                     converter: DataConverter | None = None) -> Any:
    """Construct a dataclass from its dict payload, recursing into typed fields."""
    try:
        hints = typing.get_type_hints(cls)
    except Exception:
        hints = {}
    globalns = _type_namespace(cls)
    kwargs: dict[str, Any] = {}
    for field in dataclasses.fields(cls):
        if field.name not in data:
            continue
        # ``get_type_hints`` on Python 3.10 does not deep-resolve forward
        # references nested inside container args (e.g. the ``"TreeNode"`` in
        # ``list["TreeNode"]`` on a self-referential dataclass), leaving a bare
        # string or ``ForwardRef`` that the coercion below would skip. Resolve
        # them against the class's defining module so reconstruction behaves the
        # same as it does on 3.11+.
        field_type = _resolve_forward_refs(hints.get(field.name), globalns)
        kwargs[field.name] = _coerce_to_type(data[field.name], field_type, converter)
    return cls(**kwargs)


def _type_namespace(cls: Any) -> dict[str, Any]:
    """Build the namespace used to resolve forward references in ``cls``'s hints.

    Forward references in a class's annotations are resolved against the
    module in which the class is defined, plus the class's own name (so a
    self-referential type like ``list["TreeNode"]`` resolves).
    """
    module = sys.modules.get(getattr(cls, "__module__", None) or "")
    ns: dict[str, Any] = dict(getattr(module, "__dict__", {}))
    name = getattr(cls, "__name__", None)
    if name:
        ns.setdefault(name, cls)
    return ns


def _resolve_forward_refs(tp: Any, globalns: dict[str, Any]) -> Any:
    """Resolve string / ``ForwardRef`` leaves in a type hint, recursing into args.

    Returns ``tp`` unchanged when it (or a nested name) cannot be resolved, so an
    unresolvable hint simply falls back to "leave the value as parsed JSON"
    rather than raising. Only the supported generic shapes (``Union``, ``list``,
    ``dict``, ``tuple``, etc.) are rebuilt; the destination type is still
    entirely caller-supplied, so this does not weaken the security model.
    """
    if isinstance(tp, str):
        try:
            tp = eval(tp, globalns)  # noqa: S307 - resolves caller-authored annotations
        except Exception:
            return tp
    elif isinstance(tp, typing.ForwardRef):
        try:
            tp = eval(tp.__forward_arg__, globalns)  # noqa: S307
        except Exception:
            return tp

    origin = typing.get_origin(tp)
    if origin is None:
        return tp
    args = typing.get_args(tp)
    if not args:
        return tp
    resolved = [_resolve_forward_refs(a, globalns) for a in args]
    if origin is typing.Union or origin is types.UnionType:
        return typing.Union[tuple(resolved)]
    try:
        return origin[tuple(resolved) if len(resolved) > 1 else resolved[0]]
    except TypeError:
        return tp
