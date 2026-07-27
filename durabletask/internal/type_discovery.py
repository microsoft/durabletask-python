# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Best-effort discovery of input type hints for user functions.

These helpers resolve the annotation of the *input* parameter of an
orchestrator, activity, or entity function so that inbound payloads can be
reconstructed into the annotated custom type without the caller having to pass
an explicit type.

Discovery is intentionally conservative: it only returns an annotation when the
active :class:`~durabletask.serialization.DataConverter` reports it as
*reconstructable* via :meth:`DataConverter.can_reconstruct`. The default
converter recognizes dataclasses, ``from_json()``-capable types, and ``Optional``
/ ``list`` hints wrapping them; a custom converter can recognize its own types
(e.g. ``pydantic.BaseModel``). Primitive and unknown annotations resolve to
``None`` so that existing payloads are passed through unchanged -- inbound type
discovery never invokes an arbitrary constructor on untrusted data, and never
alters the value for builtins.

All public helpers swallow exceptions and return ``None`` on failure; the caller
treats ``None`` as "no type information available" and uses the raw payload.
"""

from __future__ import annotations

import functools
import inspect
import typing
from typing import Any, Callable, NamedTuple

from durabletask.serialization import DEFAULT_DATA_CONVERTER, DataConverter


def _resolve_converter(converter: DataConverter | None) -> DataConverter:
    """Return the supplied converter, or the shared default when ``None``."""
    return converter if converter is not None else DEFAULT_DATA_CONVERTER


def _resolve_hints(fn: Callable[..., Any]) -> dict[str, Any] | None:
    """Resolve a function's type hints, honoring postponed annotations."""
    try:
        return typing.get_type_hints(fn)
    except Exception:
        return None


# Bounded so a worker that registers dynamically-created functions or closures
# cannot accumulate cache entries unboundedly over the process lifetime. The
# common case (a fixed set of module-level orchestrators/activities) fits well
# within this bound.
@functools.lru_cache(maxsize=2048)
def _resolved_hints(fn: Callable[..., Any]) -> dict[str, Any] | None:
    """Memoized :func:`_resolve_hints`.

    Results are memoized per function because discovery runs on every
    orchestrator/activity/entity execution (including replay).
    """
    return _resolve_hints(fn)


# Sentinel for "there is no annotation here worth asking the converter about"
# (parameter absent, unannotated, ``Any``, or an unresolvable string
# annotation). It is deliberately distinct from ``None`` because the two
# annotation paths treat a literal ``None`` differently, and both behaviours
# predate this cache:
#   * parameters -- a ``None`` annotation is a real annotation and is still
#     passed to the converter, so it must not collapse into the sentinel;
#   * return values -- ``activity_output_type()`` has always short-circuited on
#     ``annotation is None``, so ``_build_signature_info()`` normalizes a
#     ``-> None`` return annotation to this sentinel and it never reaches the
#     converter.
_NO_ANNOTATION: Any = object()


class _SignatureInfo(NamedTuple):
    """The converter-independent shape of a callable's signature.

    ``positional`` holds the resolved annotation of each positional parameter in
    declaration order, and ``return_annotation`` the resolved return annotation.
    Entries are :data:`_NO_ANNOTATION` when there is nothing to reconstruct.

    This depends only on the callable, so it is safe to memoize. The
    converter-dependent decision (:meth:`DataConverter.can_reconstruct`) is
    deliberately *not* part of it and stays at call time, so the same function
    discovered through two different converters still gets two answers.
    """

    positional: tuple[Any, ...]
    return_annotation: Any


def _resolve_annotation(raw: Any, name: str, hints: dict[str, Any] | None) -> Any:
    """Resolve one raw annotation, or return :data:`_NO_ANNOTATION`."""
    annotation: Any = raw
    if hints is not None and name in hints:
        annotation = hints[name]
    elif isinstance(annotation, str):
        # Could not resolve a postponed (string) annotation -- give up.
        return _NO_ANNOTATION

    if annotation is inspect.Parameter.empty or annotation is Any:
        return _NO_ANNOTATION
    return annotation


def _build_signature_info(fn: Any, *, memoized: bool) -> _SignatureInfo | None:
    """Inspect ``fn`` and resolve its annotations, or return ``None``."""
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return None

    hints = _resolved_hints(fn) if memoized else _resolve_hints(fn)
    positional = tuple(
        _resolve_annotation(p.annotation, p.name, hints)
        for p in sig.parameters.values()
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY,
                      inspect.Parameter.POSITIONAL_OR_KEYWORD)
    )
    return_annotation = _resolve_annotation(sig.return_annotation, "return", hints)
    if return_annotation is None:
        # ``activity_output_type()`` has always treated an explicit ``-> None``
        # as "nothing to reconstruct". Fold it into the sentinel here so the
        # special case stays out of the per-call path.
        return_annotation = _NO_ANNOTATION
    return _SignatureInfo(positional, return_annotation)


@functools.lru_cache(maxsize=2048)
def _memoized_signature_info(fn: Any) -> _SignatureInfo | None:
    return _build_signature_info(fn, memoized=True)


def _signature_info(fn: Any) -> _SignatureInfo | None:
    """Return ``fn``'s resolved signature shape, or ``None`` when unavailable.

    ``inspect.signature()`` and annotation resolution are comparatively
    expensive and their result depends only on the callable, so the result is
    memoized: discovery runs once per work item, and a worker executes the same
    registered functions over and over.
    """
    try:
        return _memoized_signature_info(fn)
    except TypeError:
        # Unhashable callable, so it cannot be used as a cache key. Fall back to
        # computing the shape on every call rather than failing.
        return _build_signature_info(fn, memoized=False)


def _input_annotation(fn: Callable[..., Any], position: int,
                      converter: DataConverter | None = None) -> Any | None:
    """Return the resolved annotation of the positional parameter at ``position``.

    ``position`` is the zero-based index among positional parameters (so the
    ``input`` parameter of a ``(ctx, input)`` function is at position 1, and the
    ``input`` parameter of an unbound ``(self, input)`` entity method is also at
    position 1). Returns ``None`` when the parameter is absent, unannotated, or
    its annotation is not reconstructable by ``converter``.
    """
    info = _signature_info(fn)
    if info is None or position >= len(info.positional):
        return None

    annotation = info.positional[position]
    if annotation is _NO_ANNOTATION:
        return None
    return annotation if _resolve_converter(converter).can_reconstruct(annotation) else None


def orchestrator_input_type(fn: Callable[..., Any],
                            converter: DataConverter | None = None) -> Any | None:
    """Discover the input type of an orchestrator function ``(ctx, input)``."""
    return _input_annotation(fn, 1, converter)


def activity_input_type(fn: Callable[..., Any],
                        converter: DataConverter | None = None) -> Any | None:
    """Discover the input type of an activity function ``(ctx, input)``."""
    return _input_annotation(fn, 1, converter)


def activity_output_type(fn: Any, converter: DataConverter | None = None) -> Any | None:
    """Discover the return type of an activity function.

    Returns the resolved return annotation when ``converter`` reports it as
    reconstructable (the default converter recognizes a dataclass or a
    ``from_json()``-capable type, optionally wrapped in ``Optional`` / ``list``).
    Returns ``None`` for plain callables that are not annotated with such a type,
    for string activity names, or when the annotation cannot be resolved.
    """
    if not callable(fn):
        return None

    info = _signature_info(fn)
    if info is None:
        return None

    annotation = info.return_annotation
    if annotation is _NO_ANNOTATION:
        return None
    return annotation if _resolve_converter(converter).can_reconstruct(annotation) else None


def entity_input_type(fn: Any, operation: str,
                      converter: DataConverter | None = None) -> Any | None:
    """Discover the input type of an entity operation.

    For class-based entities (a ``DurableEntity`` subclass) the operation is a
    method; its input is the first parameter after ``self``. For function-based
    entities the signature is ``(ctx, input)``. Returns ``None`` when no
    reconstructable input annotation is found.
    """
    if isinstance(fn, type):
        method = getattr(fn, operation, None)
        if method is None or not callable(method):
            return None
        # Unbound method includes ``self`` at position 0, so ``input`` is at 1.
        return _input_annotation(method, 1, converter)
    return _input_annotation(fn, 1, converter)
