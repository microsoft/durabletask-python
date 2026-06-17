# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Best-effort discovery of input type hints for user functions.

These helpers resolve the annotation of the *input* parameter of an
orchestrator, activity, or entity function so that inbound payloads can be
reconstructed into the annotated custom type (a dataclass or a type exposing a
``from_json()`` classmethod) without the caller having to pass an explicit type.

Discovery is intentionally conservative: it only returns an annotation when the
target is a *reconstructable* custom type (a dataclass, a ``from_json()``-capable
type, or an ``Optional`` / ``list`` wrapping one). Primitive and unknown
annotations resolve to ``None`` so that existing payloads are passed through
unchanged -- inbound type discovery never invokes an arbitrary constructor on
untrusted data, and never alters the value for builtins.

All public helpers swallow exceptions and return ``None`` on failure; the caller
treats ``None`` as "no type information available" and uses the raw payload.
"""

from __future__ import annotations

import collections.abc
import dataclasses
import functools
import inspect
import types
import typing
from typing import Any, Callable, cast


def is_reconstructable(annotation: Any) -> bool:
    """Return True if ``annotation`` names a custom type we can rebuild.

    Reconstructable targets are dataclasses, types exposing a callable
    ``from_json``, and ``Optional`` / ``list`` hints wrapping such types.
    Builtins (``int``, ``str``, ``dict``, ...) and unknown annotations are not
    reconstructable and resolve to ``False``.
    """
    origin = typing.get_origin(annotation)
    if origin is not None:
        args = typing.get_args(annotation)
        if origin is typing.Union or origin is types.UnionType:
            return any(
                is_reconstructable(a) for a in args if a is not type(None)
            )
        if origin in (list, collections.abc.Sequence):
            return any(is_reconstructable(a) for a in args)
        return False
    if not isinstance(annotation, type):
        return False
    if dataclasses.is_dataclass(annotation):
        return True
    return callable(getattr(cast(Any, annotation), "from_json", None))


@functools.lru_cache(maxsize=None)
def _resolved_hints(fn: Callable[..., Any]) -> dict[str, Any] | None:
    """Resolve a function's type hints, honoring postponed annotations.

    Results are memoized per function because discovery runs on every
    orchestrator/activity/entity execution (including replay).
    """
    try:
        return typing.get_type_hints(fn)
    except Exception:
        return None


def _input_annotation(fn: Callable[..., Any], position: int) -> Any | None:
    """Return the resolved annotation of the positional parameter at ``position``.

    ``position`` is the zero-based index among positional parameters (so the
    ``input`` parameter of a ``(ctx, input)`` function is at position 1, and the
    ``input`` parameter of an unbound ``(self, input)`` entity method is also at
    position 1). Returns ``None`` when the parameter is absent, unannotated, or
    its annotation is not a reconstructable custom type.
    """
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return None

    positional = [
        p for p in sig.parameters.values()
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY,
                      inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if position >= len(positional):
        return None
    param = positional[position]

    annotation: Any = param.annotation
    hints = _resolved_hints(fn)
    if hints is not None and param.name in hints:
        annotation = hints[param.name]
    elif isinstance(annotation, str):
        # Could not resolve a postponed (string) annotation -- give up.
        return None

    if annotation is inspect.Parameter.empty or annotation is Any:
        return None
    return annotation if is_reconstructable(annotation) else None


def orchestrator_input_type(fn: Callable[..., Any]) -> Any | None:
    """Discover the input type of an orchestrator function ``(ctx, input)``."""
    return _input_annotation(fn, 1)


def activity_input_type(fn: Callable[..., Any]) -> Any | None:
    """Discover the input type of an activity function ``(ctx, input)``."""
    return _input_annotation(fn, 1)


def activity_output_type(fn: Any) -> Any | None:
    """Discover the return type of an activity function.

    Returns the resolved return annotation when it names a reconstructable
    custom type (a dataclass or a ``from_json()``-capable type, optionally
    wrapped in ``Optional`` / ``list``). Returns ``None`` for plain callables
    that are not annotated with such a type, for string activity names, or when
    the annotation cannot be resolved.
    """
    if not callable(fn):
        return None
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return None

    annotation: Any = sig.return_annotation
    hints = _resolved_hints(fn)
    if hints is not None and "return" in hints:
        annotation = hints["return"]
    elif isinstance(annotation, str):
        # Could not resolve a postponed (string) annotation -- give up.
        return None

    if annotation is inspect.Signature.empty or annotation is Any or annotation is None:
        return None
    return annotation if is_reconstructable(annotation) else None


def entity_input_type(fn: Any, operation: str) -> Any | None:
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
        return _input_annotation(method, 1)
    return _input_annotation(fn, 1)
