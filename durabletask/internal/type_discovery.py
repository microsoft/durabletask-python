# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Best-effort discovery of input type hints for user functions.

These helpers resolve the annotation of the *input* parameter of an
orchestrator, activity, or entity function so that inbound payloads can be
reconstructed into the annotated custom type without the caller having to pass
an explicit type.

Discovery is intentionally conservative: it only returns an annotation when the
active :class:`~durabletask.serialization.DataConverter` reports it as
*reconstructable* via :meth:`DataConverter.is_reconstructable`. The default
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
from typing import Any, Callable

from durabletask.serialization import DEFAULT_DATA_CONVERTER, DataConverter


def _resolve_converter(converter: DataConverter | None) -> DataConverter:
    """Return the supplied converter, or the shared default when ``None``."""
    return converter if converter is not None else DEFAULT_DATA_CONVERTER


# Bounded so a worker that registers dynamically-created functions or closures
# cannot accumulate cache entries unboundedly over the process lifetime. The
# common case (a fixed set of module-level orchestrators/activities) fits well
# within this bound.
@functools.lru_cache(maxsize=2048)
def _resolved_hints(fn: Callable[..., Any]) -> dict[str, Any] | None:
    """Resolve a function's type hints, honoring postponed annotations.

    Results are memoized per function because discovery runs on every
    orchestrator/activity/entity execution (including replay).
    """
    try:
        return typing.get_type_hints(fn)
    except Exception:
        return None


def _input_annotation(fn: Callable[..., Any], position: int,
                      converter: DataConverter | None = None) -> Any | None:
    """Return the resolved annotation of the positional parameter at ``position``.

    ``position`` is the zero-based index among positional parameters (so the
    ``input`` parameter of a ``(ctx, input)`` function is at position 1, and the
    ``input`` parameter of an unbound ``(self, input)`` entity method is also at
    position 1). Returns ``None`` when the parameter is absent, unannotated, or
    its annotation is not reconstructable by ``converter``.
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
    return annotation if _resolve_converter(converter).is_reconstructable(annotation) else None


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
    return annotation if _resolve_converter(converter).is_reconstructable(annotation) else None


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
