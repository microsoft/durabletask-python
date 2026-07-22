# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Adapt durabletask-native activities to the Functions activity convention.

Azure Functions binds a single ``activityTrigger`` input to the activity
parameter named by ``input_name``. durabletask-native activities take a second
``ActivityContext`` argument that has no equivalent in the host-driven Functions
model, so :func:`wrap_activity` adapts a two-argument ``(ctx, input)`` activity
into a single-input function (passing ``None`` for the context). One-argument
activities -- the classic Functions shape -- are returned unchanged.
"""

from __future__ import annotations

import inspect
import keyword
import typing
from collections.abc import (
    Mapping,
    MutableMapping,
    MutableSequence,
    MutableSet,
    Sequence,
    Set,
)
from typing import Any, Callable, cast

from .orchestration_context import accepts_two_positional_args

# The Functions worker indexer rejects parameterized generic annotations, so an
# adapter's reproduced annotations reduce abstract container origins to concrete
# builtins.
_ABC_TO_CONCRETE: dict[Any, type] = {
    Mapping: dict,
    MutableMapping: dict,
    Sequence: list,
    MutableSequence: list,
    Set: set,
    MutableSet: set,
}


def _safe_annotation(annotation: Any) -> Any:
    """Return an annotation the Functions worker indexer accepts.

    Concrete types/classes pass through unchanged; parameterized generics (which
    the worker rejects) are reduced to a concrete origin type where possible and
    otherwise dropped (returned as ``inspect.Parameter.empty``).
    """
    if annotation is inspect.Parameter.empty:
        return annotation
    origin = typing.get_origin(annotation)
    if origin is None:
        return annotation
    concrete = _ABC_TO_CONCRETE.get(origin, origin)
    return concrete if isinstance(concrete, type) else inspect.Parameter.empty


def wrap_activity(fn: Callable[..., Any], input_name: str) -> Callable[..., Any]:
    """Adapt a durabletask-native ``(ctx, input)`` activity to single-input.

    A two-argument activity is wrapped as a single-input function whose sole
    parameter is named ``input_name`` (so the host's ``activityTrigger`` binding
    resolves it), invoking the original with ``None`` as the context. A
    one-argument activity is returned unchanged.
    """
    if not accepts_two_positional_args(fn):
        return fn

    if not input_name.isidentifier() or keyword.iskeyword(input_name):
        raise ValueError(
            "activity input_name must be a valid Python identifier; "
            f"got {input_name!r}")

    # Build a real single-parameter function whose parameter is named
    # ``input_name`` so both signature- and code-object-based indexing see it,
    # and the original is invoked positionally (its second-parameter name is
    # irrelevant).
    namespace: dict[str, Any] = {"_fn": fn}
    exec(  # noqa: S102 - input_name is validated to be a bare identifier above
        f"def _activity_adapter({input_name}):\n"
        f"    return _fn(None, {input_name})\n",
        namespace,
    )
    adapter = cast("Callable[..., Any]", namespace["_activity_adapter"])
    adapter.__name__ = getattr(fn, "__name__", "activity")
    adapter.__qualname__ = adapter.__name__
    adapter.__doc__ = getattr(fn, "__doc__", None)
    adapter.__module__ = getattr(fn, "__module__", adapter.__module__)

    # Reproduce the input/return annotations (sanitized) so the worker indexer
    # and the durable data converter see the intended types. Annotations are
    # resolved via ``get_type_hints`` so string annotations (PEP 563 /
    # ``from __future__ import annotations``) are evaluated to real types before
    # sanitizing.
    annotations: dict[str, Any] = {}
    try:
        hints = typing.get_type_hints(fn)
    except Exception:  # noqa: BLE001 - annotations may reference unresolvable names
        hints = {}
    try:
        params = list(inspect.signature(fn).parameters.values())
    except (TypeError, ValueError):
        params = []
    if len(params) >= 2 and params[1].name in hints:
        input_ann = _safe_annotation(hints[params[1].name])
        if input_ann is not inspect.Parameter.empty:
            annotations[input_name] = input_ann
    if "return" in hints:
        ret_ann = _safe_annotation(hints["return"])
        if ret_ann is not inspect.Parameter.empty:
            annotations["return"] = ret_ann
    adapter.__annotations__ = annotations
    return adapter
