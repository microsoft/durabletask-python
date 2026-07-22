# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Any, Callable, Optional

from durabletask.entities import EntityContext

from .orchestration_context import accepts_two_positional_args

# Sentinel distinguishing "no result set" from an explicit ``set_result(None)``.
# v1 treats ``None`` as a valid operation result, so it must not be confused
# with the unset state.
_UNSET = object()


class DurableEntityContext:
    """Azure Functions-style entity context (v1-compatible).

    Wraps a durabletask :class:`~durabletask.entities.EntityContext` (and the
    operation input) and exposes the v1 ``DurableEntityContext`` API. It is
    delivered to one-argument entity functions (``def entity(context):``).
    durabletask-native two-argument entity functions
    (``def entity(ctx, input):``) and class-based entities are used directly.
    """

    def __init__(self, ctx: EntityContext, operation_input: Any = None):
        self._ctx = ctx
        self._input = operation_input
        self._result: Any = _UNSET

    # -- identity ------------------------------------------------------------
    @property
    def entity_name(self) -> str:
        """Get the entity name."""
        return self._ctx.entity_id.entity

    @property
    def entity_key(self) -> str:
        """Get the entity key."""
        return self._ctx.entity_id.key

    @property
    def operation_name(self) -> str:
        """Get the current operation name."""
        return self._ctx.operation

    @property
    def is_newly_constructed(self) -> bool:
        """Whether the entity was newly constructed.

        The v1 semantics of this flag were unspecified; it is always ``False``.
        """
        return False

    # -- input / state / result ---------------------------------------------
    def get_input(self, expected_type: Optional[type] = None) -> Any:
        """Get the input for the current operation.

        ``expected_type`` is accepted for v1 compatibility but the input is
        already deserialized by durabletask, so it is returned as-is.
        """
        return self._input

    def get_state(self,
                  initializer: Optional[Callable[[], Any]] = None,
                  expected_type: Optional[type] = None) -> Any:
        """Get the current state of the entity.

        Parameters
        ----------
        initializer : Optional[Callable[[], Any]]
            A zero-argument callable providing the initial state when no state
            exists yet. It is invoked lazily -- only when there is no existing
            state -- so a side-effecting or expensive initializer does not run
            when state is already present.
        expected_type : Optional[type]
            Optional type used to reconstruct the state.
        """
        state = self._ctx.get_state(expected_type, _UNSET)
        if state is _UNSET:
            return initializer() if callable(initializer) else None
        return state

    def set_state(self, state: Any) -> None:
        """Set the state of the entity."""
        self._ctx.set_state(state)

    def set_result(self, result: Any) -> None:
        """Set the result (return value) of the current operation."""
        self._result = result

    def resolve_result(self, fallback: Any) -> Any:
        """Return the value set via :meth:`set_result`, or ``fallback`` if unset."""
        return fallback if self._result is _UNSET else self._result

    def destruct_on_exit(self) -> None:
        """Delete this entity after the operation completes."""
        self._ctx.set_state(None)


def wrap_entity(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Adapt a v1-style one-argument entity function to durabletask's ``(ctx, input)``.

    Class-based entities and durabletask-native two-argument entity functions
    are returned unchanged. For a wrapped v1 entity, the operation result is
    taken from ``context.set_result(...)`` (falling back to the function's
    return value).
    """
    if isinstance(fn, type):
        # Class-based entity: handled natively by durabletask.
        return fn
    if accepts_two_positional_args(fn):
        # durabletask-native (ctx, input) entity function.
        return fn

    def _wrapper(ctx: EntityContext, _input: Any = None) -> Any:
        adapter = DurableEntityContext(ctx, _input)
        returned = fn(adapter)
        return adapter.resolve_result(returned)

    _wrapper.__name__ = getattr(fn, "__name__", "entity")
    durable_entity_name = getattr(fn, "__durable_entity_name__", None)
    if durable_entity_name is not None:
        _wrapper.__durable_entity_name__ = durable_entity_name  # type: ignore[attr-defined]
    return _wrapper
