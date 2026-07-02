# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import inspect
from datetime import datetime
from typing import Any, Callable, Generator, Optional, cast
from uuid import UUID

from durabletask import task
from durabletask.entities import EntityInstanceId
from durabletask.task import OrchestrationContext, RetryPolicy, Task

from .token_source import TokenSource


class DurableOrchestrationContext:
    """Azure Functions-style orchestration context (v1-compatible).

    Wraps a durabletask :class:`~durabletask.task.OrchestrationContext` (and the
    orchestration input) and exposes the v1 ``DurableOrchestrationContext`` API.
    It is delivered to one-argument orchestrator functions
    (``def orchestrator(context):``); durabletask-native two-argument
    orchestrators (``def orchestrator(ctx, input):``) receive the durabletask
    context directly instead.
    """

    def __init__(self, ctx: OrchestrationContext, orchestration_input: Any = None):
        self._ctx = ctx
        self._input = orchestration_input

    # -- input ---------------------------------------------------------------
    def get_input(self) -> Any:
        """Get the orchestration input."""
        return self._input

    # -- properties ----------------------------------------------------------
    @property
    def instance_id(self) -> str:
        """Get the ID of the current orchestration instance."""
        return self._ctx.instance_id

    @property
    def is_replaying(self) -> bool:
        """Get whether the orchestrator is currently replaying."""
        return self._ctx.is_replaying

    @property
    def current_utc_datetime(self) -> datetime:
        """Get the replay-safe current UTC date/time."""
        return self._ctx.current_utc_datetime

    # -- activities ----------------------------------------------------------
    def call_activity(self, name: Callable[..., Any] | str, input_: Any = None) -> Task[Any]:
        """Schedule an activity function for execution."""
        return self._ctx.call_activity(name, input=input_)

    def call_activity_with_retry(self,
                                 name: Callable[..., Any] | str,
                                 retry_options: RetryPolicy,
                                 input_: Any = None) -> Task[Any]:
        """Schedule an activity function for execution, with retries."""
        return self._ctx.call_activity(name, input=input_, retry_policy=retry_options)

    # -- sub-orchestrators ---------------------------------------------------
    def call_sub_orchestrator(self,
                              name: Callable[..., Any] | str,
                              input_: Any = None,
                              instance_id: Optional[str] = None) -> Task[Any]:
        """Schedule a sub-orchestrator function for execution."""
        return self._ctx.call_sub_orchestrator(name, input=input_, instance_id=instance_id)

    def call_sub_orchestrator_with_retry(self,
                                         name: Callable[..., Any] | str,
                                         retry_options: RetryPolicy,
                                         input_: Any = None,
                                         instance_id: Optional[str] = None) -> Task[Any]:
        """Schedule a sub-orchestrator function for execution, with retries."""
        return self._ctx.call_sub_orchestrator(
            name, input=input_, instance_id=instance_id, retry_policy=retry_options)

    # -- timers and events ---------------------------------------------------
    def create_timer(self, fire_at: datetime) -> Task[Any]:
        """Create a durable timer that fires at the specified time."""
        return self._ctx.create_timer(fire_at)

    def wait_for_external_event(self,
                                name: str,
                                expected_type: Optional[type] = None) -> Task[Any]:
        """Wait for an external event with the given name."""
        return self._ctx.wait_for_external_event(name, data_type=expected_type)

    # -- control -------------------------------------------------------------
    def continue_as_new(self, input_: Any) -> None:
        """Restart the orchestration with a new input."""
        self._ctx.continue_as_new(input_)

    def set_custom_status(self, status: Any) -> None:
        """Set the orchestration's custom status payload."""
        self._ctx.set_custom_status(status)

    # -- deterministic IDs ---------------------------------------------------
    def new_uuid(self) -> str:
        """Create a new replay-safe UUID string."""
        return self._ctx.new_uuid()

    def new_guid(self) -> UUID:
        """Create a new replay-safe UUID."""
        return UUID(self._ctx.new_uuid())

    # -- fan-out / fan-in ----------------------------------------------------
    def task_all(self, tasks: list[Task[Any]]) -> Task[Any]:
        """Schedule all tasks and complete when all of them complete."""
        return task.when_all(tasks)

    def task_any(self, tasks: list[Task[Any]]) -> Task[Any]:
        """Schedule all tasks and complete when the first one completes."""
        return task.when_any(tasks)

    # -- entities ------------------------------------------------------------
    def call_entity(self,
                    entityId: EntityInstanceId,
                    operationName: str,
                    operationInput: Any = None) -> Task[Any]:
        """Call an entity operation and get its result."""
        return self._ctx.call_entity(entityId, operationName, operationInput)

    def signal_entity(self,
                      entityId: EntityInstanceId,
                      operationName: str,
                      operationInput: Any = None) -> None:
        """Signal an entity operation (fire and forget)."""
        self._ctx.signal_entity(entityId, operationName, input=operationInput)

    # -- durable HTTP (not yet supported) ------------------------------------
    def call_http(self,
                  method: str,
                  uri: str,
                  content: Optional[str] = None,
                  headers: Optional[dict[str, str]] = None,
                  token_source: Optional[TokenSource] = None,
                  is_raw_str: bool = False) -> Any:
        """Schedule a durable HTTP call (v1 API).

        Not yet supported: durabletask has no durable-HTTP (``call_http``)
        equivalent, so this raises ``NotImplementedError``.
        """
        raise NotImplementedError(
            "call_http is not yet supported by durabletask. The durable-HTTP "
            "API (and its TokenSource auth) has no durabletask equivalent yet.")


def accepts_two_positional_args(fn: Callable[..., Any]) -> bool:
    """Return True if ``fn`` can be called with two positional args ``(ctx, input)``.

    Two-argument functions are treated as durabletask-native orchestrators;
    single-argument functions are treated as Azure Functions / v1-style
    orchestrators that receive a wrapped :class:`DurableOrchestrationContext`.
    """
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        # Can't introspect -> assume durabletask-native and pass through.
        return True

    positional = 0
    for param in sig.parameters.values():
        if param.kind in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD):
            positional += 1
        elif param.kind == param.VAR_POSITIONAL:
            return True
    return positional >= 2


def wrap_orchestrator(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Adapt a v1-style one-argument orchestrator to durabletask's ``(ctx, input)``.

    Two-argument (durabletask-native) orchestrators are returned unchanged. The
    returned wrapper deliberately does not set ``__wrapped__`` so durabletask
    introspects the wrapper's own ``(context, _input)`` signature (and thus
    passes the raw input) rather than the wrapped function's signature.
    """
    if accepts_two_positional_args(fn):
        return fn

    name = getattr(fn, "__name__", "orchestrator")

    if inspect.isgeneratorfunction(fn):
        def _generator_wrapper(context: OrchestrationContext, _input: Any = None) -> Any:
            adapter = DurableOrchestrationContext(context, _input)
            generator = cast("Generator[Any, Any, Any]", fn(adapter))
            result: Any = yield from generator
            return result
        _generator_wrapper.__name__ = name
        return _generator_wrapper

    def _wrapper(context: OrchestrationContext, _input: Any = None) -> Any:
        adapter = DurableOrchestrationContext(context, _input)
        return fn(adapter)
    _wrapper.__name__ = name
    return _wrapper
