# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Coroutine-native orchestration context backed by the durable replay engine."""

from __future__ import annotations

import functools
import inspect
import logging
import sys
import weakref
from collections.abc import Callable, Coroutine, Generator
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Concatenate, ParamSpec, TypeVar, overload

from durabletask.entities import EntityInstanceId, EntityLock

T = TypeVar("T")
TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")
P = ParamSpec("P")
R = TypeVar("R")


def _cold_operation(
    operation: Callable[Concatenate[AsyncOrchestrationContext, P], Coroutine[Any, Any, R]],
) -> Callable[Concatenate[AsyncOrchestrationContext, P], Coroutine[Any, Any, R]]:
    """Weakly track each operation coroutine so dispatch teardown can close unstarted ones."""
    @functools.wraps(operation)
    def create(self: AsyncOrchestrationContext, /, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
        coroutine = operation(self, *args, **kwargs)
        self._operations.add(coroutine)  # pyright: ignore[reportPrivateUsage]
        return coroutine

    if sys.version_info >= (3, 12):
        inspect.markcoroutinefunction(create)
    return create


def _is_unstarted(operation: Coroutine[Any, Any, Any]) -> bool:
    return inspect.getcoroutinestate(operation) == inspect.CORO_CREATED


class AsyncOrchestrationContext:
    """Context supplied automatically to ``async def`` orchestrators.

    Durable operations return single-use native coroutines. Constructing one
    schedules nothing: validation, input serialization, event consumption, and
    relative deadline evaluation occur when it starts. Inputs are not copied
    at construction; mutations before execution are observed.

    Cancelling activity, sub-orchestration, entity, or lock waiters does not
    cancel remote work. Timers and event receives own their private local
    registrations and withdraw them when their coroutine exits. This cannot
    undo actions already committed to history or external side effects.
    """

    def __init__(self, context: task.OrchestrationContext) -> None:
        self._context = context
        self._operations: weakref.WeakSet[Coroutine[Any, Any, Any]] = weakref.WeakSet()

    @contextmanager
    def _closing_unstarted_operations(self) -> Generator[None, None, None]:
        """Close operation coroutines that never started once dispatch teardown ends.

        An unstarted coroutine has run none of its body, so closing it schedules
        nothing. Retaining them during teardown keeps the cancelled orchestrator
        from finalizing them first and reporting them as never awaited.
        """
        unstarted = [operation for operation in self._operations if _is_unstarted(operation)]
        try:
            yield
        finally:
            for operation in (*unstarted, *self._operations):
                if _is_unstarted(operation):
                    operation.close()

    @property
    def instance_id(self) -> str:
        """Get the current orchestration instance ID."""
        return self._context.instance_id

    @property
    def parent_instance_id(self) -> str | None:
        """Get the parent orchestration instance ID, if any."""
        return self._context.parent_instance_id

    @property
    def version(self) -> str | None:
        """Get the orchestration instance version, if any."""
        return self._context.version

    @property
    def current_utc_datetime(self) -> datetime:
        """Get the replay-safe current UTC time."""
        return self._context.current_utc_datetime

    @property
    def is_replaying(self) -> bool:
        """Get whether the orchestration is replaying its history."""
        return self._context.is_replaying

    def set_custom_status(self, custom_status: Any) -> None:
        """Set the orchestration's custom status."""
        self._context.set_custom_status(custom_status)

    def signal_entity(
        self, entity_id: EntityInstanceId, operation_name: str,
        input: Any = None, signal_time: datetime | None = None,
    ) -> None:
        """Send a one-way entity operation."""
        self._context.signal_entity(entity_id, operation_name, input, signal_time)

    def send_event(self, instance_id: str, event_name: str, *, data: Any | None = None) -> None:
        """Send a one-way event to another orchestration."""
        self._context.send_event(instance_id, event_name, data=data)

    def continue_as_new(
        self, new_input: Any, *, save_events: bool = False, new_version: str | None = None,
    ) -> None:
        """Continue the orchestration with new input."""
        self._context.continue_as_new(new_input, save_events=save_events, new_version=new_version)

    def new_uuid(self) -> str:
        """Create a replay-safe UUID."""
        return self._context.new_uuid()

    def create_replay_safe_logger(self, logger: logging.Logger) -> task.ReplaySafeLogger:
        """Create a logger that suppresses messages during replay."""
        return self._context.create_replay_safe_logger(logger)

    def as_task_context(self) -> task.OrchestrationContext:
        """Return the eager task-handle context for advanced interoperability.

        Its operations schedule immediately and return reusable durable handles.
        Await a handle directly or use ``handle.wait()`` for an independent
        coroutine waiter whose cancellation does not cancel the underlying work.
        """
        return self._context

    @overload
    async def call_activity(
        self, activity: task.Activity[TInput, TOutput] | str, *,
        input: TInput | None = ...,
        retry_policy: task.RetryPolicy | None = ...,
        tags: dict[str, str] | None = ...,
        return_type: type[T],
    ) -> T:
        ...

    @overload
    async def call_activity(
        self, activity: task.Activity[TInput, TOutput], *,
        input: TInput | None = ...,
        retry_policy: task.RetryPolicy | None = ...,
        tags: dict[str, str] | None = ...,
        return_type: None = ...,
    ) -> TOutput:
        ...

    @overload
    async def call_activity(
        self, activity: str, *, input: Any = ...,
        retry_policy: task.RetryPolicy | None = ...,
        tags: dict[str, str] | None = ...,
        return_type: None = ...,
    ) -> Any:
        ...

    @_cold_operation
    async def call_activity(
        self, activity: task.Activity[TInput, TOutput] | str, *,
        input: TInput | None = None,
        retry_policy: task.RetryPolicy | None = None,
        tags: dict[str, str] | None = None,
        return_type: type | None = None,
    ) -> Any:
        """Start and await an activity when this coroutine executes."""
        return await self._context.call_activity(
            activity, input=input, retry_policy=retry_policy, tags=tags,
            return_type=return_type)

    @overload
    async def call_sub_orchestrator(
        self, orchestrator: task.Orchestrator[TInput, TOutput] | str, *,
        input: TInput | None = ...,
        instance_id: str | None = ...,
        retry_policy: task.RetryPolicy | None = ...,
        version: str | None = ...,
        return_type: type[T],
    ) -> T:
        ...

    @overload
    async def call_sub_orchestrator(
        self, orchestrator: task.Orchestrator[TInput, TOutput], *,
        input: TInput | None = ...,
        instance_id: str | None = ...,
        retry_policy: task.RetryPolicy | None = ...,
        version: str | None = ...,
        return_type: None = ...,
    ) -> Any:
        ...

    @overload
    async def call_sub_orchestrator(
        self, orchestrator: str, *, input: Any = ...,
        instance_id: str | None = ...,
        retry_policy: task.RetryPolicy | None = ...,
        version: str | None = ...,
        return_type: None = ...,
    ) -> Any:
        ...

    @_cold_operation
    async def call_sub_orchestrator(
        self, orchestrator: task.Orchestrator[TInput, TOutput] | str, *,
        input: TInput | None = None,
        instance_id: str | None = None,
        retry_policy: task.RetryPolicy | None = None,
        version: str | None = None,
        return_type: type | None = None,
    ) -> Any:
        """Start and await a sub-orchestration when this coroutine executes.

        Supply ``return_type`` to reconstruct a custom result type. A callable's
        return annotation is not used to reconstruct sub-orchestration results;
        without an explicit type, the result is typed as ``Any``.
        """
        return await self._context.call_sub_orchestrator(
            orchestrator, input=input, instance_id=instance_id,
            retry_policy=retry_policy, version=version, return_type=return_type)

    @overload
    async def call_entity(
        self, entity: EntityInstanceId, operation: str, input: Any = ..., *,
        return_type: type[T],
    ) -> T:
        ...

    @overload
    async def call_entity(
        self, entity: EntityInstanceId, operation: str, input: Any = ..., *,
        return_type: None = ...,
    ) -> Any:
        ...

    @_cold_operation
    async def call_entity(
        self, entity: EntityInstanceId, operation: str, input: Any = None, *,
        return_type: type | None = None,
    ) -> Any:
        """Start and await an entity operation when this coroutine executes."""
        return await self._context.call_entity(
            entity, operation, input, return_type=return_type)

    @_cold_operation
    async def lock_entities(self, entities: list[EntityInstanceId]) -> EntityLock:
        """Acquire locks, for use as ``with await ctx.lock_entities(ids):``.

        The returned synchronous context manager releases the locks. This
        preserves the original context's entity locking and replay rules;
        cancellation does not withdraw an already-issued lock request. An
        abandoned request can still acquire locks, which remain held until
        released or the orchestration ends. Retain and shield the acquisition
        task if a timeout or another local cancellation might interrupt it,
        then await it and exit the returned lock when it is safe to do so.
        """
        return await self._context.lock_entities(entities)

    @_cold_operation
    async def create_timer(self, fire_at: datetime | timedelta) -> None:
        """Await a private durable timer, cancelling it when this wait exits.

        Relative deadlines are evaluated when execution starts, not when this
        coroutine is constructed. Cancellation removes uncommitted timer work
        where possible; it cannot retract a timer already present in history.
        """
        timer = self._context.create_timer(fire_at)
        try:
            await timer
        finally:
            timer.cancel()

    @overload
    async def wait_for_external_event(
        self, name: str, *, timeout: timedelta | None = ...,
        data_type: type[T],
    ) -> T:
        ...

    @overload
    async def wait_for_external_event(
        self, name: str, *, timeout: timedelta | None = ...,
        data_type: None = ...,
    ) -> Any:
        ...

    @_cold_operation
    async def wait_for_external_event(
        self, name: str, *, timeout: timedelta | None = None,
        data_type: type | None = None,
    ) -> Any:
        """Receive one event through a private, cancellation-owned subscription.

        A relative timeout starts when the coroutine executes and raises
        ``TimeoutError`` if its durable timer wins. Buffered events win
        immediately, even for zero/negative timeouts, without creating a timer.
        Otherwise nonpositive timeouts fail immediately without a timer.
        Pending races follow history order. On every exit, the subscription
        and any outstanding timeout timer are withdrawn locally, so a later
        receive cannot lose an event to this abandoned subscription.

        For a shared subscription use ``as_task_context().wait_for_external_event()``
        and borrowed ``Task.wait()`` instead.
        """
        from durabletask.task import when_any

        event: task.CancellableTask[Any] = self._context.wait_for_external_event(name, data_type=data_type)
        timer: task.TimerTask | None = None
        try:
            if event.is_complete:
                return event.get_result()
            if timeout is None:
                return await event
            if timeout <= timedelta(0):
                raise TimeoutError(f"Timed out waiting for event {name!r}.")
            timer = self._context.create_timer(timeout)
            winner = await when_any([event, timer])
            if winner is timer:
                raise TimeoutError(f"Timed out waiting for event {name!r}.")
            return event.get_result()
        finally:
            event.cancel()
            if timer is not None:
                timer.cancel()


# Resolve public annotations at runtime without importing task before this
# module has defined the context that task re-exports.
from durabletask import task  # noqa: E402
