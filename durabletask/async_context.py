# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Cold coroutine operations over an existing durable orchestration context."""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, TypeVar, overload

from durabletask.entities import EntityInstanceId, EntityLock

if TYPE_CHECKING:
    from durabletask import task

T = TypeVar("T")
TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")


class AsyncOrchestrationContext:
    """A coroutine-native operation surface returned by ``ctx.as_async()``.

    Every operation returns a single-use native coroutine. Constructing it
    schedules nothing: validation, input serialization, event consumption,
    and relative deadline evaluation occur only when execution starts. Inputs
    are not copied at construction; mutations before execution are observed.
    Use the original context for properties, synchronous commands, and eager,
    reusable durable handles; use ``handle.wait()`` for borrowed coroutine waits.

    Cancelling activity, sub-orchestration, entity, or lock waiters does not
    cancel remote work. Timers and event receives own their private local
    registrations and withdraw them when their coroutine exits. This cannot
    undo actions already committed to history or external side effects.
    """

    def __init__(self, context: task.OrchestrationContext) -> None:
        self._context = context

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
    ) -> TOutput:
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

    async def call_sub_orchestrator(
        self, orchestrator: task.Orchestrator[TInput, TOutput] | str, *,
        input: TInput | None = None,
        instance_id: str | None = None,
        retry_policy: task.RetryPolicy | None = None,
        version: str | None = None,
        return_type: type | None = None,
    ) -> Any:
        """Start and await a sub-orchestration when this coroutine executes."""
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

    async def call_entity(
        self, entity: EntityInstanceId, operation: str, input: Any = None, *,
        return_type: type | None = None,
    ) -> Any:
        """Start and await an entity operation when this coroutine executes."""
        return await self._context.call_entity(
            entity, operation, input, return_type=return_type)

    async def lock_entities(self, entities: list[EntityInstanceId]) -> EntityLock:
        """Acquire locks, for use as ``with await ops.lock_entities(ids):``.

        The returned synchronous context manager releases the locks. This
        preserves the original context's entity locking and replay rules;
        cancellation does not withdraw an already-issued lock request.
        """
        return await self._context.lock_entities(entities)

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
    async def receive_event(
        self, name: str, *, timeout: timedelta | None = ...,
        data_type: type[T],
    ) -> T:
        ...

    @overload
    async def receive_event(
        self, name: str, *, timeout: timedelta | None = ...,
        data_type: None = ...,
    ) -> Any:
        ...

    async def receive_event(
        self, name: str, *, timeout: timedelta | None = None,
        data_type: type | None = None,
    ) -> Any:
        """Receive one event through a private, cancellation-owned subscription.

        A finite relative timeout starts when the coroutine executes and raises
        ``TimeoutError`` if its durable timer wins. Buffered events win
        immediately, even for zero/negative timeouts, without creating a timer.
        Otherwise nonpositive timeouts fail immediately without a timer.
        Pending races follow history order. On every exit, the subscription
        and any outstanding timeout timer are withdrawn locally, so a later
        receive cannot lose an event to this abandoned subscription.

        For a shared subscription use the original context's
        ``wait_for_external_event()`` and borrowed ``Task.wait()`` instead.
        """
        from durabletask.task import when_any

        if timeout is not None and not math.isfinite(timeout.total_seconds()):
            raise ValueError("Event receive timeouts must be finite.")
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
