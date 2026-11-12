# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Dispatch-local asyncio scheduling on top of the shared durable history engine.

Private asyncio dependencies (verified against the supported Python versions):

- ``asyncio._get_running_loop`` / ``asyncio._set_running_loop``: install this
  loop as the running loop only while a dispatch drains ready callbacks, and
  restore any host loop afterwards. ``Task.__await__`` uses the getter to
  detect durable, foreign, or absent loops without raising.
- ``asyncio.tasks._enter_task`` / ``asyncio.tasks._leave_task``: suspend and
  restore the host's current task while draining, so asyncio does not see two
  tasks running on one thread.
- ``asyncio.Handle._run``: run a ready callback with asyncio's own context and
  exception-handler semantics.
- ``asyncio.Task._log_traceback``: report unobserved local-task failures during
  dispatch cleanup without marking them as retrieved.
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
from collections import deque
from collections.abc import Callable, Coroutine, Generator
from datetime import UTC, datetime
from typing import Any, TypeVar

from durabletask import task

T = TypeVar("T")

_EPOCH = datetime(1970, 1, 1)


class _CompletionObserver(task.CompositeTask[None]):
    def __init__(self, durable: task.Task[Any], callback: Callable[[], None]) -> None:
        self._callback = callback
        super().__init__([durable])

    def on_child_completed(self, task: task.Task[Any]) -> None:
        if not self._is_complete:
            self._is_complete = True
            self._notify_parent()
            self._callback()

    def detach(self) -> None:
        self._notify_parent()


class DurableEventLoop(asyncio.AbstractEventLoop):
    """A FIFO microtask loop; it never polls a selector or advances wall time.

    Only history events can complete durable waits. asyncio owns coroutine
    stepping; this loop owns the ordering of ready callbacks.
    """

    def __init__(
        self, ctx: task.OrchestrationContext, *, is_complete: Callable[[], bool],
        logger: logging.Logger | None = None,
    ) -> None:
        self.ctx = ctx
        self._is_complete = is_complete
        self._logger = logger or logging.getLogger(__name__)
        self._ready: deque[asyncio.Handle] = deque()
        self._tasks: list[asyncio.Task[Any]] = []
        self._waits: dict[int, tuple[task.Task[Any], list[asyncio.Future[Any]]]] = {}
        self._observers: dict[int, _CompletionObserver] = {}
        self._completions: deque[Callable[[], None]] = deque()
        self._closed = False
        self._closing = False
        self._fatal: BaseException | None = None
        self._root: asyncio.Task[Any] | None = None

    def time(self) -> float:
        # A fixed UTC epoch avoids local-time and platform timestamp differences.
        now = self.ctx.current_utc_datetime
        if now.tzinfo is not None:
            now = now.astimezone(UTC).replace(tzinfo=None)
        return (now - _EPOCH).total_seconds()

    def get_debug(self) -> bool:
        return False

    def is_running(self) -> bool:
        return asyncio._get_running_loop() is self

    def is_closed(self) -> bool:
        return self._closed

    def create_future(self) -> asyncio.Future[Any]:
        return asyncio.Future(loop=self)

    def get_task_factory(self) -> None:
        return None

    def create_task(
        self, coro: Coroutine[Any, Any, T] | Generator[Any, None, T], *, name: str | None = None,
        context: contextvars.Context | None = None, **kwargs: Any,
    ) -> asyncio.Task[T]:
        if self._closed:
            coro.close()
            raise RuntimeError("The durable orchestration dispatch is closed.")
        if kwargs:
            coro.close()
            raise TypeError("Custom/eager task execution is not supported in durable orchestrators.")
        future = asyncio.Task(coro, loop=self, name=name, context=context)
        self._tasks.append(future)
        return future

    def call_soon(
        self, callback: Callable[..., Any], *args: Any,
        context: contextvars.Context | None = None,
    ) -> asyncio.Handle:
        handle = asyncio.Handle(callback, args, self, context=context)
        self._ready.append(handle)
        return handle

    def call_later(self, delay: float, callback: Callable[..., Any], *args: Any,
                   context: contextvars.Context | None = None) -> asyncio.TimerHandle:
        raise NotImplementedError(
            "Loop timers, including asyncio.sleep() with a positive delay, are not "
            "supported by the durable orchestration loop. Await ctx.create_timer(...) for a durable timer.")

    def call_at(self, when: float, callback: Callable[..., Any], *args: Any,
                context: contextvars.Context | None = None) -> asyncio.TimerHandle:
        return self.call_later(when - self.time(), callback, *args, context=context)

    async def wait_durable(self, durable: task.Task[T]) -> T:
        if durable.is_complete:
            return durable.get_result()
        future = self.create_future()
        entry = self._waits.setdefault(id(durable), (durable, []))
        entry[1].append(future)
        if id(durable) not in self._observers:
            self._observers[id(durable)] = _CompletionObserver(
                durable, lambda: self._completions.append(lambda: self._complete_wait(durable)))
        try:
            return await future
        finally:
            # Cancelling one waiter never cancels a shared durable operation.
            entry[1].remove(future)
            if not entry[1]:
                self._waits.pop(id(durable), None)
                self._observers.pop(id(durable)).detach()

    def _complete_wait(self, durable: task.Task[Any]) -> None:
        entry = self._waits.get(id(durable))
        if entry is not None:
            for future in tuple(entry[1]):
                if not future.done():
                    try:
                        future.set_result(durable.get_result())
                    except Exception as error:
                        future.set_exception(error)

    def _deliver_completions(self) -> None:
        while self._completions:
            self._completions.popleft()()

    def start(self, coro: Coroutine[Any, Any, Any]) -> None:
        self._root = self.create_task(coro, name="durable-orchestrator")
        self.resume()

    def resume(self) -> None:
        if self._is_complete():
            return
        self._deliver_completions()
        self._drain()
        assert self._root is not None
        if self._root.done():
            if self._root.cancelled():
                raise task.TaskCancelledError(
                    "The orchestrator coroutine was cancelled. Catch local cancellation "
                    "inside the orchestrator; terminate the durable instance through the client.")
            # result() propagates the orchestrator's failure to the executor.
            result = self._root.result()
            raise StopIteration(result)
        if not self._has_durable_work():
            raise RuntimeError(
                "The async orchestrator is blocked without durable work. "
                "Await durable activities, events or timers; external I/O is unsupported.")

    def _has_durable_work(self) -> bool:
        return any(not durable.is_complete for durable, _ in self._waits.values())

    def _drain(self) -> None:
        previous_loop = asyncio._get_running_loop()
        previous_task = asyncio.current_task(previous_loop) if previous_loop is not None else None
        if previous_task is not None and previous_loop is not None:
            asyncio.tasks._leave_task(previous_loop, previous_task)
        asyncio._set_running_loop(self)
        try:
            while self._ready:
                handle = self._ready.popleft()
                if not handle.cancelled():
                    handle._run()
                if self._fatal is not None:
                    error, self._fatal = self._fatal, None
                    if not self._closing:
                        raise error
                    self._logger.warning(
                        "%s: Async cleanup callback failed.", self.ctx.instance_id, exc_info=error)
                if not self._closing:
                    if self._is_complete():
                        break
                    if self._root is not None and self._root.done():
                        break
                    self._deliver_completions()
        finally:
            asyncio._set_running_loop(previous_loop)
            if previous_task is not None and previous_loop is not None:
                asyncio.tasks._enter_task(previous_loop, previous_task)

    def call_exception_handler(self, context: dict[str, Any]) -> None:
        error = context.get("exception")
        if isinstance(error, StopIteration):
            failure = RuntimeError("An orchestration callback raised StopIteration.")
            failure.__cause__ = error
            error = failure
        self._fatal = error if isinstance(error, BaseException) else RuntimeError(context["message"])

    def close(self) -> None:
        """Cancel unfinished coroutines so their cleanup runs before the dispatch ends."""
        if self._closed:
            return
        self._closing = True
        try:
            for future in tuple(self._tasks):
                if not future.done():
                    future.cancel()
            self._drain()
            failure: BaseException | None = None
            for future in tuple(self._tasks):
                if not future.done():
                    future.get_coro().close()
                    failure = failure or RuntimeError(
                        "Async cleanup must finish without waiting for durable or external work.")
                elif not future.cancelled() and getattr(future, "_log_traceback", False):
                    self._logger.warning(
                        "%s: Async background task failed.", self.ctx.instance_id,
                        exc_info=future.exception())
            if failure is not None:
                raise failure
        finally:
            self._closed = True
            self._ready.clear()
            for observer in self._observers.values():
                observer.detach()
            self._completions.clear()
            self._observers.clear()
            self._waits.clear()
            self._tasks.clear()
