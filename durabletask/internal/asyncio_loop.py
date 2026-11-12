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
  dispatch cleanup without marking them as retrieved, and keep such tasks
  tracked until then.
- ``__self__`` of Task step/wakeup callbacks (including the C implementation's
  step wrapper and bound ``task_wakeup``): identify which local task a
  callback resumes, so task-group cancellation wakeups run in task-creation
  order.
- ``AbstractEventLoop._timer_handle_cancelled``: the hook ``TimerHandle.cancel``
  calls on its loop, used to withdraw the durable timer observer.
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
import math
import sys
from collections import deque
from collections.abc import Callable, Coroutine, Generator
from datetime import UTC, datetime, timedelta
from fractions import Fraction
from typing import Any, TypeVar, cast

from durabletask import task

T = TypeVar("T")

_EPOCH = datetime(1970, 1, 1)
_MICROSECOND = timedelta(microseconds=1)


def _is_process_control(error: BaseException | None) -> bool:
    if isinstance(error, BaseExceptionGroup):
        group = cast(BaseExceptionGroup[BaseException], error)
        return group.subgroup((KeyboardInterrupt, SystemExit)) is not None
    return isinstance(error, (KeyboardInterrupt, SystemExit))


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


class _DurableAsyncTask(asyncio.Task[T]):
    def __init__(
        self, coro: Coroutine[Any, Any, T] | Generator[Any, None, T], *,
        sequence: int, loop: DurableEventLoop, name: str | None,
        context: contextvars.Context | None,
    ) -> None:
        self._sequence = sequence
        super().__init__(coro, loop=loop, name=name, context=context)

    def __hash__(self) -> int:
        # Load-bearing for replay determinism: TaskGroup keeps its children in a
        # set and iterates it to cancel them. Hashing by creation sequence makes
        # that traversal, and the cancellation wakeups it schedules, follow task
        # creation order instead of memory addresses that differ across workers.
        return self._sequence

    def __await__(self) -> Generator[Any, None, T]:
        try:
            return (yield from super().__await__())
        finally:
            self._retire_if_observed()

    __iter__ = __await__

    def result(self) -> T:
        try:
            return super().result()
        finally:
            self._retire_if_observed()

    def exception(self) -> BaseException | None:
        try:
            return super().exception()
        finally:
            self._retire_if_observed()

    def _retire_if_observed(self) -> None:
        loop = self.get_loop()
        assert isinstance(loop, DurableEventLoop)
        loop.retire_task(self)

    def cancel(self, msg: Any = None) -> bool:
        loop = self.get_loop()
        assert isinstance(loop, DurableEventLoop)
        previous = loop.cancelling_task
        loop.cancelling_task = self
        try:
            return super().cancel(msg)
        finally:
            loop.cancelling_task = previous


class _DurableTimerHandle(asyncio.TimerHandle):
    def __eq__(self, other: object) -> bool:
        return self is other

    __hash__ = object.__hash__

    def __init__(
        self, when: float, callback: Callable[..., Any], args: tuple[Any, ...],
        loop: DurableEventLoop, context: contextvars.Context | None,
    ) -> None:
        super().__init__(when, callback, args, loop, context=context)
        self.durable_timer: task.TimerTask | None = None

    def cancel(self) -> None:
        super().cancel()
        if self.durable_timer is not None:
            self.durable_timer.cancel()


class DurableEventLoop(asyncio.AbstractEventLoop):
    """A FIFO microtask loop; it never polls a selector or advances wall time.

    Only history events can complete durable waits/timers. asyncio owns coroutine
    stepping, gather and structured cancellation; this loop owns their ordering.
    """

    def __init__(
        self, ctx: task.OrchestrationContext, *, is_complete: Callable[[], bool],
        logger: logging.Logger | None = None,
    ) -> None:
        self.ctx = ctx
        self._is_complete = is_complete
        self._logger = logger or logging.getLogger(__name__)
        self._ready: deque[asyncio.Handle] = deque()
        self._cancel_ready: list[tuple[int, int, asyncio.Handle]] = []
        self._tasks: dict[asyncio.Task[Any], int] = {}
        self._task_sequence = 0
        self.cancelling_task: asyncio.Task[Any] | None = None
        self._waits: dict[int, tuple[task.Task[Any], list[asyncio.Future[Any]]]] = {}
        self._observers: dict[int, _CompletionObserver] = {}
        self._completions: deque[Callable[[], None]] = deque()
        self._timers: dict[_DurableTimerHandle, _CompletionObserver] = {}
        self._asyncgens: dict[int, Any] = {}
        self._asyncgen_prune_at = 64
        self._closed = False
        self._closing = False
        self._fatal: BaseException | None = None
        self._cleanup_failure: BaseException | None = None
        self._cleanup_truncated = False
        self._root: asyncio.Task[Any] | None = None

    def _now(self) -> datetime:
        now = self.ctx.current_utc_datetime
        if now.tzinfo is not None:
            now = now.astimezone(UTC).replace(tzinfo=None)
        return now

    def time(self) -> float:
        # A fixed UTC epoch avoids local-time and platform timestamp differences.
        return (self._now() - _EPOCH).total_seconds()

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
        sequence = self._task_sequence
        self._task_sequence += 1
        future = _DurableAsyncTask(coro, sequence=sequence, loop=self, name=name, context=context)
        self._tasks[future] = sequence
        future.add_done_callback(self.retire_task)
        return future

    def retire_task(self, future: asyncio.Task[Any]) -> None:
        # A failed task may be observed after its done callbacks have run.
        # Inspect without retrieving exceptions so unobserved failures still log.
        if not self._closing and future.done() and not getattr(future, "_log_traceback", False):
            self._tasks.pop(future, None)

    def call_soon(
        self, callback: Callable[..., Any], *args: Any,
        context: contextvars.Context | None = None,
    ) -> asyncio.Handle:
        handle = asyncio.Handle(callback, args, self, context=context)
        if self.cancelling_task is not None:
            # TaskGroup cancels a set of tasks. Do not let object hashes
            # determine the order in which their finally blocks execute.
            owner = getattr(callback, "__self__", None)
            index = self._tasks.get(
                self.cancelling_task, getattr(self.cancelling_task, "_sequence", self._task_sequence))
            if isinstance(owner, asyncio.Task):
                index = self._tasks.get(cast(asyncio.Task[Any], owner), index)
            self._cancel_ready.append((index, len(self._cancel_ready), handle))
        else:
            self._ready.append(handle)
        return handle

    def call_later(
        self, delay: float, callback: Callable[..., Any], *args: Any,
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle:
        if not math.isfinite(delay):
            raise ValueError("Durable delays must be finite.")
        return self.call_at(self.time() + max(0, delay), callback, *args, context=context)

    def call_at(
        self, when: float, callback: Callable[..., Any], *args: Any,
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle:
        if not math.isfinite(when):
            raise ValueError("Durable deadlines must be finite.")
        handle = _DurableTimerHandle(when, callback, args, self, context)
        # Exact rational ceiling: float round-trips can emit a deadline that
        # precedes the requested one by a fraction of a microsecond.
        deadline_us = math.ceil(Fraction(when) * 1_000_000)
        now_us = (self._now() - _EPOCH) // _MICROSECOND
        if when <= self.time() or deadline_us <= now_us:
            self._ready.append(handle)
        else:
            if self._closing:
                raise RuntimeError("Dispatch cleanup cannot schedule durable timers.")
            deadline = _EPOCH + timedelta(microseconds=deadline_us)
            handle.durable_timer = self.ctx.create_timer(deadline)
            self._timers[handle] = _CompletionObserver(
                handle.durable_timer,
                lambda: self._completions.append(lambda: self._complete_timer(handle)))
        return handle

    def _timer_handle_cancelled(self, handle: asyncio.TimerHandle) -> None:
        if isinstance(handle, _DurableTimerHandle):
            observer = self._timers.pop(handle, None)
            if observer is not None:
                observer.detach()

    def _complete_timer(self, handle: _DurableTimerHandle) -> None:
        self._timers.pop(handle, None)
        if not handle.cancelled():
            self._ready.append(handle)

    async def wait_durable(self, durable: task.Task[T]) -> T:
        if self._closing:
            raise RuntimeError("Dispatch cleanup cannot await durable work.")
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
                # Dispatch close may already have detached and cleared observers.
                observer = self._observers.pop(id(durable), None)
                if observer is not None:
                    observer.detach()

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
            # result() deliberately propagates control-flow BaseExceptions to the
            # existing executor recovery boundary, not a success-shaped fallback.
            result = self._root.result()
            raise StopIteration(result)
        if not self._has_durable_work():
            raise RuntimeError(
                "The async orchestrator is blocked without durable work. "
                "Await durable activities, events or timers; external I/O is unsupported.")

    def _has_durable_work(self) -> bool:
        return any(not durable.is_complete for durable, _ in self._waits.values()) or bool(self._timers)

    def _drain(self, cleanup_limit: int | None = None) -> int:
        previous_loop = asyncio._get_running_loop()
        previous_task = asyncio.current_task(previous_loop) if previous_loop is not None else None
        previous_hooks = sys.get_asyncgen_hooks()
        if previous_task is not None and previous_loop is not None:
            asyncio.tasks._leave_task(previous_loop, previous_task)
        asyncio._set_running_loop(self)
        sys.set_asyncgen_hooks(firstiter=self._asyncgen_firstiter, finalizer=self._asyncgen_finalizer)
        try:
            steps = 0
            if cleanup_limit is None:
                cleanup_limit = self._cleanup_budget()
            while self._ready or self._cancel_ready:
                self._ready.extend(handle for _, _, handle in sorted(self._cancel_ready))
                self._cancel_ready.clear()
                handle = self._ready.popleft()
                if not handle.cancelled():
                    try:
                        handle._run()
                    except BaseException as error:
                        if not self._closing:
                            raise
                        # Defer all exits until other cleanup tasks have drained.
                        self._record_cleanup_error(error)
                if self._fatal is not None:
                    error, self._fatal = self._fatal, None
                    if not self._closing:
                        raise error
                    self._record_cleanup_error(error)
                if not self._closing and self._is_complete():
                    break
                if not self._closing and self._root is not None and self._root.done():
                    break
                if not self._closing:
                    self._deliver_completions()
                steps += 1
                if self._closing and steps >= cleanup_limit:
                    self._report_unquiesced_cleanup()
                    break
            return steps
        finally:
            sys.set_asyncgen_hooks(*previous_hooks)
            asyncio._set_running_loop(previous_loop)
            if previous_task is not None and previous_loop is not None:
                asyncio.tasks._enter_task(previous_loop, previous_task)

    def _asyncgen_firstiter(self, generator: Any) -> None:
        if len(self._asyncgens) >= self._asyncgen_prune_at:
            # Exhausted generators have no cleanup left; amortized pruning bounds retention.
            self._asyncgens = {
                key: value for key, value in self._asyncgens.items() if value.ag_frame is not None}
            self._asyncgen_prune_at = max(64, 2 * len(self._asyncgens))
        self._asyncgens[id(generator)] = generator

    def _asyncgen_finalizer(self, generator: Any) -> None:
        if not self._closed:
            self.create_task(generator.aclose())

    def call_exception_handler(self, context: dict[str, Any]) -> None:
        error = context.get("exception")
        if isinstance(error, StopIteration):
            failure = RuntimeError("An orchestration callback raised StopIteration.")
            failure.__cause__ = error
            error = failure
        self._fatal = error if isinstance(error, BaseException) else RuntimeError(context["message"])

    def _cleanup_budget(self) -> int:
        # Cooperative cleanup is capped in scheduling steps, not wall-clock time.
        return max(10000, 4 * (len(self._tasks) + len(self._asyncgens)))

    def _report_unquiesced_cleanup(self) -> None:
        self._ready.clear()
        self._cancel_ready.clear()
        if not self._cleanup_truncated:
            self._cleanup_truncated = True
            self._record_cleanup_error(RuntimeError(
                "Async cleanup did not quiesce; cleanup must cooperate with cancellation."))

    def _record_cleanup_error(self, error: BaseException, source: str = "cleanup callback") -> None:
        self._logger.warning("%s: Async %s failed.", self.ctx.instance_id, source, exc_info=error)
        if not isinstance(error, (Exception, asyncio.CancelledError)):
            if _is_process_control(self._cleanup_failure):
                return
            # Keep the first failure unless a process-control exit supersedes it.
            if self._cleanup_failure is None or _is_process_control(error):
                self._cleanup_failure = error

    def _close_asyncgens(self, closed: dict[int, Any]) -> None:
        # Closing one generator may first-iterate another; sweep until no new generators appear.
        # Closed generators stay referenced so their ids cannot be reused during cleanup.
        steps = 0
        # Cleanup-created tasks and generators must not replenish the sweep's budget.
        limit = self._cleanup_budget()
        while True:
            pending = [
                generator for key, generator in self._asyncgens.items() if closed.get(key) is not generator]
            if not pending:
                return
            if steps >= limit:
                self._report_unquiesced_cleanup()
                return
            for generator in pending:
                closed[id(generator)] = generator
                self.create_task(generator.aclose())
            # Each drain spends from this sweep's remaining budget.
            steps += max(1, self._drain(limit - steps))

    def close(self) -> None:
        if self._closed:
            return
        self._closing = True
        try:
            completed_at_close = {future for future in self._tasks if future.done()}
            unobserved_at_close = {
                future for future in completed_at_close if getattr(future, "_log_traceback", False)}
            for future in tuple(self._tasks):
                if not future.done():
                    future.cancel()
            self._drain()
            closed_asyncgens: dict[int, Any] = {}
            self._close_asyncgens(closed_asyncgens)
            for future in tuple(self._tasks):
                if not future.done():
                    future.cancel()
            self._drain()
            self._close_asyncgens(closed_asyncgens)
            failure: BaseException | None = self._cleanup_failure
            for future in tuple(self._tasks):
                if not future.done():
                    try:
                        future.get_coro().close()
                    except BaseException as error:
                        # Close siblings before propagating process-control exits.
                        self._record_cleanup_error(error, "coroutine cleanup")
                        failure = failure or error
                    failure = failure or RuntimeError(
                        "Async cleanup must finish without waiting for durable or external work.")
                elif not future.cancelled():
                    error = future.exception()
                    if error is not None and (future not in completed_at_close or future in unobserved_at_close):
                        self._record_cleanup_error(
                            error, "background task" if future in completed_at_close else "task cleanup")
            if self._cleanup_failure is not None:
                raise self._cleanup_failure
            if failure is not None:
                raise failure
        finally:
            self._closed = True
            self._ready.clear()
            self._cancel_ready.clear()
            for observer in (*self._observers.values(), *self._timers.values()):
                observer.detach()
            self._completions.clear()
            self._observers.clear()
            self._waits.clear()
            self._timers.clear()
            self._tasks.clear()
            self._asyncgens.clear()


_IO_HINT = "External I/O is unsupported in orchestrators; move it to an ordinary activity."
_THREAD_HINT = "Threads/executors are unsupported in orchestrators; use an activity."
_LOOP_HINT = "The worker owns the durable loop's lifecycle and configuration."

# Every asyncio loop API this loop does not implement fails with one actionable
# error instead of AbstractEventLoop's bare NotImplementedError.
_UNSUPPORTED_LOOP_APIS: dict[str, str] = {
    **dict.fromkeys((
        "create_connection", "create_server", "create_datagram_endpoint",
        "create_unix_connection", "create_unix_server", "connect_accepted_socket",
        "connect_read_pipe", "connect_write_pipe", "start_tls", "sendfile",
        "getaddrinfo", "getnameinfo", "sock_connect", "sock_accept", "sock_recv",
        "sock_recv_into", "sock_recvfrom", "sock_recvfrom_into", "sock_sendall",
        "sock_sendto", "sock_sendfile", "subprocess_exec", "subprocess_shell",
        "add_reader", "remove_reader", "add_writer", "remove_writer",
        "add_signal_handler", "remove_signal_handler",
    ), _IO_HINT),
    **dict.fromkeys((
        "run_in_executor", "set_default_executor", "shutdown_default_executor",
        "call_soon_threadsafe",
    ), _THREAD_HINT),
    **dict.fromkeys((
        "run_forever", "run_until_complete", "stop", "set_task_factory",
        "set_exception_handler",
    ), _LOOP_HINT),
}


def _unsupported_loop_api(name: str, hint: str) -> Callable[..., Any]:
    def unsupported(self: DurableEventLoop, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError(f"loop.{name}() is not supported in durable orchestrators. {hint}")

    unsupported.__name__ = unsupported.__qualname__ = name
    return unsupported


for _name, _hint in _UNSUPPORTED_LOOP_APIS.items():
    if _name not in vars(DurableEventLoop):
        setattr(DurableEventLoop, _name, _unsupported_loop_api(_name, _hint))
