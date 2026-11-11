# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Replay simulation helper for unit-testing generator or coroutine orchestrators."""

import asyncio
from collections.abc import Coroutine, Generator
from typing import Any


def orchestrator_generator_wrapper(
        generator: Generator[Any, Any, Any] | Coroutine[Any, Any, Any]) -> Generator[Any, None, None]:
    """Drive a user orchestrator the way the Durable replay engine does.

    A Durable Functions orchestrator yields or awaits durable task objects and
    is resumed with each task's result until it returns the orchestration
    output. In a real execution the Durable Task runtime performs this drive
    loop during replay. This wrapper reproduces that loop in-process so an
    orchestrator can be unit tested by mocking its context: each yielded task is
    re-yielded to the caller, the task's ``result`` is sent back into the
    orchestrator, and any exception raised while reading ``result`` is thrown
    back into the orchestrator (mirroring how failed activities, timers, or
    other tasks surface during replay).

    This mock-based helper handles direct durable awaits in synchronous or
    async tests. It does not simulate the runtime's asyncio scheduler; use
    replay/backend tests for gather, task groups, sleeps and timeouts.

    Parameters
    ----------
    generator: Generator[Any, Any, Any] | Coroutine[Any, Any, Any]
        The user orchestrator generator or coroutine. It must suspend on tasks
        that expose a ``result`` attribute and to be resumed with those results
        until it returns the orchestration output.

    Yields
    ------
    Any
        Each task object yielded by the orchestrator, followed by the
        orchestrator's final return value.
    """
    def step(value: Any = None, *, error: Exception | None = None) -> Any:
        previous_loop = asyncio._get_running_loop()
        try:
            if isinstance(generator, Coroutine):
                # Direct durable awaits belong to this mock driver, not the
                # caller's asyncio loop (or an enclosing durable dispatch).
                asyncio._set_running_loop(None)
            return generator.throw(error) if error is not None else generator.send(value)
        finally:
            asyncio._set_running_loop(previous_loop)

    try:
        previous = step()
    except StopIteration as e:
        # An orchestrator that returns immediately without yielding any task
        # completes right away (mirroring the runtime, which finishes the
        # orchestration on the first return). Surface its output as the final
        # value and stop -- letting StopIteration escape would become RuntimeError.
        yield e.value
        return
    yield previous
    while True:
        try:
            try:
                previous_result = previous.result
            except Exception as e:
                # Simulated activity exceptions, timer-interrupted exceptions,
                # or any other case where reading a task result would throw:
                # push the exception back into the orchestrator as replay does.
                previous = step(error=e)
            else:
                previous = step(previous_result)
            yield previous
        except StopIteration as e:
            # The orchestrator returned; surface its output as the final value.
            yield e.value
            return
