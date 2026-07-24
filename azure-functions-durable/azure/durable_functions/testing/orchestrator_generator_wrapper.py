# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Replay simulation helper for unit-testing orchestrator generators."""

from typing import Any, Generator


def orchestrator_generator_wrapper(
        generator: Generator[Any, Any, Any]) -> Generator[Any, None, None]:
    """Drive a user orchestrator generator the way the Durable replay engine does.

    A Durable Functions orchestrator is a generator that yields task objects and
    is resumed with each task's result until it returns the orchestration
    output. In a real execution the Durable Task runtime performs this drive
    loop during replay. This wrapper reproduces that loop in-process so an
    orchestrator can be unit tested by mocking its context: each yielded task is
    re-yielded to the caller, the task's ``result`` is sent back into the
    orchestrator, and any exception raised while reading ``result`` is thrown
    back into the orchestrator (mirroring how failed activities, timers, or
    other tasks surface during replay).

    Parameters
    ----------
    generator: Generator[Any, Any, Any]
        The user orchestrator generator. It is expected to yield task objects
        that expose a ``result`` attribute and to be resumed with those results
        until it returns the orchestration output.

    Yields
    ------
    Any
        Each task object yielded by the orchestrator, followed by the
        orchestrator's final return value.
    """
    previous = next(generator)
    yield previous
    while True:
        try:
            try:
                previous_result = previous.result
            except Exception as e:
                # Simulated activity exceptions, timer-interrupted exceptions,
                # or any other case where reading a task result would throw:
                # push the exception back into the orchestrator as replay does.
                previous = generator.throw(e)
            else:
                previous = generator.send(previous_result)
            yield previous
        except StopIteration as e:
            # The orchestrator returned; surface its output as the final value.
            yield e.value
            return
