# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import asyncio
import functools
import json
import logging
import warnings
from typing import Any

import pytest

from durabletask import task, worker
from durabletask.serialization import JsonDataConverter

logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.DEBUG)
TEST_LOGGER = logging.getLogger("tests")
TEST_INSTANCE_ID = 'abc123'
TEST_TASK_ID = 42


def test_activity_inputs():
    """Validates activity function input population"""
    def test_activity(ctx: task.ActivityContext, test_input: Any):
        # return all activity inputs back as the output
        return test_input, ctx.orchestration_id, ctx.task_id

    activity_input = "Hello, 世界!"
    executor, name = _get_activity_executor(test_activity)
    result = executor.execute(TEST_INSTANCE_ID, name, TEST_TASK_ID, json.dumps(activity_input))
    assert result is not None

    result_input, result_orchestration_id, result_task_id = json.loads(result)
    assert activity_input == result_input
    assert TEST_INSTANCE_ID == result_orchestration_id
    assert TEST_TASK_ID == result_task_id


def test_activity_not_registered():

    def test_activity(ctx: task.ActivityContext, _):
        pass  # not used

    executor, _ = _get_activity_executor(test_activity)

    caught_exception: Exception | None = None
    try:
        executor.execute(TEST_INSTANCE_ID, "Bogus", TEST_TASK_ID, None)
    except Exception as ex:
        caught_exception = ex

    assert type(caught_exception) is worker.ActivityNotRegisteredError
    assert "Bogus" in str(caught_exception)


def test_async_activity_inputs():
    """Validates that async activity functions are awaited and get their inputs"""
    async def test_activity(ctx: task.ActivityContext, test_input: Any):
        await asyncio.sleep(0)
        return test_input, ctx.orchestration_id, ctx.task_id

    activity_input = "Hello, 世界!"
    executor, name = _get_activity_executor(test_activity)
    result = asyncio.run(
        executor.execute_async(TEST_INSTANCE_ID, name, TEST_TASK_ID, json.dumps(activity_input))
    )
    assert result is not None

    result_input, result_orchestration_id, result_task_id = json.loads(result)
    assert activity_input == result_input
    assert TEST_INSTANCE_ID == result_orchestration_id
    assert TEST_TASK_ID == result_task_id


def test_async_activity_exception_propagates():
    async def test_activity(ctx: task.ActivityContext, _):
        await asyncio.sleep(0)
        raise ValueError("boom")

    executor, name = _get_activity_executor(test_activity)
    with pytest.raises(ValueError, match="boom"):
        asyncio.run(executor.execute_async(TEST_INSTANCE_ID, name, TEST_TASK_ID, None))


def test_execute_async_runs_sync_activities():
    def test_activity(ctx: task.ActivityContext, test_input: int):
        return test_input + 1

    executor, name = _get_activity_executor(test_activity)
    result = asyncio.run(executor.execute_async(TEST_INSTANCE_ID, name, TEST_TASK_ID, json.dumps(1)))
    assert result == json.dumps(2)


def test_sync_execute_rejects_awaitable_result():
    """A sync call that returns a coroutine fails with a clear error instead of a serialization error"""
    async def test_activity(ctx: task.ActivityContext, _):
        return "never"

    def wrapper(ctx: task.ActivityContext, test_input: Any):
        return test_activity(ctx, test_input)

    executor, name = _get_activity_executor(wrapper)
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)  # the coroutine is closed, not leaked
        with pytest.raises(TypeError, match="async def"):
            executor.execute(TEST_INSTANCE_ID, name, TEST_TASK_ID, None)


def test_is_async_callable():
    async def async_fn(ctx, _):
        pass

    def sync_fn(ctx, _):
        pass

    class AsyncCallable:
        async def __call__(self, ctx, _):
            pass

    class SyncCallable:
        def __call__(self, ctx, _):
            pass

    assert worker._is_async_callable(async_fn)
    assert worker._is_async_callable(functools.partial(async_fn))
    assert worker._is_async_callable(AsyncCallable())
    assert not worker._is_async_callable(sync_fn)
    assert not worker._is_async_callable(SyncCallable())


def _get_activity_executor(fn: task.Activity) -> tuple[worker._ActivityExecutor, str]:
    registry = worker._Registry()
    name = registry.add_activity(fn)
    executor = worker._ActivityExecutor(registry, TEST_LOGGER, JsonDataConverter())
    return executor, name
