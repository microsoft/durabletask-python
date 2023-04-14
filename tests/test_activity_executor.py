# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Any, Tuple

import simplejson as json

import durabletask.internal.shared as shared
from durabletask.task.activities import Activity, ActivityContext
from durabletask.task.execution import (ActivityExecutor,
                                        ActivityNotRegisteredError)
from durabletask.task.registry import Registry

TEST_LOGGER = shared.get_logger()
TEST_INSTANCE_ID = 'abc123'
TEST_TASK_ID = 42


def test_activity_inputs():
    """Validates activity function input population"""
    def test_activity(ctx: ActivityContext, test_input: Any):
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

    def test_activity(ctx: ActivityContext, _):
        pass  # not used

    executor, _ = _get_activity_executor(test_activity)

    caught_exception: Exception | None = None
    try:
        executor.execute(TEST_INSTANCE_ID, "Bogus", TEST_TASK_ID, None)
    except Exception as ex:
        caught_exception = ex

    assert type(caught_exception) is ActivityNotRegisteredError
    assert "Bogus" in str(caught_exception)


def _get_activity_executor(fn: Activity) -> Tuple[ActivityExecutor, str]:
    registry = Registry()
    name = registry.add_activity(fn)
    executor = ActivityExecutor(registry, TEST_LOGGER)
    return executor, name
