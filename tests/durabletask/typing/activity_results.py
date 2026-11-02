# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Static regression checks for the result types of scheduled activities."""

from typing import Any, assert_type

from durabletask import task


def sync_activity(ctx: task.ActivityContext, value: int) -> float:
    return float(value)


async def async_activity(ctx: task.ActivityContext, value: int) -> float:
    return float(value)


def check_activity_result_types(ctx: task.OrchestrationContext) -> None:
    assert_type(ctx.call_activity(sync_activity, input=1), task.CompletableTask[float])
    # An async activity's task completes with the awaited result, not a coroutine.
    assert_type(ctx.call_activity(async_activity, input=1), task.CompletableTask[float])
    assert_type(ctx.call_activity(async_activity, input=1, return_type=int), task.CompletableTask[int])
    assert_type(ctx.call_activity("async_activity", input=1), task.CompletableTask[Any])
