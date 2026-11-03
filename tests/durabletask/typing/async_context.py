# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""The async surface returns native coroutines with inferred result types."""

import asyncio
from collections.abc import Coroutine
from datetime import timedelta
from typing import Any, assert_type

from durabletask import task
from durabletask.entities import EntityInstanceId, EntityLock


def increment(ctx: task.ActivityContext, value: int) -> int:
    return value + 1


async def child(ctx: task.OrchestrationContext, value: int) -> str:
    return str(value)


async def check_cold_operations(ctx: task.OrchestrationContext, entity: EntityInstanceId) -> None:
    ops = ctx.as_async()
    assert_type(ops, task.AsyncOrchestrationContext)
    activity: Coroutine[Any, Any, int] = ops.call_activity(increment, input=1)
    assert_type(await activity, int)
    assert_type(await ops.call_activity("named", return_type=str), str)
    assert_type(await ops.call_activity("named"), Any)
    assert_type(await ops.call_sub_orchestrator(child, input=1), str)
    assert_type(await ops.call_sub_orchestrator("child", return_type=int), int)
    assert_type(await ops.call_sub_orchestrator("child"), Any)
    assert_type(await ops.call_entity(entity, "get", return_type=int), int)
    assert_type(await ops.call_entity(entity, "get"), Any)
    assert_type(await ops.create_timer(timedelta(seconds=1)), None)
    assert_type(await ops.receive_event("event", data_type=str), str)
    assert_type(await ops.receive_event("event"), Any)
    assert_type(await ops.lock_entities([entity]), EntityLock)
    assert_type(asyncio.create_task(ops.call_activity(increment, input=1)), asyncio.Task[int])
    async with asyncio.TaskGroup() as group:
        assert_type(group.create_task(ops.receive_event("event", data_type=str)), asyncio.Task[str])
    operation = ctx.call_activity(increment, input=1)
    assert_type(operation, task.CompletableTask[int])
    assert_type(await operation.wait(), int)
    assert_type(asyncio.create_task(operation.wait()), asyncio.Task[int])
