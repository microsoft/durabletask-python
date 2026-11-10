# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Coroutine replay through native and compatibility Functions entry points."""

import base64
import asyncio
import json
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

import pytest

import azure.durable_functions as df
from azure.durable_functions.orchestrator import Orchestrator
from azure.durable_functions.testing import orchestrator_generator_wrapper
from azure.durable_functions.worker import DurableFunctionsWorker
from durabletask import task
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb


@pytest.mark.parametrize("native", [False, True], ids=["compat", "native"])
@pytest.mark.parametrize("entry", ["worker-sync", "worker-async", "decorator", "v1-handle"])
@pytest.mark.parametrize("borrowed", [False, True], ids=["coroutines", "borrowed-handles"])
async def test_coroutine_replay_through_functions_entry_points(native: bool, entry: str, borrowed: bool) -> None:
    async def modern(ctx: task.AsyncOrchestrationContext, value: int) -> list[Any]:
        if not borrowed:
            async with asyncio.TaskGroup() as group:
                first = group.create_task(ctx.call_activity("work", input=value, return_type=str))
                second = group.create_task(ctx.call_activity("work", input=value + 1, return_type=str))
            return [first.result(), second.result()]
        handles = ctx.as_task_context()
        return list(await asyncio.gather(
            handles.call_activity("work", input=value, return_type=str),
            handles.call_activity("work", input=value + 1, return_type=str),
        ))

    async def compatible(ctx: df.AsyncDurableOrchestrationContext) -> list[Any]:
        value = ctx.get_input()
        if not borrowed:
            async with asyncio.TaskGroup() as group:
                first = group.create_task(ctx.call_activity("work", input_=value))
                second = group.create_task(ctx.call_activity("work", input_=value + 1))
            return [first.result(), second.result()]
        handles = ctx.as_task_context()
        return list(await asyncio.gather(
            handles.call_activity("work", value),
            handles.call_activity("work", value + 1),
        ))

    fn: Callable[..., Any] = modern if native else compatible
    worker = DurableFunctionsWorker()
    handle = Orchestrator.create(fn)
    app = df.DFApp()
    builder = app.orchestration_trigger(context_name="ctx")(fn)
    registered = builder.build().get_user_function()
    instance_id = "async-functions"

    async def invoke(request: pb.OrchestratorRequest) -> pb.OrchestratorResponse:
        encoded = base64.b64encode(request.SerializeToString()).decode()
        if entry == "worker-sync":
            output = worker.execute_orchestration_request(fn, encoded)
        elif entry == "worker-async":
            output = await worker.execute_orchestration_request_async(fn, encoded)
        elif entry == "decorator":
            output = await registered(ctx=encoded)
        else:
            output = handle(encoded)
        response = pb.OrchestratorResponse()
        response.ParseFromString(base64.b64decode(output))
        return response

    start = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(fn.__name__, instance_id, "3"),
    ]
    first = await invoke(pb.OrchestratorRequest(instanceId=instance_id, newEvents=start))
    assert [a.scheduleTask.name for a in first.actions] == ["work", "work"]
    assert [json.loads(a.scheduleTask.input.value) for a in first.actions] == [3, 4]
    assert [a.id for a in first.actions] == [1, 2]

    response = await invoke(pb.OrchestratorRequest(
        instanceId=instance_id,
        pastEvents=start + [helpers.new_task_scheduled_event(i, "work") for i in (1, 2)],
        newEvents=[
            helpers.new_task_completed_event(2, '"second"'),
            helpers.new_task_completed_event(1, '"first"'),
        ],
    ))
    assert len(response.actions) == 1
    complete = response.actions[0].completeOrchestration
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(complete.result.value) == ["first", "second"]


@pytest.mark.parametrize("native", [False, True], ids=["compat", "native"])
def test_owned_event_timeout_replacement_through_functions_worker(native: bool) -> None:
    async def modern(ctx: task.AsyncOrchestrationContext, _: Any) -> str:
        try:
            return await ctx.wait_for_external_event("approval", timeout=timedelta(seconds=1), data_type=str)
        except TimeoutError:
            return await ctx.wait_for_external_event("approval", data_type=str)

    async def compatible(ctx: df.AsyncDurableOrchestrationContext) -> str:
        try:
            return await ctx.wait_for_external_event("approval", str, timeout=timedelta(seconds=1))
        except TimeoutError:
            return await ctx.wait_for_external_event("approval", str)

    fn: Callable[..., Any] = modern if native else compatible
    worker = DurableFunctionsWorker()
    instance_id = "owned-event-functions"
    now = datetime(2026, 1, 1)

    def invoke(old: list[pb.HistoryEvent], new: list[pb.HistoryEvent]) -> pb.OrchestratorResponse:
        request = pb.OrchestratorRequest(instanceId=instance_id, pastEvents=old, newEvents=new)
        encoded = base64.b64encode(request.SerializeToString()).decode()
        response = pb.OrchestratorResponse()
        response.ParseFromString(base64.b64decode(worker.execute_orchestration_request(fn, encoded)))
        return response

    start = [
        helpers.new_orchestrator_started_event(now),
        helpers.new_execution_started_event(fn.__name__, instance_id, "null"),
    ]
    initial = invoke([], start)
    assert len(initial.actions) == 1
    assert initial.actions[0].createTimer.fireAt.ToDatetime() == now + timedelta(seconds=1)
    old = start + [helpers.new_timer_created_event(1, now + timedelta(seconds=1))]
    timeout = [
        helpers.new_orchestrator_started_event(now + timedelta(seconds=1)),
        helpers.new_timer_fired_event(1, now + timedelta(seconds=1)),
    ]
    assert not invoke(old, timeout).actions
    response = invoke(old + timeout, [helpers.new_event_raised_event("approval", '"approved"')])
    assert len(response.actions) == 1
    complete = response.actions[0].completeOrchestration
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED, complete.failureDetails
    assert json.loads(complete.result.value) == "approved"


@pytest.mark.parametrize("fail", [False, True])
def test_testing_wrapper_drives_coroutine(fail: bool) -> None:
    pending = task.CompletableTask[str]()

    async def orchestrator() -> str:
        try:
            return await pending
        except task.TaskFailedError:
            return "caught"

    values = orchestrator_generator_wrapper(orchestrator())
    assert next(values) is pending
    if fail:
        pending.fail("boom", ValueError("boom"))
    else:
        pending.complete("done")
    assert list(values) == ["caught" if fail else "done"]


def test_testing_wrapper_drives_immediate_coroutine() -> None:
    async def orchestrator() -> str:
        return "done"

    assert list(orchestrator_generator_wrapper(orchestrator())) == ["done"]


@pytest.mark.parametrize("fail", [False, True])
async def test_testing_wrapper_isolates_each_step_from_async_test_loop(fail: bool) -> None:
    loop = asyncio.get_running_loop()
    caller = asyncio.current_task()
    first = task.CompletableTask[str]()
    second = task.CompletableTask[str]()

    async def orchestrator() -> str:
        assert asyncio._get_running_loop() is None
        try:
            await first
        except task.TaskFailedError:
            assert asyncio._get_running_loop() is None
        assert asyncio._get_running_loop() is None
        return await second

    values = orchestrator_generator_wrapper(orchestrator())
    assert next(values) is first
    assert asyncio.get_running_loop() is loop
    assert asyncio.current_task() is caller
    if fail:
        first.fail("boom", ValueError("boom"))
    else:
        first.complete("first")
    assert next(values) is second
    assert asyncio.get_running_loop() is loop
    assert asyncio.current_task() is caller
    second.complete("done")
    assert list(values) == ["done"]
    assert asyncio.get_running_loop() is loop
    assert asyncio.current_task() is caller
    await asyncio.sleep(0)


@pytest.mark.parametrize("error", [ValueError("test failure"), SystemExit("test exit")])
async def test_testing_wrapper_restores_async_test_loop_on_exception(error: BaseException) -> None:
    loop = asyncio.get_running_loop()
    caller = asyncio.current_task()
    pending = task.CompletableTask[str]()

    async def orchestrator() -> None:
        await pending
        raise error

    values = orchestrator_generator_wrapper(orchestrator())
    assert next(values) is pending
    pending.complete("done")
    with pytest.raises(type(error)) as caught:
        next(values)
    assert caught.value is error
    assert asyncio.get_running_loop() is loop
    assert asyncio.current_task() is caller
    await asyncio.sleep(0)
