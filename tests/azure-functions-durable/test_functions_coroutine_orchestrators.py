# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Async orchestrator dispatch through the Functions entry points."""

import asyncio
import base64
import gc
import json
import warnings
from datetime import datetime, timedelta
from typing import Any

import pytest

import azure.durable_functions as df
from azure.durable_functions.internal.compat.orchestration_context import wrap_orchestrator
from azure.durable_functions.orchestrator import Orchestrator
from azure.durable_functions.worker import DurableFunctionsWorker
from durabletask import task
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb


def _encoded_start(name: str, instance_id: str = "instance") -> str:
    request = pb.OrchestratorRequest(instanceId=instance_id, newEvents=[
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, instance_id, "null"),
    ])
    return base64.b64encode(request.SerializeToString()).decode()


@pytest.mark.parametrize("entry", ["worker-sync", "worker-async", "decorator", "orchestrator-create"])
@pytest.mark.parametrize("borrowed", [False, True], ids=["coroutines", "borrowed-handles"])
async def test_coroutine_replay_through_functions_entry_points(entry: str, borrowed: bool) -> None:
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

    worker = DurableFunctionsWorker()
    handle = Orchestrator.create(modern)
    app = df.DFApp()
    builder = app.orchestration_trigger(context_name="ctx")(modern)
    registered = builder.build().get_user_function()
    instance_id = "async-functions"

    async def invoke(request: pb.OrchestratorRequest) -> pb.OrchestratorResponse:
        encoded = base64.b64encode(request.SerializeToString()).decode()
        if entry == "worker-sync":
            output = worker.execute_orchestration_request(modern, encoded)
        elif entry == "worker-async":
            output = await worker.execute_orchestration_request_async(modern, encoded)
        elif entry == "decorator":
            output = await registered(ctx=encoded)
        else:
            output = handle(encoded)
        response = pb.OrchestratorResponse()
        response.ParseFromString(base64.b64decode(output))
        return response

    start = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(modern.__name__, instance_id, "3"),
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


def test_owned_event_timeout_replacement_through_functions_worker() -> None:
    async def modern(ctx: task.AsyncOrchestrationContext, _: Any) -> str:
        try:
            return await ctx.wait_for_external_event("approval", timeout=timedelta(seconds=1), data_type=str)
        except TimeoutError:
            return await ctx.wait_for_external_event("approval", data_type=str)

    worker = DurableFunctionsWorker()
    instance_id = "owned-event-functions"
    now = datetime(2026, 1, 1)

    def invoke(old: list[pb.HistoryEvent], new: list[pb.HistoryEvent]) -> pb.OrchestratorResponse:
        request = pb.OrchestratorRequest(instanceId=instance_id, pastEvents=old, newEvents=new)
        encoded = base64.b64encode(request.SerializeToString()).decode()
        response = pb.OrchestratorResponse()
        response.ParseFromString(base64.b64decode(worker.execute_orchestration_request(modern, encoded)))
        return response

    start = [
        helpers.new_orchestrator_started_event(now),
        helpers.new_execution_started_event(modern.__name__, instance_id, "null"),
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


def test_two_argument_async_orchestrator_passes_through() -> None:
    async def native(ctx: task.AsyncOrchestrationContext, value: int) -> int:
        return await ctx.call_activity("work", input=value, return_type=int)

    assert wrap_orchestrator(native) is native
    builder = df.DFApp().orchestration_trigger(context_name="ctx", input_type=int)(native)
    registered = builder.build().get_user_function()
    assert registered.orchestrator_function is native


@pytest.mark.parametrize("entry", ["decorator", "orchestrator-create", "worker-sync", "worker-async"])
async def test_single_argument_async_orchestrator_is_rejected(entry: str) -> None:
    called = False

    async def legacy(context: df.DurableOrchestrationContext) -> str:
        nonlocal called
        called = True
        return "unreachable"

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(TypeError, match=r"legacy.*async def orchestrator\(ctx: task\.AsyncOrchestrationContext"):
            if entry == "decorator":
                df.DFApp().orchestration_trigger(context_name="context")(legacy)
            elif entry == "orchestrator-create":
                Orchestrator.create(legacy)
            elif entry == "worker-sync":
                DurableFunctionsWorker().execute_orchestration_request(legacy, _encoded_start("legacy"))
            else:
                await DurableFunctionsWorker().execute_orchestration_request_async(
                    legacy, _encoded_start("legacy"))
        gc.collect()

    assert not called
    assert [str(w.message) for w in caught if issubclass(w.category, RuntimeWarning)] == []
