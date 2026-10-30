# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Coroutine replay through native and compatibility Functions entry points."""

import base64
import json
from collections.abc import Callable
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
async def test_coroutine_replay_through_functions_entry_points(native: bool, entry: str) -> None:
    async def modern(ctx: task.OrchestrationContext, value: int) -> list[Any]:
        return await task.when_all([
            ctx.call_activity("work", input=value, return_type=str),
            ctx.call_activity("work", input=value + 1, return_type=str),
        ])

    async def compatible(ctx: df.DurableOrchestrationContext) -> list[Any]:
        value = ctx.get_input()
        return await ctx.task_all([
            ctx.call_activity("work", value),
            ctx.call_activity("work", value + 1),
        ])

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
