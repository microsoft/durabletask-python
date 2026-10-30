# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Real Pydantic AI parallel tools with ordinary activities and no trampoline.

This small proof adapter retains model/tool registration and JSON codecs. It is
not a replacement for the framework adapter's full serialization/error policy.
"""

import asyncio
import json
from contextvars import ContextVar
from datetime import timezone
from typing import Any

import pytest

from durabletask import task
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from tests.durabletask.test_coroutine_orchestrators import _completion, _execute, _start


@pytest.mark.parametrize("fail_tool", [False, True])
def test_pydantic_agent_parallel_tools_replay_without_bridge(fail_tool: bool) -> None:
    pytest.importorskip("pydantic_ai", reason="The framework requires Python 3.11+")
    from pydantic import TypeAdapter
    from pydantic_ai import Agent
    from pydantic_ai.capabilities import AbstractCapability
    from pydantic_ai.messages import ModelMessage, ModelResponse
    from pydantic_ai.models import ModelRequestParameters
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.settings import ModelSettings

    context: ContextVar[task.OrchestrationContext] = ContextVar("context")
    request_codec = TypeAdapter(tuple[list[ModelMessage], ModelSettings | None, ModelRequestParameters])
    response_codec = TypeAdapter(ModelResponse)
    invoked: list[str] = []

    class DurableModel(TestModel):
        async def request(self, messages: Any, model_settings: Any, model_request_parameters: Any) -> ModelResponse:
            payload = request_codec.dump_python((messages, model_settings, model_request_parameters), mode="json")
            # This proof adapter explicitly canonicalizes framework message
            # metadata; the SDK must not intercept wall-clock calls globally.
            stamp = context.get().current_utc_datetime.replace(tzinfo=timezone.utc).isoformat()
            for message in payload[0]:
                message["timestamp"] = stamp
                for part in message["parts"]:
                    if "timestamp" in part:
                        part["timestamp"] = stamp
            result = await context.get().call_activity("model", input=payload)
            return response_codec.validate_python(result)

    async def model(payload: Any) -> Any:
        messages, settings, parameters = request_codec.validate_python(payload)
        result = await TestModel(custom_output_text="done").request(messages, settings, parameters)
        return response_codec.dump_python(result, mode="json")

    async def first_activity(value: int) -> str:
        if fail_tool:
            raise RuntimeError("recorded tool failure")
        return f"first:{value}"

    async def second_activity(value: int) -> str:
        return f"second:{value}"

    activities = {"model": model, "first": first_activity, "second": second_activity}

    class ReplayClock(AbstractCapability[None]):
        def _run_clock(self) -> Any:
            return lambda: context.get().current_utc_datetime.replace(tzinfo=timezone.utc)

    agent = Agent(DurableModel(), name="proof", capabilities=[ReplayClock()])

    @agent.tool_plain
    async def first(value: int) -> str:
        return await context.get().call_activity("first", input=value, return_type=str)

    @agent.tool_plain
    async def second(value: int) -> str:
        return await context.get().call_activity("second", input=value, return_type=str)

    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> str:
        token = context.set(ctx)
        try:
            result = await agent.run(
                "Call both tools.", run_id=str(ctx.new_uuid()), conversation_id=ctx.instance_id)
            return result.output
        finally:
            context.reset(token)

    history = _start()
    result = _execute(orchestrator, [], history)
    batches: list[list[str]] = []
    while not any(action.HasField("completeOrchestration") for action in result.actions):
        assert result.actions
        scheduled = list(result.actions)
        batches.append([action.scheduleTask.name for action in scheduled])
        events = []
        for action in scheduled:
            assert action.HasField("scheduleTask")
            name = action.scheduleTask.name
            history.append(helpers.new_task_scheduled_event(action.id, name))
            payload = json.loads(action.scheduleTask.input.value)
            invoked.append(name)
            try:
                output = asyncio.run(activities[name](payload))
            except RuntimeError as error:
                events.append(helpers.new_task_failed_event(action.id, error))
            else:
                events.append(helpers.new_task_completed_event(action.id, json.dumps(output)))
        # Replay twice with identical recorded results: no model/tool body reruns.
        before = list(invoked)
        first_result = _execute(orchestrator, history, events)
        second_result = _execute(orchestrator, history, events)
        assert first_result.actions == second_result.actions
        assert invoked == before
        history += events
        result = first_result
        assert len(batches) < 10
    complete = _completion(result)
    if fail_tool:
        assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
        assert "recorded tool failure" in complete.failureDetails.errorMessage
        assert batches == [["model"], ["first", "second"]]
        assert invoked == ["model", "first", "second"]
        return
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED, complete.failureDetails
    assert json.loads(complete.result.value) == "done"
    assert batches == [["model"], ["first", "second"], ["model"]]
    assert invoked == ["model", "first", "second", "model"]
