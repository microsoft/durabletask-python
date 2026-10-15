# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Replay-level coverage for the async/await orchestration prototype."""

import asyncio
import json
import logging
from collections.abc import AsyncGenerator, Generator
from datetime import datetime, timedelta
from typing import Any

import pytest

from durabletask import entities, task, worker
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.serialization import JsonDataConverter


INSTANCE_ID = "coroutine-test"
NOW = datetime(2026, 1, 1)


def _start() -> list[pb.HistoryEvent]:
    return [
        helpers.new_orchestrator_started_event(NOW),
        helpers.new_execution_started_event("test", INSTANCE_ID, json.dumps(3)),
    ]


def _execute(
    fn: task.Orchestrator[Any, Any],
    old: list[pb.HistoryEvent],
    new: list[pb.HistoryEvent],
) -> worker.ExecutionResults:
    registry = worker._Registry()  # pyright: ignore[reportPrivateUsage]
    registry.add_named_orchestrator("test", fn)
    executor = worker._OrchestrationExecutor(  # pyright: ignore[reportPrivateUsage]
        registry, logging.getLogger(__name__), JsonDataConverter())
    return executor.execute(INSTANCE_ID, old, new)


def _completion(result: worker.ExecutionResults) -> pb.CompleteOrchestrationAction:
    completions = [a.completeOrchestration for a in result.actions if a.HasField("completeOrchestration")]
    assert len(completions) == 1
    return completions[0]


def test_coroutine_immediate_return() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, value: int) -> int:
        return value

    complete = _completion(_execute(orchestrator, [], _start()))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(complete.result.value) == 3


def test_await_is_concrete_and_completed_tasks_do_not_suspend() -> None:
    assert "__await__" not in task.Task.__abstractmethods__

    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> list[int]:
        completed = task.CompletableTask[int]()
        completed.complete(7)
        return [await completed, await completed]

    assert json.loads(_completion(_execute(orchestrator, [], _start())).result.value) == [7, 7]


def test_precompleted_failure_and_cancellation_are_catchable() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> list[str]:
        failed = task.CompletableTask[int]()
        failed.fail("failed", ValueError("boom"))
        cancelled = task.TimerTask()
        cancelled.cancel()
        results: list[str] = []
        try:
            await failed
        except task.TaskFailedError:
            results.append("failed")
        try:
            await cancelled
        except task.TaskCancelledError:
            results.append("cancelled")
        return results

    assert json.loads(_completion(_execute(orchestrator, [], _start())).result.value) == [
        "failed", "cancelled"]


def test_nested_async_helpers_replay_with_generator_action_parity() -> None:
    async def helper(ctx: task.OrchestrationContext, value: int) -> int:
        return await ctx.call_activity("increment", input=value, return_type=int)

    async def coroutine(ctx: task.OrchestrationContext, value: int) -> int:
        first = await helper(ctx, value)
        return await helper(ctx, first)

    def generator(ctx: task.OrchestrationContext, value: int) -> Generator[task.Task[Any], Any, int]:
        first = yield ctx.call_activity("increment", input=value, return_type=int)
        return (yield ctx.call_activity("increment", input=first, return_type=int))

    batches = [
        _start(),
        [helpers.new_task_scheduled_event(1, "increment"),
         helpers.new_task_completed_event(1, json.dumps(4))],
        [helpers.new_task_scheduled_event(2, "increment"),
         helpers.new_task_completed_event(2, json.dumps(5))],
    ]
    old: list[pb.HistoryEvent] = []
    result: worker.ExecutionResults | None = None
    for new in batches:
        result = _execute(coroutine, old, new)
        assert result.actions == _execute(generator, old, new).actions
        old += new
    assert result is not None
    assert json.loads(_completion(result).result.value) == 5


def test_fan_out_waits_for_all_and_preserves_input_order() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> list[str]:
        pending: list[task.Task[str]] = [
            ctx.call_activity("work", input=i, return_type=str) for i in range(2)]
        return await task.when_all(pending)

    old = _start()
    assert len(_execute(orchestrator, [], old).actions) == 2
    old += [helpers.new_task_scheduled_event(i, "work") for i in (1, 2)]
    second = [helpers.new_task_completed_event(2, '"second"')]
    assert _execute(orchestrator, old, second).actions == []
    complete = _completion(_execute(
        orchestrator, old + second, [helpers.new_task_completed_event(1, '"first"')]))
    assert json.loads(complete.result.value) == ["first", "second"]


@pytest.mark.parametrize("caught", [False, True])
def test_activity_failure_is_thrown_into_coroutine(caught: bool) -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> str:
        try:
            await ctx.call_activity("boom")
        except task.TaskFailedError:
            if not caught:
                raise
            return "caught"
        return "unexpected"

    old = _start() + [helpers.new_task_scheduled_event(1, "boom")]
    complete = _completion(_execute(
        orchestrator, old, [helpers.new_task_failed_event(1, ValueError("boom"))]))
    assert complete.orchestrationStatus == (
        pb.ORCHESTRATION_STATUS_COMPLETED if caught else pb.ORCHESTRATION_STATUS_FAILED)
    if caught:
        assert json.loads(complete.result.value) == "caught"
    else:
        assert complete.failureDetails.innerFailure.errorMessage == "boom"


@pytest.mark.parametrize("event_wins", [False, True])
def test_when_any_timer_and_external_event(event_wins: bool) -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> Any:
        event = ctx.wait_for_external_event("approval")
        timer = ctx.create_timer(timedelta(seconds=1))
        winner = await task.when_any([event, timer])
        if winner is event:
            timer.cancel()
            return await event
        return "timeout"

    deadline = NOW + timedelta(seconds=1)
    old = _start() + [helpers.new_timer_created_event(1, deadline)]
    new = [helpers.new_event_raised_event("approval", '"approved"')] if event_wins else [
        helpers.new_timer_fired_event(1, deadline)]
    complete = _completion(_execute(orchestrator, old, new))
    assert json.loads(complete.result.value) == ("approved" if event_wins else "timeout")


def test_sub_orchestration_and_replay_flags() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> list[Any]:
        before = ctx.is_replaying
        value = await ctx.call_sub_orchestrator("child", instance_id="child-test", return_type=str)
        return [value, before, ctx.is_replaying]

    old = _start() + [helpers.new_sub_orchestration_created_event(1, "child", "child-test")]
    complete = _completion(_execute(
        orchestrator, old, [helpers.new_sub_orchestration_completed_event(1, '"done"')]))
    assert json.loads(complete.result.value) == ["done", True, False]


def test_retry_timer_resumes_same_coroutine() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> Any:
        return await ctx.call_activity(
            "retry", retry_policy=task.RetryPolicy(
                first_retry_interval=timedelta(seconds=1), max_number_of_attempts=2))

    old = _start() + [helpers.new_task_scheduled_event(1, "retry")]
    failure = [helpers.new_task_failed_event(1, ValueError("retry me"))]
    retry = _execute(orchestrator, old, failure).actions
    assert len(retry) == 1
    assert retry[0].id == 2
    deadline = retry[0].createTimer.fireAt.ToDatetime()
    old += failure + [helpers.new_timer_created_event(2, deadline)]
    fired = [helpers.new_orchestrator_started_event(deadline), helpers.new_timer_fired_event(2, deadline)]
    scheduled = _execute(orchestrator, old, fired).actions
    assert len(scheduled) == 1
    assert scheduled[0].id == 1
    assert scheduled[0].scheduleTask.name == "retry"
    old += fired + [helpers.new_task_scheduled_event(1, "retry")]
    complete = _completion(_execute(
        orchestrator, old, [helpers.new_task_completed_event(1, '"success"')]))
    assert json.loads(complete.result.value) == "success"


def test_entity_lock_and_call_suspend_coroutine() -> None:
    entity = entities.EntityInstanceId("counter", "one")

    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> Any:
        with await ctx.lock_entities([entity]):
            return await ctx.call_entity(entity, "get")

    old = _start()
    lock = _execute(orchestrator, [], old).actions[0].sendEntityMessage.entityLockRequested
    request = json.dumps({"id": lock.criticalSectionId})
    new = [
        helpers.new_event_sent_event(1, str(entity), request),
        helpers.new_event_raised_event(lock.criticalSectionId, None),
    ]
    actions = _execute(orchestrator, old, new).actions
    assert len(actions) == 1
    call = actions[0].sendEntityMessage.entityOperationCalled
    assert call.targetInstanceId.value == str(entity)
    old += new + [helpers.new_event_sent_event(2, str(entity), json.dumps({"id": call.requestId}))]
    complete = _completion(_execute(
        orchestrator, old, [helpers.new_event_raised_event(call.requestId, json.dumps({"result": "42"}))]))
    assert json.loads(complete.result.value) == 42


def test_continue_as_new_from_coroutine() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, value: int) -> None:
        await ctx.wait_for_external_event("next")
        ctx.continue_as_new(value + 1)

    complete = _completion(_execute(
        orchestrator, _start(), [helpers.new_event_raised_event("next", None)]))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_CONTINUED_AS_NEW
    assert json.loads(complete.result.value) == 4


def test_nondeterminism_still_detected_for_coroutines() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> Any:
        return await ctx.call_activity("changed")

    complete = _completion(_execute(
        orchestrator, _start(), [helpers.new_task_scheduled_event(1, "original")]))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete.failureDetails.errorType.endswith("NonDeterminismError")


@pytest.mark.parametrize("after_activity", [False, True])
def test_non_durable_suspension_is_rejected(after_activity: bool) -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> None:
        if after_activity:
            await ctx.call_activity("work")
        await asyncio.sleep(0)

    old = _start() + [helpers.new_task_scheduled_event(1, "work")] if after_activity else []
    new = [helpers.new_task_completed_event(1, "null")] if after_activity else _start()
    complete = _completion(_execute(orchestrator, old, new))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete.failureDetails.errorType == "builtins.TypeError"
    assert "must only yield or await durabletask.Task" in complete.failureDetails.errorMessage


def test_async_generator_is_rejected() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> AsyncGenerator[task.Task[Any], None]:
        yield ctx.call_activity("work", return_type=int)

    complete = _completion(_execute(orchestrator, [], _start()))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert "Async generator orchestrators are not supported" in complete.failureDetails.errorMessage
