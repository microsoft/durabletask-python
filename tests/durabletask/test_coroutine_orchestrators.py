# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Replay-level coverage for coroutine orchestrators and generator history parity."""

import asyncio
import inspect
import json
import logging
from collections.abc import AsyncGenerator, Generator
from datetime import datetime, timedelta
from functools import partial, wraps
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
    async def orchestrator(ctx: task.AsyncOrchestrationContext, value: int) -> int:
        return value

    complete = _completion(_execute(orchestrator, [], _start()))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(complete.result.value) == 3


@pytest.mark.parametrize("shape", ["function", "wrapped", "partial", "object", "partial-object"])
def test_async_context_is_selected_for_supported_callable_shapes(shape: str) -> None:
    async def plain(ctx: task.AsyncOrchestrationContext, value: int) -> int:
        assert isinstance(ctx, task.AsyncOrchestrationContext)
        return value + 1

    @wraps(plain)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        return plain(*args, **kwargs)

    async def with_prefix(prefix: int, ctx: task.AsyncOrchestrationContext, value: int) -> int:
        assert isinstance(ctx, task.AsyncOrchestrationContext)
        return value + prefix

    class AsyncCallable:
        async def __call__(self, ctx: task.AsyncOrchestrationContext, value: int) -> int:
            assert isinstance(ctx, task.AsyncOrchestrationContext)
            return value + 1

    class AsyncPartialCallable:
        async def __call__(
            self, prefix: int, ctx: task.AsyncOrchestrationContext, value: int,
        ) -> int:
            assert isinstance(ctx, task.AsyncOrchestrationContext)
            return value + prefix

    variants = {
        "function": plain,
        "wrapped": wrapped,
        "partial": partial(with_prefix, 1),
        "object": AsyncCallable(),
        "partial-object": partial(AsyncPartialCallable(), 1),
    }
    complete = _completion(_execute(variants[shape], [], _start()))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(complete.result.value) == 4


def test_unmarked_synchronous_wrapper_returning_coroutine_is_rejected_and_closed() -> None:
    created: list[Any] = []

    async def async_orchestrator(ctx: task.AsyncOrchestrationContext, value: int) -> int:
        return value

    def wrapper(ctx: task.OrchestrationContext, value: int) -> Any:
        coroutine = async_orchestrator(ctx, value)  # type: ignore[arg-type]
        created.append(coroutine)
        return coroutine

    complete = _completion(_execute(wrapper, [], _start()))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert "synchronous orchestrator returned a coroutine" in complete.failureDetails.errorMessage
    assert inspect.getcoroutinestate(created[0]) == inspect.CORO_CLOSED


def test_async_classified_wrapper_returning_non_coroutine_is_rejected() -> None:
    async def async_orchestrator(ctx: task.AsyncOrchestrationContext, value: int) -> int:
        return value

    @wraps(async_orchestrator)
    def wrapper(*args: Any, **kwargs: Any) -> int:
        return 42

    complete = _completion(_execute(wrapper, [], _start()))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert "async orchestrator must return a coroutine" in complete.failureDetails.errorMessage


def test_async_classified_wrapper_closes_returned_generator() -> None:
    created: list[Generator[int, None, None]] = []

    async def async_orchestrator(ctx: task.AsyncOrchestrationContext, value: int) -> int:
        return value

    @wraps(async_orchestrator)
    def wrapper(*args: Any, **kwargs: Any) -> Generator[int, None, None]:
        generator = (value for value in [42])
        created.append(generator)
        return generator

    complete = _completion(_execute(wrapper, [], _start()))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert "async orchestrator must return a coroutine" in complete.failureDetails.errorMessage
    assert inspect.getgeneratorstate(created[0]) == inspect.GEN_CLOSED


def test_cyclic_async_wrapper_fails_instead_of_hanging() -> None:
    def wrapper(ctx: task.OrchestrationContext, value: int) -> int:
        return value

    wrapper.__wrapped__ = wrapper  # type: ignore[attr-defined]
    complete = _completion(_execute(wrapper, [], _start()))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert "wrapper loop" in complete.failureDetails.errorMessage


def test_async_context_metadata_and_commands_match_sync_context() -> None:
    entity = entities.EntityInstanceId("counter", "one")

    def use_context(ctx: task.OrchestrationContext | task.AsyncOrchestrationContext) -> list[Any]:
        ctx.set_custom_status({"state": "running"})
        ctx.send_event("other", "ping", data=3)
        ctx.signal_entity(entity, "add", 1)
        return [
            ctx.instance_id, ctx.parent_instance_id, ctx.version,
            ctx.current_utc_datetime.isoformat(), ctx.is_replaying, ctx.new_uuid(),
        ]

    async def async_orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> list[Any]:
        return use_context(ctx)

    def sync_orchestrator(ctx: task.OrchestrationContext, _: Any) -> list[Any]:
        return use_context(ctx)

    started = helpers.new_execution_started_event("test", INSTANCE_ID, "null", version="v1")
    started.executionStarted.parentInstance.CopyFrom(pb.ParentInstanceInfo(
        taskScheduledId=1,
        orchestrationInstance=pb.OrchestrationInstance(instanceId="parent")))
    new = [helpers.new_orchestrator_started_event(NOW), started]
    async_result = _execute(async_orchestrator, [], new)
    sync_result = _execute(sync_orchestrator, [], new)
    assert async_result.actions == sync_result.actions
    assert async_result.encoded_custom_status == sync_result.encoded_custom_status == '{"state": "running"}'
    assert json.loads(_completion(async_result).result.value)[:5] == [
        INSTANCE_ID, "parent", "v1", NOW.isoformat(), False]


def test_await_is_concrete_and_completed_tasks_do_not_suspend() -> None:
    assert "__await__" not in task.Task.__abstractmethods__

    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> list[int]:
        completed = task.CompletableTask[int]()
        completed.complete(7)
        return [await completed, await completed]

    assert json.loads(_completion(_execute(orchestrator, [], _start())).result.value) == [7, 7]


@pytest.mark.parametrize("after_activity", [False, True])
def test_generator_invalid_yield_recommends_async_authoring(after_activity: bool) -> None:
    def orchestrator(ctx: task.OrchestrationContext, _: Any) -> Generator[Any, Any, None]:
        if after_activity:
            yield ctx.call_activity("work")
        yield None

    old = _start() + [helpers.new_task_scheduled_event(1, "work")] if after_activity else []
    new = [helpers.new_task_completed_event(1, "null")] if after_activity else _start()
    complete = _completion(_execute(orchestrator, old, new))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete.failureDetails.errorType == "builtins.TypeError"
    assert "Generator orchestrators must yield durabletask.Task objects" in complete.failureDetails.errorMessage
    assert "Use async def and await for asyncio composition" in complete.failureDetails.errorMessage


def test_precompleted_failure_and_cancellation_are_catchable() -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> list[str]:
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
    async def helper(ctx: task.AsyncOrchestrationContext, value: int) -> int:
        return await ctx.call_activity("increment", input=value, return_type=int)

    async def coroutine(ctx: task.AsyncOrchestrationContext, value: int) -> int:
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
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> list[str]:
        pending: list[task.Task[str]] = [
            ctx.as_task_context().call_activity("work", input=i, return_type=str) for i in range(2)]
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
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> str:
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
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> Any:
        event = ctx.as_task_context().wait_for_external_event("approval")
        timer = ctx.as_task_context().create_timer(timedelta(seconds=1))
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
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> list[Any]:
        before = ctx.is_replaying
        value = await ctx.call_sub_orchestrator("child", instance_id="child-test", return_type=str)
        return [value, before, ctx.is_replaying]

    old = _start() + [helpers.new_sub_orchestration_created_event(1, "child", "child-test")]
    complete = _completion(_execute(
        orchestrator, old, [helpers.new_sub_orchestration_completed_event(1, '"done"')]))
    assert json.loads(complete.result.value) == ["done", True, False]


def test_retry_timer_resumes_same_coroutine() -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> Any:
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


@pytest.mark.parametrize("sub_orchestration", [False, True])
@pytest.mark.parametrize("replayed", [False, True])
@pytest.mark.parametrize("caught", [False, True])
def test_coroutine_non_retriable_failure(
        sub_orchestration: bool, replayed: bool, caught: bool) -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> Any:
        retry = task.RetryPolicy(first_retry_interval=timedelta(seconds=1), max_number_of_attempts=3)
        try:
            if sub_orchestration:
                return await ctx.call_sub_orchestrator("child", retry_policy=retry)
            return await ctx.call_activity("child", retry_policy=retry)
        except task.TaskFailedError as error:
            if not caught:
                raise
            assert error.details.message == "permanent"
            return "caught"

    old = _start()
    if sub_orchestration:
        old.append(helpers.new_sub_orchestration_created_event(1, "child", "child-instance"))
        failed = helpers.new_sub_orchestration_failed_event(1, ValueError("permanent"))
        details = failed.subOrchestrationInstanceFailed.failureDetails
    else:
        old.append(helpers.new_task_scheduled_event(1, "child"))
        failed = helpers.new_task_failed_event(1, ValueError("permanent"))
        details = failed.taskFailed.failureDetails
    details.isNonRetriable = True
    new = [helpers.new_orchestrator_started_event(NOW)]
    if replayed:
        old.append(failed)
    else:
        new.append(failed)
    result = _execute(orchestrator, old, new)
    assert len(result.actions) == 1
    complete = _completion(result)
    if caught:
        assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
        assert json.loads(complete.result.value) == "caught"
    else:
        assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
        assert complete.failureDetails.innerFailure == details


def test_entity_lock_and_call_suspend_coroutine() -> None:
    entity = entities.EntityInstanceId("counter", "one")

    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> Any:
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
    async def orchestrator(ctx: task.AsyncOrchestrationContext, value: int) -> None:
        await ctx.wait_for_external_event("next")
        ctx.continue_as_new(value + 1)

    complete = _completion(_execute(
        orchestrator, _start(), [helpers.new_event_raised_event("next", None)]))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_CONTINUED_AS_NEW
    assert json.loads(complete.result.value) == 4


def test_nondeterminism_still_detected_for_coroutines() -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> Any:
        return await ctx.call_activity("changed")

    complete = _completion(_execute(
        orchestrator, _start(), [helpers.new_task_scheduled_event(1, "original")]))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete.failureDetails.errorType.endswith("NonDeterminismError")


@pytest.mark.parametrize("after_activity", [False, True])
def test_non_durable_suspension_is_rejected(after_activity: bool) -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> None:
        if after_activity:
            await ctx.call_activity("work")
        await asyncio.Future()

    old = _start() + [helpers.new_task_scheduled_event(1, "work")] if after_activity else []
    new = [helpers.new_task_completed_event(1, "null")] if after_activity else _start()
    complete = _completion(_execute(orchestrator, old, new))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete.failureDetails.errorType == "builtins.RuntimeError"
    assert "blocked without durable work" in complete.failureDetails.errorMessage


def test_async_generator_is_rejected() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> AsyncGenerator[task.Task[Any], None]:
        yield ctx.call_activity("work", return_type=int)

    complete = _completion(_execute(orchestrator, [], _start()))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert "Async generator orchestrators are not supported" in complete.failureDetails.errorMessage
