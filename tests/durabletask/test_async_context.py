# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Cold coroutine and ownership semantics through the shared replay executor."""

import asyncio
import inspect
import json
import logging
from collections.abc import Coroutine, Generator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, get_type_hints

import pytest

from durabletask import entities, task
from durabletask.async_context import AsyncOrchestrationContext
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from tests.durabletask.test_coroutine_orchestrators import (
    NOW,
    _completion,  # pyright: ignore[reportPrivateUsage]
    _execute,  # pyright: ignore[reportPrivateUsage]
    _start,  # pyright: ignore[reportPrivateUsage]
)


def test_additive_public_api_has_no_new_abstract_requirements() -> None:
    assert "as_task_context" not in task.OrchestrationContext.__abstractmethods__
    assert "wait" not in task.Task.__abstractmethods__
    assert task.AsyncOrchestrationContext is AsyncOrchestrationContext


def test_public_async_context_annotations_resolve_at_runtime() -> None:
    assert get_type_hints(AsyncOrchestrationContext.call_activity)["return"] is Any
    assert get_type_hints(AsyncOrchestrationContext.call_sub_orchestrator)["return"] is Any
    assert get_type_hints(AsyncOrchestrationContext.as_task_context)["return"] is task.OrchestrationContext


@pytest.mark.parametrize("child_style", ["async", "generator", "name"])
@pytest.mark.parametrize("reconstruction", ["omitted", "none", "explicit"])
def test_sub_orchestration_result_requires_explicit_reconstruction_type(
    child_style: str, reconstruction: str,
) -> None:
    @dataclass
    class Result:
        value: int

    async def async_child(ctx: task.AsyncOrchestrationContext, value: int) -> Result:
        return Result(value)

    def generator_child(ctx: task.OrchestrationContext, value: int) -> Generator[task.Task[Any], Any, Result]:
        yield ctx.call_activity("work")
        return Result(value)

    child = {"async": async_child, "generator": generator_child, "name": "child"}[child_style]
    child_name = child if isinstance(child, str) else task.get_name(child)

    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> int:
        if reconstruction == "explicit":
            result = await ctx.call_sub_orchestrator(child, input=42, return_type=Result)
            assert isinstance(result, Result)
            return result.value
        raw: dict[str, int]
        if reconstruction == "none":
            raw = await ctx.call_sub_orchestrator(child, input=42, return_type=None)
        else:
            raw = await ctx.call_sub_orchestrator(child, input=42)
        assert isinstance(raw, dict)
        return raw["value"]

    start = _start()
    initial = _execute(orchestrator, [], start)
    assert len(initial.actions) == 1
    scheduled = initial.actions[0]
    assert scheduled.createSubOrchestration.name == child_name
    history = start + [helpers.new_sub_orchestration_created_event(
        scheduled.id, child_name, scheduled.createSubOrchestration.instanceId)]
    complete = _completion(_execute(orchestrator, history, [
        helpers.new_sub_orchestration_completed_event(scheduled.id, '{"value": 42}')]))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(complete.result.value) == 42


@pytest.mark.parametrize("cancel", [False, True])
def test_all_operations_are_cold_and_prestart_cancellation_schedules_nothing(cancel: bool) -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> Any:
        ops = ctx
        entity = entities.EntityInstanceId("counter", "one")
        invalid_input = object()
        operations: list[Coroutine[Any, Any, Any]] = [
            ops.call_activity("unused", input=invalid_input),
            ops.call_sub_orchestrator("unused", input=invalid_input),
            ops.call_entity(entity, "unused", invalid_input),
            ops.lock_entities([entity]),
            ops.create_timer(timedelta(days=1)),
            ops.wait_for_external_event("unused", timeout=timedelta(days=1)),
        ]
        assert all(inspect.iscoroutine(operation) for operation in operations)
        if cancel:
            pending = [asyncio.create_task(operation) for operation in operations]
            for waiter in pending:
                waiter.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        else:
            for operation in operations:
                operation.close()
        return await ctx.call_activity("actual", input=3)

    actions = _execute(orchestrator, [], _start()).actions
    assert len(actions) == 1
    assert actions[0].id == 1
    assert actions[0].scheduleTask.name == "actual"


@pytest.mark.parametrize("cancel", [False, True])
def test_cold_receive_does_not_consume_buffered_event(cancel: bool) -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> str:
        await ctx.wait_for_external_event("go")
        unused = ctx.wait_for_external_event("value")
        if cancel:
            pending = asyncio.create_task(unused)
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        else:
            unused.close()
        return await ctx.wait_for_external_event("value", data_type=str)

    result = _execute(orchestrator, _start(), [
        helpers.new_event_raised_event("value", '"kept"'),
        helpers.new_event_raised_event("go", None),
    ])
    assert json.loads(_completion(result).result.value) == "kept"


def test_create_task_and_task_group_accept_cold_operations() -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> list[int]:
        ops = ctx
        first = asyncio.create_task(ops.call_activity("first", return_type=int))
        async with asyncio.TaskGroup() as group:
            second = group.create_task(ops.call_activity("second", return_type=int))
        return [await first, second.result()]

    start = _start()
    initial = _execute(orchestrator, [], start)
    assert [(a.id, a.scheduleTask.name) for a in initial.actions] == [(1, "first"), (2, "second")]
    old = start + [
        helpers.new_task_scheduled_event(1, "first"),
        helpers.new_task_scheduled_event(2, "second"),
    ]
    result = _execute(orchestrator, old, [
        helpers.new_task_completed_event(2, "2"),
        helpers.new_task_completed_event(1, "1"),
    ])
    assert json.loads(_completion(result).result.value) == [1, 2]


def test_serialization_and_scheduling_follow_coroutine_execution_not_construction() -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> Any:
        ops = ctx
        payload = {"value": 1}
        first = ops.call_activity("first", input=payload)
        second = ops.call_activity("second", input=payload)
        payload["value"] = 2
        try:
            await second
            return await first
        finally:
            first.close()

    actions = _execute(orchestrator, [], _start()).actions
    assert len(actions) == 1
    assert actions[0].id == 1
    assert actions[0].scheduleTask.name == "second"
    assert json.loads(actions[0].scheduleTask.input.value) == {"value": 2}


def test_cold_relative_timer_deadline_is_evaluated_when_started() -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> None:
        timer = ctx.create_timer(timedelta(hours=1))
        try:
            await ctx.wait_for_external_event("go")
            await timer
        finally:
            timer.close()

    later = NOW + timedelta(hours=2)
    result = _execute(orchestrator, _start(), [
        helpers.new_orchestrator_started_event(later),
        helpers.new_event_raised_event("go", None),
    ])
    assert len(result.actions) == 1
    assert result.actions[0].createTimer.fireAt.ToDatetime() == later + timedelta(hours=1)


def test_borrowed_waiters_are_independent_reusable_and_do_not_cancel_operations() -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> list[str]:
        operation = ctx.as_task_context().call_activity("shared", return_type=str)
        cancelled = asyncio.create_task(operation.wait())
        remaining = asyncio.create_task(operation.wait())
        await asyncio.sleep(0)
        cancelled.cancel()
        await asyncio.gather(cancelled, return_exceptions=True)
        return [await remaining, await operation.wait(), await operation]

    old = _start() + [helpers.new_task_scheduled_event(1, "shared")]
    result = _execute(orchestrator, old, [helpers.new_task_completed_event(1, '"ok"')])
    assert json.loads(_completion(result).result.value) == ["ok", "ok", "ok"]


def test_borrowed_event_cancellation_preserves_shared_subscription() -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> int:
        event = ctx.as_task_context().wait_for_external_event("value", data_type=int)
        first = asyncio.create_task(event.wait())
        second = asyncio.create_task(event.wait())
        await asyncio.sleep(0)
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        return await second

    result = _execute(orchestrator, _start(), [helpers.new_event_raised_event("value", "42")])
    assert json.loads(_completion(result).result.value) == 42


def test_borrowed_cancelled_subscription_still_receives_before_owned_replacement() -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> list[int]:
        shared = ctx.as_task_context().wait_for_external_event("value", data_type=int)
        pending = asyncio.create_task(shared.wait())
        await asyncio.sleep(0)
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        next_value = await ctx.wait_for_external_event("value", data_type=int)
        return [await shared.wait(), next_value]

    result = _execute(orchestrator, _start(), [
        helpers.new_event_raised_event("value", "1"),
        helpers.new_event_raised_event("value", "2"),
    ])
    assert json.loads(_completion(result).result.value) == [1, 2]


def test_cold_coroutines_are_single_use_not_shared_operation_handles() -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> int:
        operation = ctx.call_activity("work", return_type=int)
        value = await operation
        with pytest.raises(RuntimeError, match="cannot reuse"):
            await operation
        return value

    old = _start() + [helpers.new_task_scheduled_event(1, "work")]
    result = _execute(orchestrator, old, [helpers.new_task_completed_event(1, "42")])
    assert json.loads(_completion(result).result.value) == 42


def test_owned_timeout_withdraws_subscription_before_replacement() -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> int:
        ops = ctx
        try:
            await ops.wait_for_external_event("value", timeout=timedelta(seconds=5))
        except TimeoutError:
            return await ops.wait_for_external_event("value", data_type=int)
        raise AssertionError("Expected timeout")

    deadline = NOW + timedelta(seconds=5)
    initial = _execute(orchestrator, [], _start())
    assert initial.actions[0].id == 1
    old = _start() + [helpers.new_timer_created_event(1, deadline)]
    expired = [helpers.new_orchestrator_started_event(deadline), helpers.new_timer_fired_event(1, deadline)]
    assert _execute(orchestrator, old, expired).actions == []
    result = _execute(orchestrator, old + expired, [helpers.new_event_raised_event("value", "42")])
    assert json.loads(_completion(result).result.value) == 42


@pytest.mark.parametrize("with_timeout", [False, True])
def test_owned_cancellation_withdraws_subscription_and_uncommitted_timer(with_timeout: bool) -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> int:
        ops = ctx
        pending = asyncio.create_task(ops.wait_for_external_event(
            "value", timeout=timedelta(seconds=5) if with_timeout else None))
        await asyncio.sleep(0)
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        return await ops.wait_for_external_event("value", data_type=int)

    assert _execute(orchestrator, [], _start()).actions == []
    result = _execute(orchestrator, _start(), [helpers.new_event_raised_event("value", "42")])
    assert json.loads(_completion(result).result.value) == 42


@pytest.mark.parametrize("buffered", [False, True])
@pytest.mark.parametrize("seconds", [-1, 0, 5])
def test_buffered_events_and_immediate_timeouts(buffered: bool, seconds: int) -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> Any:
        await ctx.wait_for_external_event("go")
        try:
            return await ctx.wait_for_external_event(
                "value", timeout=timedelta(seconds=seconds), data_type=int)
        except TimeoutError:
            return "timeout"

    new = ([helpers.new_event_raised_event("value", "42")] if buffered else []) + [
        helpers.new_event_raised_event("go", None)]
    result = _execute(orchestrator, _start(), new)
    if not buffered and seconds > 0:
        assert len(result.actions) == 1
        assert result.actions[0].HasField("createTimer")
    else:
        assert len(result.actions) == 1
        assert json.loads(_completion(result).result.value) == (42 if buffered else "timeout")


@pytest.mark.parametrize("event_first", [False, True])
def test_owned_receive_race_follows_history_order(event_first: bool) -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> str:
        ops = ctx
        try:
            value = await ops.wait_for_external_event("value", timeout=timedelta(seconds=5), data_type=int)
            return f"event:{value}"
        except TimeoutError:
            return f"timeout:{await ops.wait_for_external_event('value', data_type=int)}"

    deadline = NOW + timedelta(seconds=5)
    old = _start() + [helpers.new_timer_created_event(1, deadline)]
    arrival = helpers.new_event_raised_event("value", "42")
    fired = helpers.new_timer_fired_event(1, deadline)
    result = _execute(orchestrator, old, [arrival, fired] if event_first else [fired, arrival])
    assert json.loads(_completion(result).result.value) == ("event:42" if event_first else "timeout:42")


@pytest.mark.parametrize("seconds", [float("inf"), float("-inf"), float("nan")])
def test_nonfinite_timeout_rejected_before_consuming_event(seconds: float) -> None:
    class InfiniteTimeout(timedelta):
        def total_seconds(self) -> float:
            return seconds

    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> int:
        await ctx.wait_for_external_event("go")
        ops = ctx
        with pytest.raises(ValueError, match="finite"):
            await ops.wait_for_external_event("value", timeout=InfiniteTimeout())
        return await ops.wait_for_external_event("value", data_type=int)

    result = _execute(orchestrator, _start(), [
        helpers.new_event_raised_event("value", "42"),
        helpers.new_event_raised_event("go", None),
    ])
    assert json.loads(_completion(result).result.value) == 42


def test_owned_timer_cancellation_does_not_cancel_borrowed_timer() -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> None:
        borrowed = ctx.as_task_context().create_timer(timedelta(seconds=10))
        owned = asyncio.create_task(ctx.create_timer(timedelta(seconds=5)))
        waiter = asyncio.create_task(borrowed.wait())
        await asyncio.sleep(0)
        owned.cancel()
        waiter.cancel()
        await asyncio.gather(owned, waiter, return_exceptions=True)
        await borrowed

    result = _execute(orchestrator, [], _start())
    assert len(result.actions) == 1
    assert result.actions[0].id == 1
    assert result.actions[0].createTimer.fireAt.ToDatetime() == NOW + timedelta(seconds=10)


def test_owned_timer_completes_through_history() -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> str:
        await asyncio.create_task(ctx.create_timer(timedelta(seconds=5)))
        return "done"

    deadline = NOW + timedelta(seconds=5)
    old = _start() + [helpers.new_timer_created_event(1, deadline)]
    result = _execute(orchestrator, old, [helpers.new_timer_fired_event(1, deadline)])
    assert json.loads(_completion(result).result.value) == "done"


def test_owned_event_completion_cancels_only_its_uncommitted_timeout_timer() -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> Any:
        await ctx.wait_for_external_event("value", timeout=timedelta(seconds=5))
        return await ctx.call_activity("next")

    result = _execute(orchestrator, _start(), [helpers.new_event_raised_event("value", "42")])
    assert len(result.actions) == 1
    assert result.actions[0].id == 2
    assert result.actions[0].scheduleTask.name == "next"


def test_dispatch_disposal_of_owned_resources_preserves_response_and_has_no_cleanup_errors(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> None:
        ops = ctx
        await asyncio.gather(
            ops.create_timer(timedelta(seconds=10)),
            ops.wait_for_external_event("value", timeout=timedelta(seconds=5)),
            ops.wait_for_external_event("other"),
        )

    with caplog.at_level(logging.WARNING):
        result = _execute(orchestrator, [], _start())
    assert [(a.id, a.createTimer.fireAt.ToDatetime()) for a in result.actions] == [
        (1, NOW + timedelta(seconds=10)), (2, NOW + timedelta(seconds=5))]
    assert not caplog.records


def test_sequential_cold_operations_preserve_generator_actions() -> None:
    async def coroutine(ctx: task.AsyncOrchestrationContext, _: Any) -> int:
        ops = ctx
        first = await ops.call_activity("increment", input=3, return_type=int)
        return await ops.call_activity("increment", input=first, return_type=int)

    def generator(ctx: task.OrchestrationContext, _: Any) -> Generator[task.Task[Any], Any, int]:
        first = yield ctx.call_activity("increment", input=3, return_type=int)
        return (yield ctx.call_activity("increment", input=first, return_type=int))

    batches = [
        _start(),
        [helpers.new_task_scheduled_event(1, "increment"), helpers.new_task_completed_event(1, "4")],
        [helpers.new_task_scheduled_event(2, "increment"), helpers.new_task_completed_event(2, "5")],
    ]
    old: list[pb.HistoryEvent] = []
    result = None
    for new in batches:
        result = _execute(coroutine, old, new)
        assert result.actions == _execute(generator, old, new).actions
        old += new
    assert result is not None
    assert json.loads(_completion(result).result.value) == 5


def test_entity_lock_and_typed_call_use_existing_replay_protocol() -> None:
    entity = entities.EntityInstanceId("counter", "one")

    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> int:
        ops = ctx
        with await ops.lock_entities([entity]):
            return await ops.call_entity(entity, "get", return_type=int)

    old = _start()
    lock = _execute(orchestrator, [], old).actions[0].sendEntityMessage.entityLockRequested
    new = [
        helpers.new_event_sent_event(1, str(entity), json.dumps({"id": lock.criticalSectionId})),
        helpers.new_event_raised_event(lock.criticalSectionId, None),
    ]
    actions = _execute(orchestrator, old, new).actions
    call = actions[0].sendEntityMessage.entityOperationCalled
    old += new + [helpers.new_event_sent_event(2, str(entity), json.dumps({"id": call.requestId}))]
    complete = _completion(_execute(
        orchestrator, old, [helpers.new_event_raised_event(call.requestId, json.dumps({"result": "42"}))]))
    assert json.loads(complete.result.value) == 42


def test_timed_out_shielded_lock_is_released_after_acquisition() -> None:
    entity = entities.EntityInstanceId("counter", "one")

    async def orchestrator(ctx: task.AsyncOrchestrationContext, _: Any) -> str:
        acquire = asyncio.create_task(ctx.lock_entities([entity]))
        try:
            lock = await asyncio.wait_for(asyncio.shield(acquire), timeout=2)
        except TimeoutError:
            lock = await acquire
            with lock:
                pass
            return "timed out"
        with lock:
            return "acquired"

    start = _start()
    initial = _execute(orchestrator, [], start)
    assert len(initial.actions) == 2
    lock_action = next(action for action in initial.actions if action.HasField("sendEntityMessage"))
    timer_action = next(action for action in initial.actions if action.HasField("createTimer"))
    section = lock_action.sendEntityMessage.entityLockRequested.criticalSectionId
    deadline = timer_action.createTimer.fireAt.ToDatetime()
    assert deadline == NOW + timedelta(seconds=2)
    history = start + [
        helpers.new_event_sent_event(lock_action.id, str(entity), json.dumps({"id": section})),
        helpers.new_timer_created_event(timer_action.id, deadline),
    ]
    timed_out = [
        helpers.new_orchestrator_started_event(deadline),
        helpers.new_timer_fired_event(timer_action.id, deadline),
    ]
    assert not _execute(orchestrator, history, timed_out).actions
    history += timed_out
    granted = [helpers.new_event_raised_event(section, None)]
    result = _execute(orchestrator, history, granted)
    assert result.actions == _execute(orchestrator, history, granted).actions
    assert result.actions[0].sendEntityMessage.entityUnlockSent.targetInstanceId.value == str(entity)
    assert json.loads(_completion(result).result.value) == "timed out"


@pytest.mark.parametrize("sub_orchestration", [False, True])
def test_cold_retry_and_call_options_preserve_eager_actions(sub_orchestration: bool) -> None:
    policy = task.RetryPolicy(first_retry_interval=timedelta(seconds=2), max_number_of_attempts=3)

    async def cold(ctx: task.AsyncOrchestrationContext, _: Any) -> int:
        if sub_orchestration:
            return await ctx.call_sub_orchestrator(
                "child", input=3, instance_id="child-id", retry_policy=policy,
                version="v1", return_type=int)
        return await ctx.call_activity(
            "work", input=3, retry_policy=policy, tags={"tag": "value"}, return_type=int)

    def eager(ctx: task.OrchestrationContext, _: Any) -> Generator[task.Task[Any], Any, int]:
        if sub_orchestration:
            return (yield ctx.call_sub_orchestrator(
                "child", input=3, instance_id="child-id", retry_policy=policy,
                version="v1", return_type=int))
        return (yield ctx.call_activity(
            "work", input=3, retry_policy=policy, tags={"tag": "value"}, return_type=int))

    start = _start()
    assert _execute(cold, [], start).actions == _execute(eager, [], start).actions
    if sub_orchestration:
        scheduled = helpers.new_sub_orchestration_created_event(1, "child", "child-id")
        failed = helpers.new_sub_orchestration_failed_event(1, ValueError("retry me"))
        completed = helpers.new_sub_orchestration_completed_event(1, "42")
    else:
        scheduled = helpers.new_task_scheduled_event(1, "work")
        failed = helpers.new_task_failed_event(1, ValueError("retry me"))
        completed = helpers.new_task_completed_event(1, "42")
    old = start + [scheduled]
    retry = _execute(cold, old, [failed])
    assert retry.actions == _execute(eager, old, [failed]).actions
    assert len(retry.actions) == 1
    assert retry.actions[0].id == 2
    deadline = retry.actions[0].createTimer.fireAt.ToDatetime()
    old += [failed, helpers.new_timer_created_event(2, deadline)]
    fired = [helpers.new_orchestrator_started_event(deadline), helpers.new_timer_fired_event(2, deadline)]
    restarted = _execute(cold, old, fired)
    assert restarted.actions == _execute(eager, old, fired).actions
    assert restarted.actions[0].id == 1
    old += fired + [scheduled]
    complete = _completion(_execute(cold, old, [completed]))
    assert json.loads(complete.result.value) == 42
