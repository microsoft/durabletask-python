# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Deterministic asyncio and AnyIO behavior through the real history executor."""

import asyncio
import gc
import json
import sys
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any
from weakref import ref

import anyio
import pytest

from durabletask import task
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from tests.durabletask.test_coroutine_orchestrators import _completion, _execute, _start, NOW


def test_gather_resumes_each_branch_without_waiting_for_batch() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> list[Any]:
        async def branch(name: str) -> Any:
            value = await ctx.call_activity(name)
            await asyncio.sleep(0)
            return await ctx.call_activity(name + "-next", input=value)

        return await asyncio.gather(branch("a"), branch("b"))

    start = _start()
    actions = _execute(orchestrator, [], start).actions
    assert [a.scheduleTask.name for a in actions] == ["a", "b"]
    old = start + [helpers.new_task_scheduled_event(i, name) for i, name in [(1, "a"), (2, "b")]]
    second = [helpers.new_task_completed_event(2, '"b"')]
    actions = _execute(orchestrator, old, second).actions
    assert [(a.id, a.scheduleTask.name) for a in actions] == [(3, "b-next")]
    third = [helpers.new_task_scheduled_event(3, "b-next"),
             helpers.new_task_completed_event(1, '"a"'),
             helpers.new_task_scheduled_event(4, "a-next"),
             helpers.new_task_completed_event(4, '"a-done"'),
             helpers.new_task_completed_event(3, '"b-done"')]
    assert json.loads(_completion(_execute(orchestrator, old + second, third)).result.value) == [
        "a-done", "b-done"]


@pytest.mark.parametrize("return_exceptions", [False, True])
def test_gather_failure_does_not_cancel_sibling(return_exceptions: bool) -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> Any:
        async def sibling() -> str:
            await ctx.call_activity("slow")
            return await ctx.call_activity("continued", return_type=str)

        pending = asyncio.create_task(sibling())
        try:
            values = await asyncio.gather(ctx.call_activity("fail"), pending, return_exceptions=return_exceptions)
            return [isinstance(values[0], task.TaskFailedError), values[1]]
        except task.TaskFailedError:
            assert not pending.cancelled()
            return await pending

    old = _start() + [helpers.new_task_scheduled_event(1, "fail"),
                      helpers.new_task_scheduled_event(2, "slow")]
    failed = [helpers.new_task_failed_event(1, ValueError("boom"))]
    assert _execute(orchestrator, old, failed).actions == []
    completed = [helpers.new_task_completed_event(2, "null")]
    assert _execute(orchestrator, old + failed, completed).actions[0].scheduleTask.name == "continued"
    result = _completion(_execute(orchestrator, old + failed + completed, [
        helpers.new_task_scheduled_event(3, "continued"), helpers.new_task_completed_event(3, '"ok"')]))
    assert json.loads(result.result.value) == ([True, "ok"] if return_exceptions else "ok")


@pytest.mark.parametrize("native", [False, True])
def test_task_group_failure_cancels_waiters_not_durable_work(native: bool) -> None:
    if native and sys.version_info < (3, 11):
        pytest.skip("asyncio.TaskGroup requires Python 3.11")
    cleaned: list[str] = []

    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> None:
        async def branch(name: str) -> None:
            try:
                await ctx.call_activity(name)
            finally:
                with anyio.CancelScope(shield=True):
                    await asyncio.sleep(0)
                    cleaned.append(name)

        if native:
            async with asyncio.TaskGroup() as group:
                group.create_task(branch("fail"))
                group.create_task(branch("slow"))
        else:
            async with anyio.create_task_group() as group:
                group.start_soon(branch, "fail")
                group.start_soon(branch, "slow")

    old = _start() + [helpers.new_task_scheduled_event(1, "fail"),
                      helpers.new_task_scheduled_event(2, "slow")]
    result = _completion(_execute(orchestrator, old, [helpers.new_task_failed_event(1, ValueError("boom"))]))
    assert result.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert cleaned == ["fail", "slow"]


@pytest.mark.parametrize("kind", ["sleep", "wait_for", "anyio", "timeout"])
def test_durable_timer_and_timeout(kind: str) -> None:
    if kind == "timeout" and sys.version_info < (3, 11):
        pytest.skip("asyncio.timeout requires Python 3.11")

    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> str:
        try:
            if kind == "sleep":
                await asyncio.sleep(2)
            elif kind == "wait_for":
                await asyncio.wait_for(ctx.wait_for_external_event("never"), timeout=2)
            elif kind == "timeout":
                async with asyncio.timeout(2):
                    await ctx.wait_for_external_event("never")
            else:
                with anyio.fail_after(2):
                    await ctx.wait_for_external_event("never")
        except TimeoutError:
            return "timed out"
        return "slept"

    actions = _execute(orchestrator, [], _start()).actions
    assert len(actions) == 1
    deadline = actions[0].createTimer.fireAt.ToDatetime()
    assert deadline == NOW + timedelta(seconds=2)
    old = _start() + [helpers.new_timer_created_event(1, deadline)]
    new = [helpers.new_orchestrator_started_event(deadline), helpers.new_timer_fired_event(1, deadline)]
    assert json.loads(_completion(_execute(orchestrator, old, new)).result.value) == (
        "slept" if kind == "sleep" else "timed out")


def test_sleep_zero_fifo_without_timer() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> list[int]:
        observed: list[int] = []

        async def branch(index: int) -> None:
            observed.append(index)
            await asyncio.sleep(0)
            observed.append(index + 2)

        await asyncio.gather(branch(0), branch(1))
        return observed

    result = _execute(orchestrator, [], _start())
    assert len(result.actions) == 1
    assert json.loads(_completion(result).result.value) == [0, 1, 2, 3]


@pytest.mark.parametrize("under_loop", [False, True])
def test_dispatch_cleanup_unwinds_scopes_contexts_asyncgens(under_loop: bool) -> None:
    cleaned: list[str] = []
    references: list[Any] = []

    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> None:
        references.extend([ref(ctx), ref(asyncio.get_running_loop()), ref(asyncio.current_task())])

        async def resource():
            try:
                yield 1
            finally:
                await asyncio.sleep(0)
                cleaned.append("asyncgen")

        opened = resource()
        await anext(opened)
        try:
            async with anyio.create_task_group():
                await ctx.call_activity("wait")
        finally:
            await asyncio.sleep(0)
            cleaned.append("root")

    def dispatch() -> None:
        result = _execute(orchestrator, [], _start())
        assert [a.scheduleTask.name for a in result.actions] == ["wait"]
        assert cleaned == ["root", "asyncgen"]

    async def outer() -> None:
        before = asyncio.current_task()
        dispatch()
        assert asyncio.current_task() is before
        await asyncio.sleep(0)

    if under_loop:
        asyncio.run(outer())
    else:
        dispatch()
    gc.collect()
    assert all(reference() is None for reference in references)


def test_shared_operation_survives_local_waiter_cancellation() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> Any:
        durable = ctx.call_activity("shared")

        async def wait() -> Any:
            return await durable

        first = asyncio.create_task(wait())
        second = asyncio.create_task(wait())
        await asyncio.sleep(0)
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        return await second

    old = _start() + [helpers.new_task_scheduled_event(1, "shared")]
    assert json.loads(_completion(_execute(orchestrator, old, [
        helpers.new_task_completed_event(1, '"ok"')])).result.value) == "ok"


def test_local_durable_cancellation_wakes_waiter_without_history() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> str:
        timer = ctx.create_timer(timedelta(days=1))

        async def waiter() -> None:
            await timer

        pending = asyncio.create_task(waiter())
        await asyncio.sleep(0)
        timer.cancel()
        try:
            await pending
        except task.TaskCancelledError:
            return "cancelled"
        return "unexpected"

    result = _execute(orchestrator, [], _start())
    assert len(result.actions) == 1
    assert json.loads(_completion(result).result.value) == "cancelled"


@pytest.mark.parametrize("terminal", ["suspend", "terminate", "continue", "history-failure"])
def test_async_teardown_preserves_response_and_cannot_schedule_work(
        terminal: str, caplog: pytest.LogCaptureFixture) -> None:
    cleaned: list[str] = []

    @asynccontextmanager
    async def resource():
        try:
            yield
        finally:
            await asyncio.sleep(0)
            cleaned.append("resource")

    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> None:
        async with resource():
            try:
                ctx.set_custom_status("waiting")
                if terminal == "continue":
                    ctx.continue_as_new("next")
                await ctx.call_activity("wait")
            finally:
                try:
                    await asyncio.sleep(0)
                    cleaned.append("inner")
                    ctx.call_activity("forbidden-cleanup")
                finally:
                    await asyncio.sleep(0)
                    cleaned.append("outer")

    old = _start()
    if terminal == "terminate":
        new = [pb.HistoryEvent(executionTerminated=pb.ExecutionTerminatedEvent())]
    elif terminal == "history-failure":
        new = [helpers.new_task_scheduled_event(1, "wrong")]
    else:
        old, new = [], old
    result = _execute(orchestrator, old, new)
    assert cleaned == ["inner", "outer", "resource"]
    assert result.encoded_custom_status == '"waiting"'
    assert all(action.scheduleTask.name != "forbidden-cleanup" for action in result.actions)
    if terminal == "terminate":
        assert _completion(result).orchestrationStatus == pb.ORCHESTRATION_STATUS_TERMINATED
    elif terminal == "history-failure":
        assert _completion(result).failureDetails.errorType.endswith("NonDeterminismError")
    elif terminal == "continue":
        assert _completion(result).orchestrationStatus == pb.ORCHESTRATION_STATUS_CONTINUED_AS_NEW
    else:
        assert [action.scheduleTask.name for action in result.actions] == ["wait"]
    assert "Dispatch cleanup cannot schedule durable work" in caplog.text


def test_unhandled_local_root_cancellation_fails_not_abandons() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> None:
        raise asyncio.CancelledError

    complete = _completion(_execute(orchestrator, [], _start()))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete.failureDetails.errorType.endswith("TaskCancelledError")


@pytest.mark.parametrize("delay", [0.0000003, 0.000001, 0.0000013, 2.25])
def test_timer_rounding_never_precedes_loop_deadline(delay: float) -> None:
    observed: list[float] = []

    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> None:
        loop = asyncio.get_running_loop()
        observed.append(loop.time() + delay)
        await asyncio.sleep(delay)

    actions = _execute(orchestrator, [], _start()).actions
    assert len(actions) == 1
    deadline = actions[0].createTimer.fireAt.ToDatetime()
    from datetime import datetime
    assert (deadline - datetime(1970, 1, 1)).total_seconds() >= observed[0]


def test_async_sleep_uses_shared_long_timer_chunking() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> str:
        await asyncio.sleep(7 * 86400)
        return "done"

    old = _start()
    for timer_id, days in [(1, 3), (2, 6), (3, 7)]:
        result = _execute(orchestrator, [], old)
        assert len(result.actions) == 1
        action = result.actions[0]
        assert action.id == timer_id
        deadline = NOW + timedelta(days=days)
        assert action.createTimer.fireAt.ToDatetime() == deadline
        old += [helpers.new_timer_created_event(timer_id, deadline),
                helpers.new_orchestrator_started_event(deadline),
                helpers.new_timer_fired_event(timer_id, deadline)]
    assert json.loads(_completion(_execute(orchestrator, old[:-1], old[-1:])).result.value) == "done"


@pytest.mark.parametrize("api", ["thread", "io", "foreign-loop"])
def test_unsupported_work_fails_actionably(api: str) -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> None:
        if api == "thread":
            await asyncio.to_thread(lambda: None)
        elif api == "io":
            await asyncio.open_connection("localhost", 1)
        else:
            foreign = asyncio.new_event_loop()
            try:
                await foreign.create_future()
            finally:
                foreign.close()

    complete = _completion(_execute(orchestrator, [], _start()))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert any(text in complete.failureDetails.errorMessage for text in (
        "use an activity", "ordinary activity", "different loop"))
