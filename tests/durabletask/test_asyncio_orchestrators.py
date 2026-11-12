# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Deterministic asyncio behavior through the real history executor."""

import asyncio
import gc
import json
import os
import subprocess
import sys
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_CEILING, Decimal, localcontext
from functools import partial
from types import SimpleNamespace
from typing import Any, cast
from weakref import ReferenceType, ref

import pytest

from durabletask import task
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.internal.asyncio_loop import _UNSUPPORTED_LOOP_APIS, DurableEventLoop  # pyright: ignore[reportPrivateUsage]
from tests.durabletask.test_coroutine_orchestrators import (
    NOW,
    _completion,  # pyright: ignore[reportPrivateUsage]
    _execute,  # pyright: ignore[reportPrivateUsage]
    _start,  # pyright: ignore[reportPrivateUsage]
)


@pytest.mark.parametrize("fail_branch", [False, True])
def test_task_group_branches_inherit_context_and_replay_stable_actions(fail_branch: bool) -> None:
    context: ContextVar[task.OrchestrationContext] = ContextVar("orchestration")

    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> Any:
        token = context.set(ctx)
        try:
            results: dict[str, Any] = {}

            async def branch(name: str) -> None:
                current = context.get()
                assert current is ctx
                results[name] = await current.call_activity(name, input=current.instance_id)

            async with asyncio.TaskGroup() as group:
                group.create_task(branch("first"))
                group.create_task(branch("second"))
            return await context.get().call_activity("finish", input=[results["first"], results["second"]])
        finally:
            context.reset(token)

    def replay(old: list[pb.HistoryEvent], new: list[pb.HistoryEvent]):
        first = _execute(orchestrator, old, new)
        assert _execute(orchestrator, old, new).actions == first.actions
        with pytest.raises(LookupError):
            context.get()
        return first

    start = _start()
    initial = replay([], start)
    assert [action.scheduleTask.name for action in initial.actions] == ["first", "second"]
    assert all(json.loads(action.scheduleTask.input.value) == "coroutine-test" for action in initial.actions)
    old = start + [helpers.new_task_scheduled_event(1, "first"),
                   helpers.new_task_scheduled_event(2, "second")]
    second_done = [helpers.new_task_completed_event(2, '"second result"')]
    assert replay(old, second_done).actions == []
    first_done = [helpers.new_task_failed_event(1, ValueError("recorded branch failure"))] if fail_branch else [
        helpers.new_task_completed_event(1, '"first result"')]
    result = replay(old + second_done, first_done)
    if fail_branch:
        complete = _completion(result)
        assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
        assert "recorded branch failure" in str(complete.failureDetails)
    else:
        assert len(result.actions) == 1
        assert result.actions[0].scheduleTask.name == "finish"
        assert json.loads(result.actions[0].scheduleTask.input.value) == ["first result", "second result"]
        complete = _completion(replay(old + second_done + first_done, [
            helpers.new_task_scheduled_event(3, "finish"),
            helpers.new_task_completed_event(3, '"done"')]))
        assert json.loads(complete.result.value) == "done"


def test_gather_resumes_each_branch_without_waiting_for_batch() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> list[Any]:
        async def branch(name: str) -> Any:
            value: Any = await ctx.call_activity(name)
            await asyncio.sleep(0)
            return await ctx.call_activity(name + "-next", input=value)

        return list(await asyncio.gather(branch("a"), branch("b")))

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

        failure = asyncio.create_task(ctx.call_activity("fail", return_type=str).wait())
        pending = asyncio.create_task(sibling())
        try:
            values = await asyncio.gather(
                failure, pending, return_exceptions=return_exceptions)
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


def test_task_group_failure_cancels_waiters_not_durable_work() -> None:
    cleaned: list[str] = []

    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> None:
        async def branch(name: str) -> None:
            try:
                await ctx.call_activity(name)
            finally:
                await asyncio.sleep(0)
                cleaned.append(name)

        async with asyncio.TaskGroup() as group:
            group.create_task(branch("fail"))
            group.create_task(branch("slow"))

    old = _start() + [helpers.new_task_scheduled_event(1, "fail"),
                      helpers.new_task_scheduled_event(2, "slow")]
    result = _completion(_execute(orchestrator, old, [helpers.new_task_failed_event(1, ValueError("boom"))]))
    assert result.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert cleaned == ["fail", "slow"]


@pytest.mark.parametrize("kind", ["sleep", "wait_for", "timeout"])
def test_durable_timer_and_timeout(kind: str) -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> str:
        try:
            if kind == "sleep":
                await asyncio.sleep(2)
            elif kind == "wait_for":
                await asyncio.wait_for(ctx.wait_for_external_event("never"), timeout=2)
            else:
                async with asyncio.timeout(2):
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


def test_discarded_gather_batches_release_decoded_results_during_replay() -> None:
    references: list[ReferenceType[Any]] = []

    @dataclass
    class Payload:
        value: int

        def __post_init__(self) -> None:
            references.append(ref(self))

    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> str:
        for batch in range(8):
            await asyncio.gather(*(ctx.call_activity("work", return_type=Payload) for _ in range(4)))
            await asyncio.sleep(0)
            gc.collect()
            assert len(references) == (batch + 1) * 4
            # The history handler still holds the most recent decoded result
            # while resume() drains this batch's callbacks.
            assert sum(reference() is not None for reference in references) <= 1
        return "done"

    history = _start()
    for batch in range(8):
        ids = range(batch * 4 + 1, batch * 4 + 5)
        history += [helpers.new_task_scheduled_event(index, "work") for index in ids]
        history += [helpers.new_task_completed_event(index, json.dumps({"value": index})) for index in ids]
    complete = _completion(_execute(orchestrator, history[:-1], history[-1:]))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED, complete.failureDetails
    assert json.loads(complete.result.value) == "done"
    gc.collect()
    assert all(reference() is None for reference in references)


@pytest.mark.parametrize("observe", [
    "await", "gather", "gather-exceptions", "result", "exception", "task-group",
])
def test_observed_failures_release_traceback_payloads_during_replay(observe: str) -> None:
    references: list[ReferenceType[Any]] = []

    @dataclass
    class Payload:
        value: int

        def __post_init__(self) -> None:
            references.append(ref(self))

    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> str:
        async def branch() -> None:
            payload = await ctx.call_activity("work", return_type=Payload)
            raise ValueError(f"Failed with payload {payload.value}")

        for batch in range(8):
            if observe == "task-group":
                group = asyncio.TaskGroup()
                try:
                    async with group:
                        group.create_task(branch())
                except* ValueError:
                    pass
                del group
            else:
                pending = asyncio.create_task(branch())
                await asyncio.sleep(0)
                # Observe only after the failed task's initial retirement callback.
                await ctx.call_activity("gate")
                if observe == "gather-exceptions":
                    await asyncio.gather(pending, return_exceptions=True)
                elif observe == "exception":
                    assert isinstance(pending.exception(), ValueError)
                else:
                    try:
                        if observe == "await":
                            await pending
                        elif observe == "gather":
                            await asyncio.gather(pending)
                        else:
                            pending.result()
                    except ValueError:
                        pass
                del pending
            await asyncio.sleep(0)
            gc.collect()
            assert len(references) == batch + 1
            assert sum(reference() is not None for reference in references) <= 1
        return "done"

    history = _start()
    sequence = 0
    for batch in range(8):
        sequence += 1
        work_id = sequence
        history.append(helpers.new_task_scheduled_event(work_id, "work"))
        if observe != "task-group":
            sequence += 1
            history.append(helpers.new_task_scheduled_event(sequence, "gate"))
        history.append(helpers.new_task_completed_event(work_id, json.dumps({"value": batch})))
        if observe != "task-group":
            history.append(helpers.new_task_completed_event(sequence, "null"))
    complete = _completion(_execute(orchestrator, history[:-1], history[-1:]))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED, complete.failureDetails
    assert json.loads(complete.result.value) == "done"
    gc.collect()
    assert all(reference() is None for reference in references)


def test_retired_tasks_do_not_reuse_cancellation_order() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> str:
        async def completed() -> None:
            pass

        async def branch(index: int) -> None:
            try:
                await ctx.wait_for_external_event(str(index))
            finally:
                await ctx.call_activity("compensate", input=index)

        asyncio.create_task(completed())
        first = asyncio.create_task(branch(0))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        second = asyncio.create_task(branch(1))
        await asyncio.sleep(0)
        second.cancel()
        first.cancel()
        await asyncio.gather(first, second, return_exceptions=True)
        return "done"

    initial = _execute(orchestrator, [], _start())
    assert [json.loads(action.scheduleTask.input.value) for action in initial.actions] == [0, 1]
    assert _execute(orchestrator, [], _start()).actions == initial.actions
    old = _start() + [helpers.new_task_scheduled_event(index, "compensate") for index in (1, 2)]
    new = [helpers.new_task_completed_event(index, "null") for index in (1, 2)]
    assert json.loads(_completion(_execute(orchestrator, old, new)).result.value) == "done"


def test_shared_operation_survives_local_waiter_cancellation() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> Any:
        durable = ctx.call_activity("shared", return_type=str)

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
    assert (deadline - datetime(1970, 1, 1)).total_seconds() >= observed[0]


@pytest.mark.parametrize("offset", [0.000003, 0.0000003, 0.000001, 0.0000013, 1.0, 2.25])
def test_call_at_deadline_is_exact_microsecond_ceiling(offset: float) -> None:
    requested: list[float] = []

    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> None:
        loop = asyncio.get_running_loop()
        when = loop.time() + offset
        requested.append(when)
        fired = loop.create_future()
        loop.call_at(when, fired.set_result, None)
        await fired

    actions = _execute(orchestrator, [], _start()).actions
    assert len(actions) == 1
    fire_at = actions[0].createTimer.fireAt
    assert fire_at.nanos % 1000 == 0
    emitted_us = fire_at.seconds * 1_000_000 + fire_at.nanos // 1000
    # Decimal(float) is exact, so this oracle shares no float arithmetic with the loop.
    with localcontext() as exact:
        exact.prec = 400
        requested_seconds = Decimal(requested[0])
        oracle_us = int((requested_seconds * 1_000_000).to_integral_value(rounding=ROUND_CEILING))
        assert emitted_us == oracle_us
        assert Decimal(emitted_us) / 1_000_000 >= requested_seconds


def test_call_at_known_float_deadline_rounds_up_to_next_microsecond() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> None:
        loop = asyncio.get_running_loop()
        fired = loop.create_future()
        loop.call_at(loop.time() + 0.000003, fired.set_result, None)
        await fired

    actions = _execute(orchestrator, [], _start()).actions
    assert actions[0].createTimer.fireAt.ToDatetime() == NOW + timedelta(microseconds=4)


@pytest.mark.parametrize("offset", [0.0, -1.0])
def test_call_at_due_deadline_runs_without_durable_timer(offset: float) -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> str:
        loop = asyncio.get_running_loop()
        fired = loop.create_future()
        loop.call_at(loop.time() + offset, fired.set_result, "fired")
        return await fired

    result = _execute(orchestrator, [], _start())
    assert json.loads(_completion(result).result.value) == "fired"


@pytest.mark.parametrize("when", [1e20, float(sys.float_info.max)])
def test_call_at_out_of_range_deadline_fails(when: float) -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> None:
        asyncio.get_running_loop().call_at(when, lambda: None)

    complete = _completion(_execute(orchestrator, [], _start()))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete.failureDetails.errorType.endswith("OverflowError")


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


@pytest.mark.parametrize("name", sorted(_UNSUPPORTED_LOOP_APIS))
def test_every_unsupported_loop_api_fails_actionably(name: str) -> None:
    ctx = cast(task.OrchestrationContext, SimpleNamespace(current_utc_datetime=NOW))
    loop = DurableEventLoop(ctx, is_complete=lambda: False)
    with pytest.raises(RuntimeError, match=rf"loop\.{name}\(\) is not supported") as error:
        getattr(loop, name)()
    assert str(error.value).endswith(_UNSUPPORTED_LOOP_APIS[name])


def test_untracked_task_finalized_after_dispatch_close_is_quiet(monkeypatch: pytest.MonkeyPatch) -> None:
    unraisable: list[Any] = []
    monkeypatch.setattr(sys, "unraisablehook", unraisable.append)
    untracked: list[asyncio.Task[Any]] = []

    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> None:
        untracked.append(asyncio.Task(ctx.call_activity("background").wait()))
        await ctx.call_activity("work")

    _execute(orchestrator, [], _start())
    untracked.clear()
    gc.collect()
    assert not [hook for hook in unraisable if isinstance(hook.exc_value, KeyError)]


def test_task_group_cancellation_durable_cleanup_has_stable_action_order() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> None:
        async def branch(index: int) -> None:
            try:
                await ctx.call_activity("work", input=index)
            finally:
                # This is real task-group cancellation during replay, not
                # dispatch teardown: durable compensation is still allowed.
                await ctx.call_activity("compensate", input=index)

        async with asyncio.TaskGroup() as group:
            for index in range(4):
                group.create_task(branch(index))

    old = _start() + [helpers.new_task_scheduled_event(index, "work") for index in range(1, 5)]
    failure = [helpers.new_task_failed_event(1, ValueError("boom"))]
    # The failing branch first enters its durable finally, so deliver that
    # completion before task-group sibling cancellation starts.
    first = _execute(orchestrator, old, failure)
    assert [(a.id, a.scheduleTask.name, json.loads(a.scheduleTask.input.value)) for a in first.actions] == [
        (5, "compensate", 0)]
    old += failure + [helpers.new_task_scheduled_event(5, "compensate"),
                      helpers.new_task_completed_event(5, "null")]
    expected = [helpers.new_schedule_task_action(index + 6, "compensate", json.dumps(index + 1), None)
                for index in range(3)]
    for _ in range(10):
        assert _execute(orchestrator, [], old).actions == expected


async def _shared_future_cancellation_orchestrator(
    ctx: task.OrchestrationContext, _: Any, *, direct_future: bool, gather_wait: bool,
) -> str:
    loop = asyncio.get_running_loop()
    futures: list[asyncio.Future[None]] = [
        asyncio.Future[None]() if direct_future else loop.create_future() for _ in range(2)]

    def on_cancelled(index: int, future: asyncio.Future[None]) -> None:
        assert future.cancelled()
        ctx.call_activity(f"cancelled-{index}")

    for index, future in enumerate(futures):
        future.add_done_callback(partial(on_cancelled, index))

    async def wait(future: asyncio.Future[None]) -> None:
        if gather_wait:
            await asyncio.gather(future)
        else:
            await future

    async def fail() -> None:
        await asyncio.sleep(0)
        raise ValueError("cancel siblings")

    try:
        async with asyncio.TaskGroup() as group:
            for future in futures * 2:
                group.create_task(wait(future))
            group.create_task(fail())
    except* ValueError:
        pass
    return await ctx.call_activity("join", return_type=str)


@pytest.mark.parametrize("direct_future", [False, True])
@pytest.mark.parametrize("gather_wait", [False, True])
def test_shared_future_cancellation_callbacks_replay_on_fresh_executors(
    direct_future: bool, gather_wait: bool,
) -> None:
    orchestrator = partial(
        _shared_future_cancellation_orchestrator, direct_future=direct_future, gather_wait=gather_wait)
    start = _start()
    initial = _execute(orchestrator, [], start)
    assert sorted(a.scheduleTask.name for a in initial.actions[:-1]) == ["cancelled-0", "cancelled-1"]
    assert initial.actions[-1].scheduleTask.name == "join"
    recorded = start + [
        helpers.new_task_scheduled_event(action.id, action.scheduleTask.name)
        for action in initial.actions
    ]
    # Activity workers may complete in any order. Each dispatch reconstructs
    # all local tasks and futures from history, as on a different worker.
    completed = [
        helpers.new_task_completed_event(initial.actions[1].id, "null"),
        helpers.new_task_completed_event(initial.actions[0].id, "null"),
    ]
    for _ in range(30):
        assert _execute(orchestrator, [], start).actions == initial.actions
        assert _execute(orchestrator, recorded, completed).actions == []
        complete = _completion(_execute(
            orchestrator, recorded + completed,
            [helpers.new_task_completed_event(initial.actions[-1].id, '"joined"')]))
        assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED, complete.failureDetails
        assert json.loads(complete.result.value) == "joined"


def test_shared_future_cancellation_history_replays_in_other_worker_processes() -> None:
    orchestrator = partial(
        _shared_future_cancellation_orchestrator, direct_future=True, gather_wait=False)
    start = _start()
    initial = _execute(orchestrator, [], start)
    history = start + [
        helpers.new_task_scheduled_event(action.id, action.scheduleTask.name)
        for action in initial.actions
    ]
    history += [
        helpers.new_task_completed_event(initial.actions[1].id, "null"),
        helpers.new_task_completed_event(initial.actions[0].id, "null"),
    ]
    payload = json.dumps({
        "initial": [action.SerializeToString().hex() for action in initial.actions],
        "history": [event.SerializeToString().hex() for event in history],
        "join_id": initial.actions[-1].id,
    })
    code = """
import json
import sys
from functools import partial
from durabletask.internal import helpers, orchestrator_service_pb2 as pb
from tests.durabletask.test_asyncio_orchestrators import _shared_future_cancellation_orchestrator
from tests.durabletask.test_coroutine_orchestrators import _completion, _execute, _start

data = json.load(sys.stdin)
orchestrator = partial(_shared_future_cancellation_orchestrator, direct_future=True, gather_wait=False)
initial = _execute(orchestrator, [], _start())
assert [action.SerializeToString().hex() for action in initial.actions] == data["initial"]
history = [pb.HistoryEvent.FromString(bytes.fromhex(event)) for event in data["history"]]
result = _completion(_execute(
    orchestrator, history, [helpers.new_task_completed_event(data["join_id"], '"joined"')]))
assert result.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED, result.failureDetails
assert json.loads(result.result.value) == "joined"
"""
    for seed in ("0", "999"):
        process = subprocess.run(
            [sys.executable, "-c", code], input=payload, capture_output=True, text=True,
            env={**os.environ, "PYTHONHASHSEED": seed}, timeout=30)
        assert process.returncode == 0, process.stdout + process.stderr


def test_cancelling_last_loop_timer_cannot_hide_foreign_wait() -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> None:
        handle = asyncio.get_running_loop().call_later(1, lambda: None)
        handle.cancel()
        await asyncio.Future()

    complete = _completion(_execute(orchestrator, [], _start()))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert "blocked without durable work" in complete.failureDetails.errorMessage


@pytest.mark.parametrize("observed", [False, True])
@pytest.mark.parametrize("probe_pending", [False, True])
def test_background_failure_is_reported_unless_observed(
        observed: bool, probe_pending: bool, caplog: pytest.LogCaptureFixture) -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> str:
        async def fail() -> None:
            raise ValueError("background failure")

        pending = asyncio.create_task(fail())
        if probe_pending:
            with pytest.raises(asyncio.InvalidStateError):
                pending.result()
            with pytest.raises(asyncio.InvalidStateError):
                pending.exception()
        await asyncio.sleep(0)
        if observed:
            await asyncio.gather(pending, return_exceptions=True)
        del pending
        await asyncio.sleep(0)
        gc.collect()
        return "done"

    assert json.loads(_completion(_execute(orchestrator, [], _start())).result.value) == "done"
    assert ("background failure" in caplog.text) is not observed


@pytest.mark.parametrize("count", [128, 4096])
def test_fanout_waiter_inspections_scale_linearly(
        count: int, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    inspections = 0

    def is_complete(durable: task.Task[Any]) -> bool:
        nonlocal inspections
        inspections += 1
        return durable._is_complete  # pyright: ignore[reportPrivateUsage]

    monkeypatch.setattr(task.Task, "is_complete", property(is_complete))

    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> list[Any]:
        async def branch(index: int) -> Any:
            return await ctx.call_activity("work", input=index)

        return await asyncio.gather(*(branch(index) for index in range(count)))

    result = _execute(orchestrator, [], _start())
    assert [action.id for action in result.actions] == list(range(1, count + 1))
    assert inspections < count * 20
    assert not caplog.records


@pytest.mark.parametrize("timer", [False, True])
def test_callback_stop_iteration_fails_instead_of_completing_orchestrator(timer: bool) -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> None:
        def fail() -> None:
            raise StopIteration("callback stopped")

        loop = asyncio.get_running_loop()
        if timer:
            loop.call_later(1, fail)
        else:
            loop.call_soon(fail)
        await ctx.wait_for_external_event("never")

    start = _start()
    if timer:
        deadline = NOW + timedelta(seconds=1)
        old = start + [helpers.new_timer_created_event(1, deadline)]
        new = [helpers.new_orchestrator_started_event(deadline), helpers.new_timer_fired_event(1, deadline)]
    else:
        old, new = [], start
    complete = _completion(_execute(orchestrator, old, new))
    assert complete.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete.failureDetails.errorType == "builtins.RuntimeError"
    assert complete.failureDetails.innerFailure.errorType == "builtins.StopIteration"
    assert complete.failureDetails.innerFailure.errorMessage == "callback stopped"


@pytest.mark.parametrize("cancelled", [None, 0, 1])
def test_identical_timer_callbacks_complete_and_cancel_independently(cancelled: int | None) -> None:
    async def orchestrator(ctx: task.OrchestrationContext, _: Any) -> int:
        loop = asyncio.get_running_loop()
        result = loop.create_future()
        completed: list[None] = []
        expected = 2 if cancelled is None else 1

        def callback() -> None:
            completed.append(None)
            if len(completed) == expected:
                result.set_result(len(completed))

        handles = [loop.call_at(loop.time() + 1, callback) for _ in range(2)]
        assert handles[0] != handles[1]
        assert len(set(handles)) == 2
        if cancelled is not None:
            handles[cancelled].cancel()
        return await result

    start = _start()
    initial = _execute(orchestrator, [], start)
    remaining = [index + 1 for index in range(2) if index != cancelled]
    assert [action.id for action in initial.actions] == remaining
    deadline = NOW + timedelta(seconds=1)
    old = start + [helpers.new_timer_created_event(index, deadline) for index in remaining]
    for offset, timer_id in enumerate(remaining):
        new = [helpers.new_orchestrator_started_event(deadline), helpers.new_timer_fired_event(timer_id, deadline)]
        result = _execute(orchestrator, old, new)
        assert _execute(orchestrator, old, new).actions == result.actions
        if offset < len(remaining) - 1:
            assert result.actions == []
        else:
            assert json.loads(_completion(result).result.value) == len(remaining)
        old += new
