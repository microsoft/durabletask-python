# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Replay and ownership tests for the Functions coroutine compatibility context."""

import asyncio
import base64
import inspect
import json
from collections.abc import Callable, Coroutine, Generator
from datetime import UTC, datetime, timedelta
from functools import partial, wraps
from typing import Any
from uuid import UUID

import pytest

import azure.durable_functions as df
from azure.durable_functions.internal.serialization import DEFAULT_FUNCTIONS_DATA_CONVERTER
from azure.durable_functions.worker import DurableFunctionsWorker
from durabletask import entities, task
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb


INSTANCE_ID = "async-compat-test"
NOW = datetime(2026, 1, 1)


class TypedInput:
    def __init__(self, value: int) -> None:
        self.value = value

    def to_json(self) -> dict[str, int]:
        return {"value": self.value}

    @classmethod
    def from_json(cls, data: dict[str, int]) -> "TypedInput":
        return cls(data["value"])


def _start() -> list[pb.HistoryEvent]:
    return [
        helpers.new_orchestrator_started_event(NOW),
        helpers.new_execution_started_event("compat_test", INSTANCE_ID, "null"),
    ]


def _execute(
    orchestrator: Callable[..., Any],
    old: list[pb.HistoryEvent],
    new: list[pb.HistoryEvent],
) -> pb.OrchestratorResponse:
    request = pb.OrchestratorRequest(instanceId=INSTANCE_ID, pastEvents=old, newEvents=new)
    encoded = base64.b64encode(request.SerializeToString()).decode()

    def invoke() -> pb.OrchestratorResponse:
        response = pb.OrchestratorResponse()
        result = DurableFunctionsWorker().execute_orchestration_request(orchestrator, encoded)
        response.ParseFromString(base64.b64decode(result))
        return response

    response = invoke()
    assert invoke().actions == response.actions
    return response


def _completed(response: pb.OrchestratorResponse) -> Any:
    assert len(response.actions) == 1
    assert response.actions[0].HasField("completeOrchestration")
    completed = response.actions[0].completeOrchestration
    assert completed.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED, completed.failureDetails
    return json.loads(completed.result.value)


def test_task_all_preserves_duplicates_order_and_shared_tasks() -> None:
    async def orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> list[Any]:
        shared = asyncio.create_task(ctx.call_activity("shared"))
        repeated = ctx.call_activity("repeated")
        left = asyncio.create_task(ctx.task_all([repeated, shared, repeated]))
        right = asyncio.create_task(ctx.task_all([shared]))
        return list(await asyncio.gather(left, right))

    start = _start()
    initial = _execute(orchestrator, [], start)
    assert [(action.id, action.scheduleTask.name) for action in initial.actions] == [
        (1, "shared"), (2, "repeated")]
    history = start + [
        helpers.new_task_scheduled_event(1, "shared"),
        helpers.new_task_scheduled_event(2, "repeated"),
    ]
    repeated_done = [helpers.new_task_completed_event(2, '"R"')]
    assert not _execute(orchestrator, history, repeated_done).actions
    shared_done = [helpers.new_task_completed_event(1, '"S"')]
    assert _completed(_execute(orchestrator, history + repeated_done, shared_done)) == [
        ["R", "S", "R"], ["S"]]


def test_cancelling_one_group_does_not_cancel_another_groups_shared_task() -> None:
    async def orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> list[Any]:
        shared = asyncio.create_task(ctx.wait_for_external_event("shared"))
        cancelled = asyncio.create_task(ctx.task_all([shared]))
        survivor = asyncio.create_task(ctx.task_all([shared, shared]))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        cancelled.cancel()
        try:
            await cancelled
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("Expected group cancellation")
        return await survivor

    start = _start()
    assert not _execute(orchestrator, [], start).actions
    assert _completed(_execute(
        orchestrator, start, [helpers.new_event_raised_event("shared", '"value"')])) == ["value", "value"]


def test_task_all_waits_for_every_child_and_raises_first_observed_failure() -> None:
    async def orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> str:
        try:
            await ctx.task_all([
                ctx.call_activity("first"),
                ctx.call_activity("second"),
                ctx.call_activity("last"),
            ])
        except task.TaskFailedError as error:
            return str(error)
        raise AssertionError("Expected the group to fail")

    start = _start()
    assert [a.scheduleTask.name for a in _execute(orchestrator, [], start).actions] == [
        "first", "second", "last"]
    history = start + [
        helpers.new_task_scheduled_event(1, "first"),
        helpers.new_task_scheduled_event(2, "second"),
        helpers.new_task_scheduled_event(3, "last"),
    ]
    second_failed = [helpers.new_task_failed_event(2, ValueError("second failure"))]
    first_failed = [helpers.new_task_failed_event(1, ValueError("first failure"))]
    assert not _execute(orchestrator, history, second_failed).actions
    assert not _execute(orchestrator, history + second_failed, first_failed).actions
    result = _completed(_execute(
        orchestrator, history + second_failed + first_failed,
        [helpers.new_task_completed_event(3, '"done"')]))
    assert "second failure" in result
    assert "first failure" not in result


@pytest.mark.parametrize("group_method", ["task_all", "task_any"])
def test_group_failure_order_follows_completions_in_one_history_batch(group_method: str) -> None:
    async def orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> str:
        first = asyncio.create_task(ctx.call_activity("first"))
        second = asyncio.create_task(ctx.call_activity("second"))
        try:
            if group_method == "task_all":
                await ctx.task_all([first, second])
            else:
                winner = await ctx.task_any([first, second])
                assert winner is second
                try:
                    await winner
                finally:
                    try:
                        await first
                    except task.TaskFailedError:
                        pass
        except task.TaskFailedError as error:
            return str(error)
        raise AssertionError("Expected the group to fail")

    start = _start()
    assert [a.scheduleTask.name for a in _execute(orchestrator, [], start).actions] == ["first", "second"]
    history = start + [
        helpers.new_task_scheduled_event(1, "first"),
        helpers.new_task_scheduled_event(2, "second"),
    ]
    failures = [
        helpers.new_task_failed_event(2, ValueError("second failure")),
        helpers.new_task_failed_event(1, ValueError("first failure")),
    ]
    result = _completed(_execute(orchestrator, history, failures))
    assert "second failure" in result
    assert "first failure" not in result


@pytest.mark.parametrize("fail", [False, True])
def test_task_any_returns_original_winner_and_keeps_loser_running(fail: bool) -> None:
    async def orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> list[Any]:
        slow = asyncio.create_task(ctx.call_activity("slow"))
        fast = asyncio.create_task(ctx.call_activity("fast"))
        winner = await ctx.task_any([slow, fast, slow])
        assert winner is fast
        assert not slow.cancelled()
        try:
            value = await winner
        except task.TaskFailedError:
            value = "failed"
        return [value, await slow]

    start = _start()
    assert [a.scheduleTask.name for a in _execute(orchestrator, [], start).actions] == ["slow", "fast"]
    history = start + [
        helpers.new_task_scheduled_event(1, "slow"),
        helpers.new_task_scheduled_event(2, "fast"),
    ]
    fast_done = [helpers.new_task_failed_event(2, ValueError("fast failure"))] if fail else [
        helpers.new_task_completed_event(2, '"fast"')]
    assert not _execute(orchestrator, history, fast_done).actions
    slow_done = [helpers.new_task_completed_event(1, '"slow"')]
    assert _completed(_execute(orchestrator, history + fast_done, slow_done)) == [
        "failed" if fail else "fast", "slow"]


def test_task_any_keeps_owned_coroutine_loser_running() -> None:
    async def orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> str:
        async def slower() -> None:
            await ctx.call_activity("slow")
            await ctx.call_activity("continued")

        winner = await ctx.task_any([slower(), ctx.call_activity("fast")])
        assert await winner == "fast"
        return await ctx.wait_for_external_event("finish", str)

    start = _start()
    assert [a.scheduleTask.name for a in _execute(orchestrator, [], start).actions] == ["slow", "fast"]
    history = start + [
        helpers.new_task_scheduled_event(1, "slow"),
        helpers.new_task_scheduled_event(2, "fast"),
    ]
    fast_done = [helpers.new_task_completed_event(2, '"fast"')]
    assert not _execute(orchestrator, history, fast_done).actions
    slow_done = [helpers.new_task_completed_event(1, '"slow"')]
    continued = _execute(orchestrator, history + fast_done, slow_done)
    assert [(a.id, a.scheduleTask.name) for a in continued.actions] == [(3, "continued")]
    history += fast_done + slow_done + [
        helpers.new_task_scheduled_event(3, "continued"),
        helpers.new_task_completed_event(3, '"done"'),
    ]
    assert _completed(_execute(
        orchestrator, history, [helpers.new_event_raised_event("finish", '"done"')])) == "done"


@pytest.mark.parametrize("group_method", ["task_all", "task_any"])
def test_cancelling_group_withdraws_owned_events_but_not_borrowed_tasks(group_method: str) -> None:
    async def orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> list[Any]:
        borrowed = asyncio.create_task(ctx.wait_for_external_event("borrowed"))
        operations = [ctx.wait_for_external_event("owned"), borrowed]
        group = asyncio.create_task(getattr(ctx, group_method)(operations))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        group.cancel()
        try:
            await group
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("Expected group cancellation")
        assert not borrowed.done()
        replacement = await ctx.wait_for_external_event("owned")
        return [replacement, await borrowed]

    start = _start()
    assert not _execute(orchestrator, [], start).actions
    events = [
        helpers.new_event_raised_event("owned", '"replacement"'),
        helpers.new_event_raised_event("borrowed", '"borrowed"'),
    ]
    assert _completed(_execute(orchestrator, start, events)) == ["replacement", "borrowed"]


@pytest.mark.parametrize("group_method", ["task_all", "task_any"])
def test_cancelling_group_withdraws_owned_timers_but_not_borrowed_tasks(group_method: str) -> None:
    async def orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> str:
        borrowed = asyncio.create_task(ctx.create_timer(timedelta(seconds=5)))
        operations = [ctx.create_timer(timedelta(seconds=10)), borrowed]
        group = asyncio.create_task(getattr(ctx, group_method)(operations))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        group.cancel()
        try:
            await group
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("Expected group cancellation")
        assert not borrowed.cancelled()
        await borrowed
        return "survived"

    start = _start()
    initial = _execute(orchestrator, [], start)
    assert len(initial.actions) == 1
    timer = initial.actions[0]
    assert timer.createTimer.fireAt.ToDatetime() == NOW + timedelta(seconds=5)
    deadline = NOW + timedelta(seconds=5)
    history = start + [helpers.new_timer_created_event(timer.id, deadline)]
    fired = [helpers.new_orchestrator_started_event(deadline), helpers.new_timer_fired_event(timer.id, deadline)]
    assert _completed(_execute(orchestrator, history, fired)) == "survived"


@pytest.mark.parametrize("group_method", ["task_all", "task_any"])
def test_group_timeout_withdraws_owned_event_wait(group_method: str) -> None:
    async def orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> str:
        try:
            await asyncio.wait_for(
                getattr(ctx, group_method)([ctx.wait_for_external_event("approval")]), timeout=1)
        except TimeoutError:
            return await ctx.wait_for_external_event("approval", str)
        raise AssertionError("Expected timeout")

    start = _start()
    initial = _execute(orchestrator, [], start)
    assert len(initial.actions) == 1
    timer = initial.actions[0]
    deadline = NOW + timedelta(seconds=1)
    assert timer.createTimer.fireAt.ToDatetime() == deadline
    history = start + [helpers.new_timer_created_event(timer.id, deadline)]
    timeout = [helpers.new_orchestrator_started_event(deadline), helpers.new_timer_fired_event(timer.id, deadline)]
    assert not _execute(orchestrator, history, timeout).actions
    assert _completed(_execute(
        orchestrator, history + timeout, [helpers.new_event_raised_event("approval", '"approved"')])) == "approved"


def test_empty_and_invalid_groups() -> None:
    async def orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> list[Any]:
        assert await ctx.task_all([]) == []
        errors: list[str] = []
        invalid: Any = object()
        operations: list[Coroutine[Any, Any, Any]] = [
            ctx.task_any([]), ctx.task_all([invalid]), ctx.task_any([invalid])]
        for operation in operations:
            try:
                await operation
            except (TypeError, ValueError) as error:
                errors.append(str(error))
        return errors

    result = _completed(_execute(orchestrator, [], _start()))
    assert result == [
        "task_any requires at least one operation.",
        "Orchestration operations must be awaitable.",
        "Orchestration operations must be awaitable.",
    ]


@pytest.mark.parametrize("group_method", ["task_all", "task_any"])
def test_invalid_group_does_not_start_its_other_operations(group_method: str) -> None:
    async def orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> str:
        operation = ctx.call_activity("work")
        try:
            await getattr(ctx, group_method)([operation, object()])
        except TypeError as error:
            assert str(error) == "Orchestration operations must be awaitable."
        else:
            raise AssertionError("Expected invalid group to fail")
        return await operation

    start = _start()
    initial = _execute(orchestrator, [], start)
    assert [(a.id, a.scheduleTask.name) for a in initial.actions] == [(1, "work")]
    history = start + [helpers.new_task_scheduled_event(1, "work")]
    assert _completed(_execute(
        orchestrator, history, [helpers.new_task_completed_event(1, '"done"')])) == "done"


@pytest.mark.parametrize("shape", ["function", "wrapped", "partial", "object", "partial-object"])
def test_v1_async_callable_shapes_receive_compatibility_context(shape: str) -> None:
    async def plain(ctx: df.AsyncDurableOrchestrationContext) -> Any:
        assert isinstance(ctx, df.AsyncDurableOrchestrationContext)
        return await ctx.call_activity("work", ctx.get_input())

    @wraps(plain)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        return plain(*args, **kwargs)

    async def with_prefix(prefix: str, ctx: df.AsyncDurableOrchestrationContext) -> Any:
        assert isinstance(ctx, df.AsyncDurableOrchestrationContext)
        return await ctx.call_activity("work", prefix)

    class AsyncCallable:
        async def __call__(self, ctx: df.AsyncDurableOrchestrationContext) -> Any:
            return await plain(ctx)

    class PartialCallable:
        async def __call__(self, prefix: str, ctx: df.AsyncDurableOrchestrationContext) -> Any:
            return await with_prefix(prefix, ctx)

    variants = {
        "function": plain,
        "wrapped": wrapped,
        "partial": partial(with_prefix, "value"),
        "object": AsyncCallable(),
        "partial-object": partial(PartialCallable(), "value"),
    }
    fn = variants[shape]
    start = _start()
    initial = _execute(fn, [], start)
    assert len(initial.actions) == 1
    assert initial.actions[0].scheduleTask.name == "work"
    history = start + [helpers.new_task_scheduled_event(1, "work")]
    assert _completed(_execute(fn, history, [helpers.new_task_completed_event(1, '"done"')])) == "done"


def test_unmarked_v1_wrapper_returning_coroutine_fails_and_closes_it() -> None:
    created: list[Any] = []

    async def async_orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> str:
        return "unexpected"

    def wrapper(ctx: df.DurableOrchestrationContext) -> Any:
        coroutine = async_orchestrator(ctx)  # type: ignore[arg-type]
        created.append(coroutine)
        return coroutine

    result = _execute(wrapper, [], _start())
    failure = result.actions[0].completeOrchestration
    assert failure.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert "synchronous orchestrator returned a coroutine" in failure.failureDetails.errorMessage
    assert all(inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED for coroutine in created)


def test_metadata_status_and_eager_context_share_state() -> None:
    def inspect_context(ctx: df.DurableOrchestrationContext | df.AsyncDurableOrchestrationContext) -> list[Any]:
        ctx.set_custom_status({"step": 1})
        assert ctx.custom_status == {"step": 1}
        assert ctx.get_input(dict) == {"number": 7}
        assert vars(ctx.function_context) == {}
        with pytest.raises(NotImplementedError):
            _ = ctx.histories
        uuid = ctx.new_uuid()
        guid = ctx.new_guid()
        assert isinstance(guid, UUID)
        return [
            ctx.instance_id,
            ctx.parent_instance_id,
            ctx.version,
            ctx.is_replaying,
            ctx.current_utc_datetime.isoformat(),
            uuid,
            str(guid),
        ]

    async def coroutine(ctx: df.AsyncDurableOrchestrationContext) -> list[Any]:
        result = inspect_context(ctx)
        eager = ctx.as_task_context()
        assert eager.custom_status == ctx.custom_status
        eager.set_custom_status({"step": 2})
        assert ctx.custom_status == {"step": 2}
        return result

    def generator_style(ctx: df.DurableOrchestrationContext) -> list[Any]:
        result = inspect_context(ctx)
        ctx.set_custom_status({"step": 2})
        return result

    started = helpers.new_execution_started_event(
        "compat_test", INSTANCE_ID, '{"number": 7}', version="v1")
    started.executionStarted.parentInstance.CopyFrom(pb.ParentInstanceInfo(
        taskScheduledId=1,
        orchestrationInstance=pb.OrchestrationInstance(instanceId="parent")))
    start = [helpers.new_orchestrator_started_event(NOW), started]
    result = _execute(coroutine, [], start)
    assert result.actions == _execute(generator_style, [], start).actions
    values = _completed(result)
    assert values[:5] == [INSTANCE_ID, "parent", "v1", False, NOW.replace(tzinfo=UTC).isoformat()]
    assert str(UUID(values[5])) == values[5]
    assert str(UUID(values[6])) == values[6]
    assert json.loads(result.customStatus.value) == {"step": 2}


@pytest.mark.parametrize("strict", [False, True])
async def test_decorator_input_type_reaches_async_compat_context(monkeypatch: pytest.MonkeyPatch, strict: bool) -> None:
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "1" if strict else "0")
    app = df.DFApp()

    async def orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> int:
        typed = ctx.get_input()
        assert isinstance(typed, TypedInput)
        assert isinstance(ctx.get_input(TypedInput), TypedInput)
        return typed.value

    function = app.orchestration_trigger(context_name="ctx", input_type=TypedInput)(orchestrator)
    encoded_input = DEFAULT_FUNCTIONS_DATA_CONVERTER.serialize(TypedInput(7))
    start = [
        helpers.new_orchestrator_started_event(NOW),
        helpers.new_execution_started_event("compat_test", INSTANCE_ID, encoded_input),
    ]
    request = pb.OrchestratorRequest(instanceId=INSTANCE_ID, newEvents=start)
    encoded = base64.b64encode(request.SerializeToString()).decode()
    result = await function.build().get_user_function()(ctx=encoded)
    response = pb.OrchestratorResponse()
    response.ParseFromString(base64.b64decode(result))
    assert _completed(response) == 7


def test_continue_as_new_shares_compatibility_state() -> None:
    async def orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> None:
        await ctx.wait_for_external_event("next")
        ctx.as_task_context().continue_as_new({"next": 1})
        assert ctx.will_continue_as_new

    result = _execute(orchestrator, _start(), [helpers.new_event_raised_event("next", "null")])
    assert len(result.actions) == 1
    completion = result.actions[0].completeOrchestration
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_CONTINUED_AS_NEW
    assert json.loads(completion.result.value) == {"next": 1}


@pytest.mark.parametrize("sub_orchestration", [False, True])
def test_retry_operations_match_generator_replay(sub_orchestration: bool) -> None:
    policy = df.RetryPolicy(first_retry_interval=timedelta(seconds=2), max_number_of_attempts=3)

    async def coroutine(ctx: df.AsyncDurableOrchestrationContext) -> Any:
        if sub_orchestration:
            return await ctx.call_sub_orchestrator_with_retry("child", policy, 3, "child-id")
        return await ctx.call_activity_with_retry("work", policy, 3)

    def generator_style(ctx: df.DurableOrchestrationContext) -> Generator[task.Task[Any], Any, Any]:
        if sub_orchestration:
            return (yield ctx.call_sub_orchestrator_with_retry("child", policy, 3, "child-id"))
        return (yield ctx.call_activity_with_retry("work", policy, 3))

    def replay(old: list[pb.HistoryEvent], new: list[pb.HistoryEvent]) -> pb.OrchestratorResponse:
        response = _execute(coroutine, old, new)
        assert response.actions == _execute(generator_style, old, new).actions
        return response

    start = _start()
    initial = replay([], start)
    if sub_orchestration:
        assert json.loads(initial.actions[0].createSubOrchestration.input.value) == 3
        scheduled = helpers.new_sub_orchestration_created_event(1, "child", "child-id")
        failed = helpers.new_sub_orchestration_failed_event(1, ValueError("retry me"))
        completed = helpers.new_sub_orchestration_completed_event(1, "42")
    else:
        assert json.loads(initial.actions[0].scheduleTask.input.value) == 3
        scheduled = helpers.new_task_scheduled_event(1, "work")
        failed = helpers.new_task_failed_event(1, ValueError("retry me"))
        completed = helpers.new_task_completed_event(1, "42")
    history = start + [scheduled]
    retry = replay(history, [failed])
    assert retry.actions[0].id == 2
    deadline = retry.actions[0].createTimer.fireAt.ToDatetime()
    history += [failed, helpers.new_timer_created_event(2, deadline)]
    fired = [helpers.new_orchestrator_started_event(deadline), helpers.new_timer_fired_event(2, deadline)]
    assert replay(history, fired).actions[0].id == 1
    assert _completed(replay(history + fired + [scheduled], [completed])) == 42


def test_entity_call_and_signal_replay_with_v1_argument_names() -> None:
    entity = entities.EntityInstanceId("counter", "one")

    async def orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> Any:
        ctx.signal_entity(entityId=entity, operationName="add", operationInput=3)
        return await ctx.call_entity(entityId=entity, operationName="get")

    start = _start()
    initial = _execute(orchestrator, [], start)
    assert len(initial.actions) == 2
    signal = initial.actions[0].sendEntityMessage.entityOperationSignaled
    call = initial.actions[1].sendEntityMessage.entityOperationCalled
    assert signal.operation == "add"
    assert json.loads(signal.input.value) == 3
    assert call.operation == "get"
    assert call.targetInstanceId.value == str(entity)
    history = start + [
        helpers.new_event_sent_event(1, str(entity), json.dumps({"id": signal.requestId})),
        helpers.new_event_sent_event(2, str(entity), json.dumps({"id": call.requestId})),
    ]
    response = helpers.new_event_raised_event(call.requestId, json.dumps({"result": "42"}))
    assert _completed(_execute(orchestrator, history, [response])) == 42


def test_durable_http_is_cold_and_decodes_response() -> None:
    from azure.durable_functions.http.builtin import BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME

    async def orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> list[Any]:
        invalid = ctx.call_http("POST", "https://example.com", {"bad": True}, is_raw_str=True)
        try:
            await invalid
        except TypeError:
            pass
        else:
            raise AssertionError("Expected invalid raw content to fail when started")
        response = await ctx.call_http(
            "POST", "https://example.com", {"number": 3}, {"x-test": "value"},
            df.ManagedIdentityTokenSource("https://management.core.windows.net/"))
        assert isinstance(response, df.DurableHttpResponse)
        return [response.status_code, response.headers, response.content]

    start = _start()
    initial = _execute(orchestrator, [], start)
    assert len(initial.actions) == 1
    action = initial.actions[0].createSubOrchestration
    assert action.name == BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME
    request = json.loads(action.input.value)
    assert request == {
        "method": "POST",
        "uri": "https://example.com",
        "content": '{"number": 3}',
        "headers": {"x-test": "value"},
        "tokenSource": {"kind": "AzureManagedIdentity", "resource": "https://management.core.windows.net/"},
    }
    history = start + [helpers.new_sub_orchestration_created_event(
        1, action.name, action.instanceId)]
    result = DEFAULT_FUNCTIONS_DATA_CONVERTER.serialize(
        df.DurableHttpResponse(201, {"reply": "yes"}, "ok"))
    assert result is not None
    assert _completed(_execute(
        orchestrator, history, [helpers.new_sub_orchestration_completed_event(1, result)])) == [
            201, {"reply": "yes"}, "ok"]


def test_durable_http_retries_are_snapshotted_when_coroutine_starts() -> None:
    from azure.durable_functions.http.builtin import BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME

    class CountingPolicy(df.HttpRetryOptions):
        def __init__(self) -> None:
            super().__init__(
                first_retry_interval=timedelta(seconds=2), max_number_of_attempts=3,
                status_codes_to_retry=[503])
            self.snapshots = 0

        def to_json(self) -> dict[str, Any]:
            self.snapshots += 1
            return super().to_json()

    async def orchestrator(ctx: df.AsyncDurableOrchestrationContext) -> str | None:
        headers = {"stage": "before"}
        policy = CountingPolicy()
        request = ctx.call_http("GET", "https://example.com", headers=headers, retry_options=policy)
        assert policy.snapshots == 0
        headers["stage"] = "after"
        response = await request
        assert policy.snapshots > 0
        return response.content

    start = _start()
    initial = _execute(orchestrator, [], start)
    assert len(initial.actions) == 1
    action = initial.actions[0].createSubOrchestration
    assert action.name == BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME
    request = json.loads(action.input.value)
    assert request["headers"] == {"stage": "after"}
    assert request["retryOptions"] == df.HttpRetryOptions(
        first_retry_interval=timedelta(seconds=2), max_number_of_attempts=3,
        status_codes_to_retry=[503]).to_json()
    history = start + [helpers.new_sub_orchestration_created_event(
        1, action.name, action.instanceId)]
    response = DEFAULT_FUNCTIONS_DATA_CONVERTER.serialize(df.DurableHttpResponse(200, content="ok"))
    assert response is not None
    assert _completed(_execute(
        orchestrator, history, [helpers.new_sub_orchestration_completed_event(1, response)])) == "ok"
