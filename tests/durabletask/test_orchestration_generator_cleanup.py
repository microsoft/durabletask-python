# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import asyncio
import gc
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any, Generator
from unittest.mock import Mock
from weakref import ReferenceType, ref

import anyio
import pytest

from durabletask import task, worker
from durabletask.internal import exceptions, helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.serialization import JsonDataConverter

TEST_INSTANCE_ID = "generator-cleanup"
TEST_LOGGER = logging.getLogger("tests.generator_cleanup")
START_TIME = datetime(2026, 1, 1)


def _executor(
        orchestrator: task.Orchestrator[Any, Any],
) -> worker._OrchestrationExecutor:  # pyright: ignore[reportPrivateUsage]
    registry = worker._Registry()  # pyright: ignore[reportPrivateUsage]
    registry.add_named_orchestrator("orchestrator", orchestrator)
    return worker._OrchestrationExecutor(  # pyright: ignore[reportPrivateUsage]
        registry, TEST_LOGGER, JsonDataConverter())


def _start_events() -> list[pb.HistoryEvent]:
    return [
        helpers.new_orchestrator_started_event(START_TIME),
        helpers.new_execution_started_event("orchestrator", TEST_INSTANCE_ID),
    ]


def test_suspended_generator_closes_after_response_snapshot():
    closed: list[str] = []
    generators: list[Generator[task.Task[Any], Any, Any]] = []

    def episode(ctx: task.OrchestrationContext):
        timer = ctx.create_timer(timedelta(seconds=5))
        ctx.set_custom_status("waiting")
        try:
            yield timer
        finally:
            closed.append("closed")
            timer.cancel()
            ctx.call_activity("cleanup")
            ctx.set_custom_status("cleanup")
            ctx.continue_as_new("cleanup")

    def orchestrator(ctx: task.OrchestrationContext, _: Any):
        generator = episode(ctx)
        generators.append(generator)
        return generator

    result = _executor(orchestrator).execute(TEST_INSTANCE_ID, [], _start_events())

    assert closed == ["closed"]
    assert len(result.actions) == 1
    assert result.actions[0].HasField("createTimer")
    assert result.actions[0].createTimer.fireAt.ToDatetime() == START_TIME + timedelta(seconds=5)
    assert result.encoded_custom_status == '"waiting"'
    generators[0].close()
    generators[0].close()
    assert closed == ["closed"]


@pytest.mark.parametrize("fail", [False, True])
@pytest.mark.parametrize("suspend_first", [False, True])
def test_exhausted_generator_cleanup_is_not_repeated(fail: bool, suspend_first: bool):
    closed: list[str] = []
    generators: list[Generator[task.Task[Any], Any, Any]] = []

    def episode(ctx: task.OrchestrationContext):
        try:
            if suspend_first:
                yield ctx.wait_for_external_event("ready")
            if fail:
                raise ValueError("orchestrator failed")
            return "done"
        finally:
            closed.append("closed")

    def orchestrator(ctx: task.OrchestrationContext, _: Any):
        generator = episode(ctx)
        generators.append(generator)
        return generator

    result = _executor(orchestrator).execute(
        TEST_INSTANCE_ID, [], _start_events() + [helpers.new_event_raised_event("ready", None)])

    assert closed == ["closed"]
    generators[0].close()
    assert closed == ["closed"]
    assert len(result.actions) == 1
    completion = result.actions[0].completeOrchestration
    if fail:
        assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
        assert completion.failureDetails.errorMessage == "orchestrator failed"
    else:
        assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
        assert completion.result.value == '"done"'


def test_generator_stays_open_across_old_and_new_history_events():
    observed: list[str] = []

    def orchestrator(ctx: task.OrchestrationContext, _: Any) -> Generator[task.Task[Any], str, None]:
        try:
            observed.append((yield ctx.call_activity("first", return_type=str)))
            observed.append((yield ctx.call_activity("second", return_type=str)))
            assert observed == ["first result", "second result"]
            yield ctx.call_activity("third", return_type=str)
        finally:
            observed.append("closed")

    old_events = _start_events() + [
        helpers.new_task_scheduled_event(1, "first"),
        helpers.new_task_completed_event(1, '"first result"'),
        helpers.new_task_scheduled_event(2, "second"),
    ]
    new_events = [helpers.new_task_completed_event(2, '"second result"')]
    result = _executor(orchestrator).execute(TEST_INSTANCE_ID, old_events, new_events)

    assert observed == ["first result", "second result", "closed"]
    assert len(result.actions) == 1
    assert result.actions[0].id == 3
    assert result.actions[0].scheduleTask.name == "third"


def test_continue_as_new_closes_suspended_generator():
    closed: list[str] = []

    def orchestrator(ctx: task.OrchestrationContext, _: Any):
        try:
            ctx.continue_as_new("next")
            yield ctx.wait_for_external_event("unused")
        finally:
            closed.append("closed")
            ctx.call_activity("cleanup")

    result = _executor(orchestrator).execute(TEST_INSTANCE_ID, [], _start_events())

    assert closed == ["closed"]
    assert len(result.actions) == 1
    completion = result.actions[0].completeOrchestration
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_CONTINUED_AS_NEW
    assert completion.result.value == '"next"'


def test_termination_closes_suspended_generator():
    closed: list[str] = []

    def orchestrator(ctx: task.OrchestrationContext, _: Any):
        try:
            yield ctx.wait_for_external_event("ready")
        finally:
            closed.append("closed")

    result = _executor(orchestrator).execute(
        TEST_INSTANCE_ID, _start_events(),
        [pb.HistoryEvent(executionTerminated=pb.ExecutionTerminatedEvent())])

    assert closed == ["closed"]
    assert len(result.actions) == 1
    assert result.actions[0].completeOrchestration.orchestrationStatus == pb.ORCHESTRATION_STATUS_TERMINATED


def test_response_construction_failure_still_closes_generator(monkeypatch: pytest.MonkeyPatch):
    closed: list[str] = []

    def orchestrator(ctx: task.OrchestrationContext, _: Any):
        try:
            yield ctx.wait_for_external_event("ready")
        finally:
            closed.append("closed")

    def fail_get_actions(ctx: worker._RuntimeOrchestrationContext):  # pyright: ignore[reportPrivateUsage]
        raise ValueError("response failed")

    monkeypatch.setattr(
        worker._RuntimeOrchestrationContext, "get_actions", fail_get_actions)  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(ValueError, match="response failed"):
        _executor(orchestrator).execute(TEST_INSTANCE_ID, [], _start_events())
    assert closed == ["closed"]


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_history_failure_closes_suspended_generator(
        cleanup_fails: bool, caplog: pytest.LogCaptureFixture):
    closed: list[str] = []

    def orchestrator(ctx: task.OrchestrationContext, _: Any):
        try:
            yield ctx.call_activity("expected", return_type=str)
        finally:
            closed.append("closed")
            if cleanup_fails:
                raise ValueError("cleanup failed")

    old_events = _start_events()
    with caplog.at_level(logging.WARNING, logger=TEST_LOGGER.name):
        result = _executor(orchestrator).execute(
            TEST_INSTANCE_ID, old_events, [helpers.new_task_scheduled_event(1, "wrong")])

    assert closed == ["closed"]
    assert len(result.actions) == 1
    completion = result.actions[0].completeOrchestration
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert "NonDeterminismError" in completion.failureDetails.errorType
    assert "cleanup failed" not in completion.failureDetails.errorMessage
    assert bool(caplog.records) == cleanup_fails


@pytest.mark.parametrize("strategy", list(worker.VersionFailureStrategy))
@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_version_failure_closes_generator_without_changing_dispatch_outcome(
        strategy: worker.VersionFailureStrategy, cleanup_fails: bool,
        monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    closed: list[str] = []

    def orchestrator(ctx: task.OrchestrationContext, _: Any):
        try:
            yield ctx.wait_for_external_event("ready")
        finally:
            closed.append("closed")
            if cleanup_fails:
                raise ValueError("cleanup failed")

    executor = _executor(orchestrator)
    executor._registry.versioning = worker.VersioningOptions(  # pyright: ignore[reportPrivateUsage]
        failure_strategy=strategy, match_strategy=worker.VersionMatchStrategy.NONE)
    process_event = executor.process_event
    failure = pb.TaskFailureDetails(errorType="VersionMismatch", errorMessage="version failed")

    def process_with_version_failure(
            ctx: worker._RuntimeOrchestrationContext,  # pyright: ignore[reportPrivateUsage]
            event: pb.HistoryEvent):
        process_event(ctx, event)
        if event.HasField("eventRaised"):
            raise exceptions.VersionFailureException(failure)

    monkeypatch.setattr(executor, "process_event", process_with_version_failure)
    events = _start_events() + [helpers.new_event_raised_event("unrelated", None)]
    with caplog.at_level(logging.WARNING, logger=TEST_LOGGER.name):
        if strategy == worker.VersionFailureStrategy.REJECT:
            with pytest.raises(exceptions.AbandonOrchestrationError):
                executor.execute(TEST_INSTANCE_ID, [], events)
        else:
            result = executor.execute(TEST_INSTANCE_ID, [], events)
            assert len(result.actions) == 1
            assert result.actions[0].completeOrchestration.failureDetails == failure
    assert closed == ["closed"]
    assert bool(caplog.records) == cleanup_fails


@pytest.mark.parametrize("yield_during_close", [False, True])
def test_cleanup_failure_is_logged_without_changing_returned_actions(
        yield_during_close: bool, caplog: pytest.LogCaptureFixture):
    generators: list[Generator[task.Task[Any], Any, Any]] = []

    def episode(ctx: task.OrchestrationContext):
        try:
            yield ctx.create_timer(timedelta(seconds=5))
        finally:
            if yield_during_close:
                yield ctx.wait_for_external_event("invalid-cleanup")
            else:
                raise ValueError("cleanup failed")

    def orchestrator(ctx: task.OrchestrationContext, _: Any):
        generator = episode(ctx)
        generators.append(generator)
        return generator

    try:
        with caplog.at_level(logging.WARNING, logger=TEST_LOGGER.name):
            result = _executor(orchestrator).execute(TEST_INSTANCE_ID, [], _start_events())
        assert len(result.actions) == 1
        assert result.actions[0].HasField("createTimer")
        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.levelno == logging.WARNING
        assert TEST_INSTANCE_ID in record.message
        assert "orchestrator" in record.message
        assert "<unknown>" not in record.message
        assert record.exc_info is not None
        expected = "generator ignored GeneratorExit" if yield_during_close else "cleanup failed"
        assert str(record.exc_info[1]) == expected
    finally:
        for generator in generators:
            generator.close()


def _cancelling_orchestrator(
        ctx: task.OrchestrationContext, _: Any,
) -> Generator[task.Task[Any], Any, None]:
    loop = asyncio.new_event_loop()

    async def body():
        await asyncio.Future[None]()

    root = loop.create_task(body())
    loop.run_until_complete(asyncio.sleep(0))
    ctx.set_custom_status("waiting")
    try:
        yield ctx.create_timer(timedelta(seconds=5))
    finally:
        root.cancel()
        try:
            loop.run_until_complete(root)
        finally:
            loop.close()


@pytest.mark.parametrize("via_worker", [False, True])
def test_teardown_cancellation_preserves_response(
        via_worker: bool, caplog: pytest.LogCaptureFixture):
    executor = _executor(_cancelling_orchestrator)
    with caplog.at_level(logging.WARNING, logger=TEST_LOGGER.name):
        if via_worker:
            with worker.TaskHubGrpcWorker(logger=TEST_LOGGER) as host:
                host._registry = executor._registry  # pyright: ignore[reportPrivateUsage]
                stub = Mock()
                request = pb.OrchestratorRequest(instanceId=TEST_INSTANCE_ID, newEvents=_start_events())
                host._execute_orchestrator(request, stub, "token")  # pyright: ignore[reportPrivateUsage]
            stub.CompleteOrchestratorTask.assert_called_once()
            stub.AbandonTaskOrchestratorWorkItem.assert_not_called()
            response: pb.OrchestratorResponse = stub.CompleteOrchestratorTask.call_args.args[0]
            assert response.instanceId == TEST_INSTANCE_ID
            assert response.completionToken == "token"
            actions = list(response.actions)
            custom_status = response.customStatus.value
        else:
            result = executor.execute(TEST_INSTANCE_ID, [], _start_events())
            actions = result.actions
            custom_status = result.encoded_custom_status

    assert actions == [helpers.new_create_timer_action(1, START_TIME + timedelta(seconds=5))]
    assert custom_status == '"waiting"'
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.levelno == logging.WARNING
    assert record.message == f"{TEST_INSTANCE_ID}: Failed to close orchestrator orchestrator generator."
    assert record.exc_info is not None
    assert record.exc_info[0] is asyncio.CancelledError


@pytest.mark.parametrize("via_worker", [False, True])
def test_teardown_cancellation_preserves_version_rejection(
        via_worker: bool, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture):
    executor = _executor(_cancelling_orchestrator)
    executor._registry.versioning = worker.VersioningOptions(  # pyright: ignore[reportPrivateUsage]
        failure_strategy=worker.VersionFailureStrategy.REJECT,
        match_strategy=worker.VersionMatchStrategy.NONE)
    process_event = worker._OrchestrationExecutor.process_event  # pyright: ignore[reportPrivateUsage]

    def process_with_version_failure(
            self: worker._OrchestrationExecutor,  # pyright: ignore[reportPrivateUsage]
            ctx: worker._RuntimeOrchestrationContext,  # pyright: ignore[reportPrivateUsage]
            event: pb.HistoryEvent):
        process_event(self, ctx, event)
        if event.HasField("eventRaised"):
            raise exceptions.VersionFailureException(pb.TaskFailureDetails(errorType="VersionMismatch"))

    monkeypatch.setattr(
        worker._OrchestrationExecutor, "process_event", process_with_version_failure)  # pyright: ignore[reportPrivateUsage]
    events = _start_events() + [helpers.new_event_raised_event("unrelated", None)]
    with caplog.at_level(logging.WARNING, logger=TEST_LOGGER.name):
        if via_worker:
            with worker.TaskHubGrpcWorker(logger=TEST_LOGGER) as host:
                host._registry = executor._registry  # pyright: ignore[reportPrivateUsage]
                stub = Mock()
                request = pb.OrchestratorRequest(instanceId=TEST_INSTANCE_ID, newEvents=events)
                host._execute_orchestrator(request, stub, "token")  # pyright: ignore[reportPrivateUsage]
            stub.CompleteOrchestratorTask.assert_not_called()
            stub.AbandonTaskOrchestratorWorkItem.assert_called_once_with(
                pb.AbandonOrchestrationTaskRequest(completionToken="token"))
        else:
            with pytest.raises(exceptions.AbandonOrchestrationError):
                executor.execute(TEST_INSTANCE_ID, [], events)

    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.levelno == logging.WARNING
    assert record.exc_info is not None
    assert record.exc_info[0] is asyncio.CancelledError


@pytest.mark.parametrize("error_type", [SystemExit, KeyboardInterrupt])
def test_process_control_exception_from_cleanup_still_propagates(error_type: type[BaseException]):
    def orchestrator(ctx: task.OrchestrationContext, _: Any):
        try:
            yield ctx.wait_for_external_event("ready")
        finally:
            raise error_type("stop")

    with pytest.raises(error_type, match="stop"):
        _executor(orchestrator).execute(TEST_INSTANCE_ID, [], _start_events())


@pytest.mark.parametrize("thread_pool", [False, True])
def test_async_bridge_resources_are_released(thread_pool: bool):
    references: dict[str, ReferenceType[Any]] = {}
    cleaned: list[str] = []

    def episode(ctx: task.OrchestrationContext):
        loop = asyncio.new_event_loop()

        async def resource():
            try:
                yield
            finally:
                await asyncio.sleep(0)
                cleaned.append("asyncgen closed")

        async def body():
            assert ctx.instance_id == TEST_INSTANCE_ID
            opened_resource = resource()
            await anext(opened_resource)
            references["asyncgen"] = ref(opened_resource)
            try:
                async with anyio.create_task_group():
                    await asyncio.Future()
            finally:
                await asyncio.sleep(0)
                cleaned.append("closed")

        root = loop.create_task(body())
        references["loop"] = ref(loop)
        references["root"] = ref(root)
        loop.run_until_complete(asyncio.sleep(0))
        try:
            yield ctx.create_timer(timedelta(seconds=5))
        finally:
            root.cancel()
            loop.run_until_complete(asyncio.gather(root, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    def orchestrator(ctx: task.OrchestrationContext, _: Any):
        generator = episode(ctx)
        references["context"] = ref(ctx)
        references["generator"] = ref(generator)
        return generator

    def dispatch():
        result = _executor(orchestrator).execute(TEST_INSTANCE_ID, [], _start_events())
        assert len(result.actions) == 1
        assert result.actions[0].HasField("createTimer")
        assert sorted(cleaned) == ["asyncgen closed", "closed"]
        loop = references["loop"]()
        if loop is not None:
            assert loop.is_closed()
        root = references["root"]()
        if root is not None:
            assert root.done()

    try:
        if thread_pool:
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(dispatch).result()
        else:
            dispatch()
        gc.collect()
        assert set(references) == {"context", "generator", "loop", "root", "asyncgen"}
        assert all(weak_reference() is None for weak_reference in references.values())
    finally:
        weak_generator = references.get("generator")
        generator = weak_generator() if weak_generator else None
        if generator is not None:
            generator.close()
