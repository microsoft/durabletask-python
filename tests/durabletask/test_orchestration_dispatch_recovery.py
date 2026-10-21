# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# pyright: reportPrivateUsage=false

import asyncio
import json
import logging
import sys
from threading import Event
from typing import Any, Generator
from unittest.mock import Mock

import pytest

from durabletask import task, worker
from durabletask.entities import EntityInstanceId
from durabletask.internal import exceptions, helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.serialization import JsonDataConverter

INSTANCE_ID = "dispatch-recovery"
LOGGER = logging.getLogger("tests.dispatch_recovery")


class TeardownExit(BaseException):
    pass


def _request() -> pb.OrchestratorRequest:
    return pb.OrchestratorRequest(instanceId=INSTANCE_ID, newEvents=[
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event("orchestrator", INSTANCE_ID),
    ])


def _executor(orchestrator: task.Orchestrator[Any, Any]) -> worker._OrchestrationExecutor:
    registry = worker._Registry()
    registry.add_named_orchestrator("orchestrator", orchestrator)
    return worker._OrchestrationExecutor(registry, LOGGER, JsonDataConverter())


@pytest.mark.parametrize("during_teardown", [False, True])
def test_unexpected_base_exception_fails_orchestration_without_reprocessing(
        during_teardown: bool, caplog: pytest.LogCaptureFixture):
    error = TeardownExit("deterministic dispatch failure")

    def orchestrator(ctx: task.OrchestrationContext, _: Any) -> Generator[task.Task[Any], Any, None]:
        if not during_teardown:
            raise error
        try:
            ctx.set_custom_status("waiting")
            yield ctx.call_activity("work")
        finally:
            raise error

    with worker.TaskHubGrpcWorker(logger=LOGGER) as host:
        host._registry = _executor(orchestrator)._registry
        stub = Mock()
        reset = Mock()
        with caplog.at_level(logging.ERROR, logger=LOGGER.name):
            host._execute_orchestrator(
                _request(), stub, "original-token", _on_outcome_uncertain=reset)

    stub.CompleteOrchestratorTask.assert_called_once()
    stub.AbandonTaskOrchestratorWorkItem.assert_not_called()
    reset.assert_not_called()
    response: pb.OrchestratorResponse = stub.CompleteOrchestratorTask.call_args.args[0]
    assert response.instanceId == INSTANCE_ID
    assert response.completionToken == "original-token"
    completion = next(a.completeOrchestration for a in response.actions if a.HasField("completeOrchestration"))
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert completion.failureDetails.errorType.endswith(".TeardownExit")
    assert completion.failureDetails.errorMessage == str(error)
    assert "raise error" in completion.failureDetails.stackTrace.value
    if during_teardown:
        assert len(response.actions) == 1
        assert not response.HasField("customStatus")
    assert any(record.exc_info and record.exc_info[1] is error for record in caplog.records)


def test_unexpected_cleanup_does_not_replace_existing_failure():
    def orchestrator(ctx: task.OrchestrationContext, _: Any) -> Generator[task.Task[Any], Any, None]:
        try:
            yield ctx.call_activity("expected")
        finally:
            raise TeardownExit("secondary cleanup failure")

    request = _request()
    request.newEvents.append(helpers.new_task_scheduled_event(1, "wrong"))
    result = _executor(orchestrator).execute(INSTANCE_ID, [], request.newEvents)

    completion = next(a.completeOrchestration for a in result.actions if a.HasField("completeOrchestration"))
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert "NonDeterminismError" in completion.failureDetails.errorType
    assert "secondary cleanup failure" not in completion.failureDetails.errorMessage


@pytest.mark.parametrize("release_before_wait", [False, True])
def test_unexpected_teardown_failure_preserves_entity_lock_releases(release_before_wait: bool):
    entity_id = EntityInstanceId("Counter", "key")

    def orchestrator(ctx: task.OrchestrationContext, _: Any) -> Generator[task.Task[Any], Any, None]:
        acquired = False
        try:
            with (yield ctx.lock_entities([entity_id])):
                acquired = True
                if not release_before_wait:
                    yield ctx.wait_for_external_event("ready")
            yield ctx.wait_for_external_event("ready")
        finally:
            if acquired:
                ctx.set_custom_status("discarded")
                raise TeardownExit("cleanup failed with acquired lock")

    executor = _executor(orchestrator)
    request = _request()
    initial = executor.execute(INSTANCE_ID, [], request.newEvents)
    lock_id = initial.actions[0].sendEntityMessage.entityLockRequested.criticalSectionId
    result = executor.execute(INSTANCE_ID, request.newEvents, [
        helpers.new_event_sent_event(1, str(entity_id), json.dumps({"id": lock_id})),
        helpers.new_event_raised_event(lock_id, None),
    ])

    assert len(result.actions) == 2
    release = result.actions[0].sendEntityMessage.entityUnlockSent
    assert release.criticalSectionId == lock_id
    assert release.targetInstanceId.value == str(entity_id)
    assert release.parentInstanceId.value == INSTANCE_ID
    assert result.actions[1].completeOrchestration.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert result.actions[0].id < result.actions[1].id
    assert result.encoded_custom_status is None


def test_cleanup_does_not_replace_pending_control_flow_error(monkeypatch: pytest.MonkeyPatch):
    primary = asyncio.CancelledError("original cancellation")

    def orchestrator(ctx: task.OrchestrationContext, _: Any):
        try:
            yield ctx.wait_for_external_event("ready")
        finally:
            raise TeardownExit("secondary cleanup failure")

    executor = _executor(orchestrator)
    process_event = executor.process_event

    def process_then_cancel(ctx: worker._RuntimeOrchestrationContext, event: pb.HistoryEvent):
        process_event(ctx, event)
        if event.HasField("executionStarted"):
            raise primary

    monkeypatch.setattr(executor, "process_event", process_then_cancel)
    with pytest.raises(asyncio.CancelledError) as caught:
        executor.execute(INSTANCE_ID, [], _request().newEvents)
    assert caught.value is primary


@pytest.mark.parametrize("error", [TeardownExit("unexpected"), SystemExit("stop"), asyncio.CancelledError("cancel")])
def test_caller_exception_state_does_not_suppress_teardown_outcome(error: BaseException):
    def orchestrator(ctx: task.OrchestrationContext, _: Any):
        try:
            yield ctx.wait_for_external_event("ready")
        finally:
            raise error

    try:
        raise ValueError("unrelated caller exception")
    except ValueError:
        executor = _executor(orchestrator)
        if isinstance(error, SystemExit):
            with pytest.raises(SystemExit) as caught:
                executor.execute(INSTANCE_ID, [], _request().newEvents)
            assert caught.value is error
        else:
            result = executor.execute(INSTANCE_ID, [], _request().newEvents)
            if isinstance(error, TeardownExit):
                assert result.actions[0].completeOrchestration.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
            else:
                assert result.actions == []


def _control_flow_errors() -> list[BaseException]:
    errors: list[BaseException] = [
        asyncio.CancelledError("cancel"),
        SystemExit("exit"),
        KeyboardInterrupt("interrupt"),
        GeneratorExit("close"),
    ]
    if sys.version_info >= (3, 11):
        errors.extend([
            BaseExceptionGroup("grouped cancellation", [asyncio.CancelledError("cancel")]),
            BaseExceptionGroup("mixed", [ValueError("ordinary"), asyncio.CancelledError("cancel")]),
            BaseExceptionGroup("nested", [
                TeardownExit("unexpected"),
                BaseExceptionGroup("inner", [SystemExit("exit")]),
            ]),
        ])
    return errors


@pytest.mark.parametrize("error", _control_flow_errors(), ids=lambda error: str(error))
@pytest.mark.parametrize("during_teardown", [False, True])
def test_control_flow_propagates_after_same_token_abandonment(
        error: BaseException, during_teardown: bool):
    if during_teardown and isinstance(error, (asyncio.CancelledError, GeneratorExit)):
        pytest.skip("Direct teardown cancellation and GeneratorExit retain their existing cleanup semantics.")

    def orchestrator(ctx: task.OrchestrationContext, _: Any):
        if not during_teardown:
            raise error
        try:
            yield ctx.wait_for_external_event("ready")
        finally:
            raise error

    with worker.TaskHubGrpcWorker(logger=LOGGER) as host:
        host._registry = _executor(orchestrator)._registry
        stub = Mock()
        reset = Mock()
        with pytest.raises(type(error)) as caught:
            host._execute_orchestrator(
                _request(), stub, "original-token", _on_outcome_uncertain=reset)

    assert caught.value is error
    stub.CompleteOrchestratorTask.assert_not_called()
    stub.AbandonTaskOrchestratorWorkItem.assert_called_once_with(
        pb.AbandonOrchestrationTaskRequest(completionToken="original-token"))
    reset.assert_not_called()


@pytest.mark.parametrize("stage", ["prepare", "complete", "abandon"])
@pytest.mark.parametrize("error", [RuntimeError("rpc failed"), asyncio.CancelledError("rpc cancelled")])
def test_rpc_uncertainty_resets_stream_without_duplicate_outcomes(
        stage: str, error: BaseException, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture):
    with worker.TaskHubGrpcWorker(logger=LOGGER) as host:
        prepare = Mock(return_value=pb.OrchestratorResponse(
            instanceId=INSTANCE_ID, completionToken="original-token"))
        monkeypatch.setattr(host, "_get_orchestrator_response", prepare)
        stub = Mock()
        reset = Mock()
        if stage == "prepare":
            prepare.side_effect = error
            stub.AbandonTaskOrchestratorWorkItem.side_effect = RuntimeError("abandon failed")
        elif stage == "complete":
            stub.CompleteOrchestratorTask.side_effect = error
        else:
            prepare.side_effect = exceptions.AbandonOrchestrationError()
            stub.AbandonTaskOrchestratorWorkItem.side_effect = error

        with caplog.at_level(logging.ERROR, logger=LOGGER.name):
            if isinstance(error, Exception):
                host._execute_orchestrator(
                    _request(), stub, "original-token", _on_outcome_uncertain=reset)
            else:
                with pytest.raises(type(error)) as caught:
                    host._execute_orchestrator(
                        _request(), stub, "original-token", _on_outcome_uncertain=reset)
                assert caught.value is error

    assert stub.CompleteOrchestratorTask.call_count == (stage == "complete")
    assert stub.AbandonTaskOrchestratorWorkItem.call_count == (stage != "complete")
    if stage != "complete":
        assert stub.AbandonTaskOrchestratorWorkItem.call_args.args[0].completionToken == "original-token"
    reset.assert_called_once_with()
    assert any("uncertain outcome" in record.message for record in caplog.records)
    assert any(record.exc_info and record.exc_info[1] is error for record in caplog.records)


@pytest.mark.parametrize("secondary", [RuntimeError("secondary"), asyncio.CancelledError("secondary")])
def test_failed_recovery_preserves_original_control_flow(
        secondary: BaseException, monkeypatch: pytest.MonkeyPatch):
    original = asyncio.CancelledError("primary")
    with worker.TaskHubGrpcWorker(logger=LOGGER) as host:
        monkeypatch.setattr(host, "_get_orchestrator_response", Mock(side_effect=original))
        stub = Mock()
        stub.AbandonTaskOrchestratorWorkItem.side_effect = secondary
        reset = Mock(side_effect=TeardownExit("reset failure"))
        with pytest.raises(asyncio.CancelledError) as caught:
            host._execute_orchestrator(
                _request(), stub, "original-token", _on_outcome_uncertain=reset)
    assert caught.value is original
    reset.assert_called_once()


def test_recovery_process_control_is_not_swallowed(monkeypatch: pytest.MonkeyPatch):
    stop = SystemExit("stop during recovery")
    with worker.TaskHubGrpcWorker(logger=LOGGER) as host:
        monkeypatch.setattr(host, "_get_orchestrator_response", Mock(side_effect=ValueError("preparation")))
        stub = Mock()
        stub.AbandonTaskOrchestratorWorkItem.side_effect = stop
        reset = Mock()
        with pytest.raises(SystemExit) as caught:
            host._execute_orchestrator(
                _request(), stub, "original-token", _on_outcome_uncertain=reset)
    assert caught.value is stop
    reset.assert_called_once()


@pytest.mark.parametrize("reset_error", _control_flow_errors(), ids=lambda error: str(error))
def test_reset_control_flow_is_not_hidden_by_ordinary_abandonment_error(
        reset_error: BaseException, monkeypatch: pytest.MonkeyPatch):
    with worker.TaskHubGrpcWorker(logger=LOGGER) as host:
        monkeypatch.setattr(host, "_get_orchestrator_response", Mock(side_effect=ValueError("preparation")))
        stub = Mock()
        stub.AbandonTaskOrchestratorWorkItem.side_effect = RuntimeError("abandonment failed")
        reset = Mock(side_effect=reset_error)
        with pytest.raises(type(reset_error)) as caught:
            host._execute_orchestrator(
                _request(), stub, "original-token", _on_outcome_uncertain=reset)
    assert caught.value is reset_error
    reset.assert_called_once()


def test_failure_details_do_not_pass_base_exception_to_public_properties_provider():
    provider = Mock()
    error = TeardownExit("unexpected")
    details = helpers.new_failure_details(error, provider, LOGGER)
    assert details.errorType.endswith(".TeardownExit")
    assert details.errorMessage == "unexpected"
    provider.get_exception_properties.assert_not_called()


@pytest.mark.skipif(sys.version_info < (3, 11), reason="Exception groups require Python 3.11+.")
def test_grouped_unexpected_failure_keeps_nested_reasons():
    if sys.version_info >= (3, 11):
        def orchestrator(ctx: task.OrchestrationContext, _: Any):
            try:
                yield ctx.wait_for_external_event("ready")
            finally:
                raise BaseExceptionGroup("teardown", [TeardownExit("root cause"), ValueError("second cause")])

        result = _executor(orchestrator).execute(INSTANCE_ID, [], _request().newEvents)
        failure = result.actions[0].completeOrchestration.failureDetails
        assert failure.errorType.endswith(".BaseExceptionGroup")
        assert "TeardownExit: root cause" in failure.stackTrace.value
        assert "ValueError: second cause" in failure.stackTrace.value


@pytest.mark.asyncio
async def test_cancelling_executor_await_does_not_abandon_running_dispatch():
    started = Event()
    finish = Event()

    def orchestrator(ctx: task.OrchestrationContext, _: Any):
        started.set()
        assert finish.wait(timeout=5)
        return "finished"

    with worker.TaskHubGrpcWorker(logger=LOGGER) as host:
        host._registry = _executor(orchestrator)._registry
        manager = host._async_worker_manager
        queue: asyncio.Queue[Any] = asyncio.Queue()
        semaphore = asyncio.Semaphore(0)
        queue.put_nowait(None)
        stub = Mock()
        reset = Mock()

        def dispatch():
            host._execute_orchestrator(
                _request(), stub, "original-token", _on_outcome_uncertain=reset)

        running = asyncio.create_task(manager._process_work_item(
            semaphore, queue, dispatch, host._cancel_orchestrator, (), {}))
        try:
            assert await asyncio.to_thread(started.wait, 2)
            running.cancel()
            with pytest.raises(asyncio.CancelledError):
                await running
            stub.CompleteOrchestratorTask.assert_not_called()
            stub.AbandonTaskOrchestratorWorkItem.assert_not_called()
            reset.assert_not_called()
            await asyncio.wait_for(queue.join(), timeout=2)
            await asyncio.wait_for(semaphore.acquire(), timeout=2)
        finally:
            finish.set()
            manager.thread_pool.shutdown(wait=True)

    stub.CompleteOrchestratorTask.assert_called_once()
    stub.AbandonTaskOrchestratorWorkItem.assert_not_called()
    reset.assert_not_called()


@pytest.mark.asyncio
async def test_base_exception_recovery_keeps_queue_and_semaphore_live():
    with worker.TaskHubGrpcWorker(logger=LOGGER) as host:
        manager = host._async_worker_manager
        queue: asyncio.Queue[Any] = asyncio.Queue()
        semaphore = asyncio.Semaphore(1)
        stub = Mock()
        error = asyncio.CancelledError("dispatch cancelled")

        def orchestrator(ctx: task.OrchestrationContext, _: Any):
            raise error

        host._registry = _executor(orchestrator)._registry
        queue.put_nowait(None)
        await semaphore.acquire()
        with pytest.raises(asyncio.CancelledError):
            await manager._process_work_item(
                semaphore, queue, host._execute_orchestrator, host._cancel_orchestrator,
                (_request(), stub, "original-token"), {})
        await asyncio.wait_for(queue.join(), timeout=2)
        await asyncio.wait_for(semaphore.acquire(), timeout=2)

        def healthy(ctx: task.OrchestrationContext, _: Any):
            return "healthy"

        host._registry = _executor(healthy)._registry
        queue.put_nowait(None)
        await manager._process_work_item(
            semaphore, queue, host._execute_orchestrator, host._cancel_orchestrator,
            (_request(), stub, "next-token"), {})
        await asyncio.wait_for(queue.join(), timeout=2)
        await asyncio.wait_for(semaphore.acquire(), timeout=2)
        semaphore.release()
        manager.thread_pool.shutdown(wait=True)

    stub.AbandonTaskOrchestratorWorkItem.assert_called_once()
    stub.CompleteOrchestratorTask.assert_called_once()
    assert stub.CompleteOrchestratorTask.call_args.args[0].completionToken == "next-token"
