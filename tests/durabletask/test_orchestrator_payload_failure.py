# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Permanent payload failures must complete without stranding entity locks."""

import asyncio
from threading import Event
from unittest.mock import MagicMock

import pytest

from durabletask import client, entities, task, worker
from durabletask.internal import helpers as ph
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.internal.orchestration_entity_context import OrchestrationEntityContext
from durabletask.testing import create_test_backend
from tests.durabletask._port_utils import find_free_port
from tests.durabletask.test_large_payload import FakePayloadStore


ENTITY_IDS = [
    entities.EntityInstanceId("counter", "a"),
    entities.EntityInstanceId("counter", "b"),
]
LARGE_PAYLOAD = "x" * 1024


def _fail_payload(ctx: task.OrchestrationContext, mode: str):
    if mode == "status":
        ctx.set_custom_status(LARGE_PAYLOAD)
    elif mode == "call":
        yield ctx.call_entity(ENTITY_IDS[0], "add", LARGE_PAYLOAD)
    elif mode == "continue":
        ctx.continue_as_new(LARGE_PAYLOAD)
    return LARGE_PAYLOAD if mode == "result" else "done"


def _request(name: str) -> pb.OrchestratorRequest:
    return pb.OrchestratorRequest(
        instanceId="owner",
        newEvents=[
            ph.new_orchestrator_started_event(),
            ph.new_execution_started_event(name, "owner"),
        ],
    )


@pytest.mark.parametrize("mode", ["result", "status", "call", "continue"])
def test_permanent_payload_failure_releases_acquired_locks(mode):
    def orchestrator(ctx, _):
        with (yield ctx.lock_entities(list(reversed(ENTITY_IDS)))):
            return (yield from _fail_payload(ctx, mode))

    store = FakePayloadStore(threshold_bytes=100, max_stored_payload_bytes=200)
    provider = MagicMock()
    stub = MagicMock()
    with worker.TaskHubGrpcWorker(payload_store=store, exception_properties_provider=provider) as w:
        w.add_orchestrator(orchestrator)
        first = _request("orchestrator")
        w._execute_orchestrator(first, stub, "first-token")
        first_response = stub.CompleteOrchestratorTask.call_args.args[0]
        lock_action = first_response.actions[0]
        lock_request = lock_action.sendEntityMessage.entityLockRequested
        stub.reset_mock()

        req = pb.OrchestratorRequest(
            instanceId="owner",
            pastEvents=[
                *first.newEvents,
                pb.HistoryEvent(eventId=lock_action.id, entityLockRequested=lock_request),
            ],
            newEvents=[
                ph.new_orchestrator_started_event(),
                pb.HistoryEvent(entityLockGranted=pb.EntityLockGrantedEvent(
                    criticalSectionId=lock_request.criticalSectionId)),
            ],
        )
        w._execute_orchestrator(req, stub, "failure-token")

    stub.CompleteOrchestratorTask.assert_called_once()
    stub.AbandonTaskOrchestratorWorkItem.assert_not_called()
    response = stub.CompleteOrchestratorTask.call_args.args[0]
    assert response.instanceId == "owner"
    assert response.completionToken == "failure-token"
    assert not response.HasField("customStatus")
    assert response.ByteSize() < 1024
    assert len(response.actions) == 3
    releases = [action.sendEntityMessage.entityUnlockSent for action in response.actions[:-1]]
    assert [release.targetInstanceId.value for release in releases] == [str(eid) for eid in ENTITY_IDS]
    assert all(release.parentInstanceId.value == "owner" for release in releases)
    assert all(release.criticalSectionId == lock_request.criticalSectionId for release in releases)
    assert [action.id for action in response.actions] == sorted({action.id for action in response.actions})
    completion = response.actions[-1].completeOrchestration
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert not completion.HasField("result")
    failure = completion.failureDetails
    assert failure.isNonRetriable
    assert failure.errorType == "durabletask.internal.exceptions.PayloadSizeExceededError"
    assert "exceeds the maximum" in failure.errorMessage
    assert not failure.HasField("stackTrace")
    assert not failure.HasField("innerFailure")
    assert not failure.properties
    provider.get_exception_properties.assert_not_called()


@pytest.mark.parametrize("error", [OSError("unavailable"), ValueError("store error"), asyncio.CancelledError()])
def test_storage_failure_and_cancellation_do_not_become_permanent(error):
    def orchestrator(ctx, _):
        return LARGE_PAYLOAD

    store = FakePayloadStore(threshold_bytes=100)
    original_upload = store.upload
    store.upload = MagicMock(side_effect=error)
    stub = MagicMock()
    with worker.TaskHubGrpcWorker(payload_store=store) as w:
        w.add_orchestrator(orchestrator)
        req = _request("orchestrator")
        if isinstance(error, asyncio.CancelledError):
            with pytest.raises(asyncio.CancelledError):
                w._execute_orchestrator(req, stub, "token")
        else:
            w._execute_orchestrator(req, stub, "token")
        stub.CompleteOrchestratorTask.assert_not_called()

        store.upload = original_upload
        w._execute_orchestrator(req, stub, "retry-token")

    completion = stub.CompleteOrchestratorTask.call_args.args[0].actions[-1].completeOrchestration
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert not completion.HasField("failureDetails")


def test_failure_preserves_emitted_releases_before_outstanding_releases(monkeypatch):
    entity_ctx = OrchestrationEntityContext("owner")
    entity_ctx.emit_acquire_message("old-section", ENTITY_IDS)
    emitted = [
        pb.OrchestratorAction(id=index, sendEntityMessage=release)
        for index, release in enumerate(entity_ctx.emit_lock_release_messages(), start=2)
    ]
    entity_ctx.emit_acquire_message("new-section", ENTITY_IDS)
    entity_ctx.complete_acquire("new-section")
    result = worker.ExecutionResults(
        actions=[
            *emitted,
            ph.new_call_entity_action(4, "owner", ENTITY_IDS[0], "add", LARGE_PAYLOAD, "request"),
        ],
        encoded_custom_status="status to discard",
        entity_lock_releases=list(entity_ctx.get_lock_release_messages()),
    )
    monkeypatch.setattr(worker._OrchestrationExecutor, "execute", lambda *args: result)
    store = FakePayloadStore(threshold_bytes=1, max_stored_payload_bytes=200)
    stub = MagicMock()
    with worker.TaskHubGrpcWorker(payload_store=store) as w:
        w._execute_orchestrator(_request("unused"), stub, "token")

    response = stub.CompleteOrchestratorTask.call_args.args[0]
    assert response.actions[:2] == emitted
    releases = [action.sendEntityMessage.entityUnlockSent for action in response.actions[:-1]]
    assert [(release.criticalSectionId, release.targetInstanceId.value) for release in releases] == [
        (section, str(eid)) for section in ("old-section", "new-section") for eid in ENTITY_IDS
    ]
    assert all(release.parentInstanceId.value == "owner" for release in releases)
    assert [action.id for action in response.actions] == [2, 3, 5, 6, 7]
    assert response.actions[-1].completeOrchestration.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert not response.HasField("customStatus")


@pytest.mark.parametrize("mode", ["result", "status", "call"])
def test_next_lock_caller_progresses_after_permanent_payload_failure(monkeypatch, mode):
    locked = Event()
    second_queued = Event()

    def failing(ctx, _):
        with (yield ctx.lock_entities(ENTITY_IDS)):
            locked.set()
            yield ctx.wait_for_external_event("fail")
            return (yield from _fail_payload(ctx, mode))

    def next_caller(ctx, _):
        with (yield ctx.lock_entities(ENTITY_IDS)):
            return (yield ctx.call_entity(ENTITY_IDS[0], "get"))

    def counter(ctx: entities.EntityContext, _):
        return 42

    port = find_free_port()
    host = f"localhost:{port}"
    backend = create_test_backend(port=port)
    original_grant = backend._try_grant_lock

    def observe_grant(pending):
        granted = original_grant(pending)
        if pending.parent_instance_id == "second" and not granted:
            second_queued.set()
        return granted

    monkeypatch.setattr(backend, "_try_grant_lock", observe_grant)
    store = FakePayloadStore(threshold_bytes=100, max_stored_payload_bytes=200)
    try:
        with worker.TaskHubGrpcWorker(host_address=host, payload_store=store) as w:
            w.add_orchestrator(failing)
            w.add_orchestrator(next_caller)
            w.add_entity(counter)
            w.start()
            with client.TaskHubGrpcClient(host_address=host) as c:
                c.schedule_new_orchestration(failing, instance_id="first")
                assert locked.wait(10), "First orchestration did not acquire both locks"
                c.schedule_new_orchestration(next_caller, instance_id="second")
                assert second_queued.wait(10), "Second caller was not blocked by the first"
                c.raise_orchestration_event("first", "fail")
                failed = c.wait_for_orchestration_completion("first", timeout=10)
                completed = c.wait_for_orchestration_completion("second", timeout=10)

        assert failed is not None
        assert failed.runtime_status == client.OrchestrationStatus.FAILED
        assert failed.failure_details is not None
        assert failed.failure_details.error_type == "durabletask.internal.exceptions.PayloadSizeExceededError"
        assert completed is not None
        assert completed.runtime_status == client.OrchestrationStatus.COMPLETED
        assert completed.serialized_output == "42"
    finally:
        backend.stop()
        backend.reset()
