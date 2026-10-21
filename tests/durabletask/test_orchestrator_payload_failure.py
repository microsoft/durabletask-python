# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Permanent payload failures must complete without stranding entity locks."""

import asyncio
import json
import logging
from threading import Event
from unittest.mock import MagicMock

import pytest

from durabletask import client, entities, task, worker
from durabletask.internal import helpers as ph
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.internal.orchestration_entity_context import OrchestrationEntityContext
from durabletask.serialization import JsonDataConverter
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


@pytest.mark.parametrize("call_failed", [False, True])
def test_legacy_multi_lock_release_history_replays(call_failed):
    def orchestrator(ctx, _):
        with (yield ctx.lock_entities(ENTITY_IDS)):
            try:
                yield ctx.call_entity(ENTITY_IDS[0], "get")
            except task.TaskFailedError:
                # Handle the expected failure so replay can verify lock releases.
                pass
        yield ctx.wait_for_external_event("next")
        return "done"

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)
    executor = worker._OrchestrationExecutor(registry, logging.getLogger("test"), JsonDataConverter())
    started = list(_request(name).newEvents)
    first = executor.execute("owner", [], started)
    section = first.actions[0].sendEntityMessage.entityLockRequested.criticalSectionId
    granted = [
        ph.new_event_sent_event(1, str(ENTITY_IDS[0]), json.dumps({"id": section})),
        ph.new_event_raised_event(section, None),
    ]
    second = executor.execute("owner", started, granted)
    request_id = second.actions[0].sendEntityMessage.entityOperationCalled.requestId
    response = {"result": "42"}
    if call_failed:
        response["exceptionType"] = "ValueError"
    called = [
        ph.new_event_sent_event(2, str(ENTITY_IDS[0]), json.dumps({"id": request_id})),
        ph.new_event_raised_event(request_id, json.dumps(response)),
    ]
    third = executor.execute("owner", started + granted, called)
    # Before acquired/available lock tracking was separated, completing a call
    # moved that entity to the end of the shared list used for releases.
    legacy_releases = [(3, str(ENTITY_IDS[1])), (4, str(ENTITY_IDS[0]))]
    assert [
        (action.id, action.sendEntityMessage.entityUnlockSent.targetInstanceId.value)
        for action in third.actions
    ] == legacy_releases
    released = [ph.new_event_sent_event(action_id, target, None) for action_id, target in legacy_releases]
    history = started + granted + called + released
    replay = executor.execute("owner", history, [ph.new_orchestrator_started_event()])
    assert not replay.actions
    assert not replay.entity_lock_releases
    completed = executor.execute("owner", history, [ph.new_event_raised_event("next", None)])
    assert len(completed.actions) == 1
    assert completed.actions[0].id == 5
    assert completed.actions[0].completeOrchestration.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert completed.actions[0].completeOrchestration.result.value == '"done"'


def _timeout_history(executor, name):
    started = list(_request(name).newEvents)
    first = executor.execute("owner", [], started)
    section = first.actions[0].sendEntityMessage.entityLockRequested.criticalSectionId
    granted = [
        ph.new_event_sent_event(1, str(ENTITY_IDS[0]), json.dumps({"id": section})),
        ph.new_event_raised_event(section, None),
    ]
    second = executor.execute("owner", started, granted)
    call = second.actions[0].sendEntityMessage.entityOperationCalled
    pending = ph.new_event_sent_event(2, str(ENTITY_IDS[0]), json.dumps({"id": call.requestId}))
    return started + granted + [pending], section


def test_legacy_pending_call_timeout_history_replays_through_activity():
    def orchestrator(ctx, _):
        with (yield ctx.lock_entities(ENTITY_IDS)):
            call = ctx.call_entity(ENTITY_IDS[0], "get")
            timeout = ctx.wait_for_external_event("timeout")
            yield task.when_any([call, timeout])
        return (yield ctx.call_activity("after_timeout"))

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)
    executor = worker._OrchestrationExecutor(registry, logging.getLogger("test"), JsonDataConverter())
    history, _ = _timeout_history(executor, name)
    timed_out = ph.new_event_raised_event("timeout", None)
    result = executor.execute("owner", history, [timed_out])
    assert [action.id for action in result.actions] == [3, 4]
    assert result.actions[0].sendEntityMessage.entityUnlockSent.targetInstanceId.value == str(ENTITY_IDS[1])
    assert result.actions[1].scheduleTask.name == "after_timeout"
    assert [release.entityUnlockSent.targetInstanceId.value for release in result.entity_lock_releases] == [
        str(ENTITY_IDS[0]),
    ]

    # Literal history from the old emitter: only b was released, so the next
    # activity kept ID 4 even though the call to a was still outstanding.
    history += [
        timed_out,
        ph.new_event_sent_event(3, str(ENTITY_IDS[1]), None),
        ph.new_task_scheduled_event(4, "after_timeout"),
    ]
    assert not executor.execute("owner", history, [ph.new_orchestrator_started_event()]).actions
    completed = executor.execute("owner", history, [ph.new_task_completed_event(4, '"done"')])
    assert len(completed.actions) == 1
    assert completed.actions[0].id == 5
    assert completed.actions[0].completeOrchestration.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert completed.actions[0].completeOrchestration.result.value == '"done"'


@pytest.mark.parametrize("mode", ["result", "status", "activity"])
@pytest.mark.parametrize("replay_exit", [False, True])
def test_payload_failure_after_normal_pending_call_exit_releases_all_locks(mode, replay_exit):
    def orchestrator(ctx, _):
        with (yield ctx.lock_entities(ENTITY_IDS)):
            call = ctx.call_entity(ENTITY_IDS[0], "get")
            yield task.when_any([call, ctx.wait_for_external_event("timeout")])
        if replay_exit:
            yield ctx.wait_for_external_event("fail")
        if mode == "result":
            return LARGE_PAYLOAD
        if mode == "status":
            ctx.set_custom_status(LARGE_PAYLOAD)
        yield ctx.call_activity("after_timeout", input=LARGE_PAYLOAD if mode == "activity" else None)
        return None

    store = FakePayloadStore(threshold_bytes=100, max_stored_payload_bytes=200)
    stub = MagicMock()
    with worker.TaskHubGrpcWorker(payload_store=store) as w:
        w.add_orchestrator(orchestrator)
        executor = worker._OrchestrationExecutor(w._registry, logging.getLogger("test"), JsonDataConverter())
        history, section = _timeout_history(executor, "orchestrator")
        events = [ph.new_event_raised_event("timeout", None)]
        if replay_exit:
            exited = executor.execute("owner", history, events)
            assert len(exited.actions) == 1
            assert exited.actions[0].id == 3
            history += events + [ph.new_event_sent_event(3, str(ENTITY_IDS[1]), None)]
            events = [ph.new_event_raised_event("fail", None)]
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId="owner", pastEvents=history,
            newEvents=events,
        ), stub, "failure-token")

    response = stub.CompleteOrchestratorTask.call_args.args[0]
    assert response.completionToken == "failure-token"
    assert len(response.actions) == (2 if replay_exit else 3)
    if not replay_exit:
        assert response.actions[0].id == 3
    releases = [action.sendEntityMessage.entityUnlockSent for action in response.actions[:-1]]
    expected_targets = [str(ENTITY_IDS[0])] if replay_exit else [str(ENTITY_IDS[1]), str(ENTITY_IDS[0])]
    assert [release.targetInstanceId.value for release in releases] == expected_targets
    assert all(release.parentInstanceId.value == "owner" for release in releases)
    assert all(release.criticalSectionId == section for release in releases)
    completion = response.actions[-1].completeOrchestration
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert completion.failureDetails.isNonRetriable
    assert not response.HasField("customStatus")


def test_release_snapshot_retains_pending_locks_after_completed_calls():
    entity_ctx = OrchestrationEntityContext("owner")
    entity_ctx.emit_acquire_message("section", ENTITY_IDS)
    entity_ctx.complete_acquire("section")
    assert entity_ctx.validate_operation_transition(ENTITY_IDS[0], False) == (True, "")
    entity_ctx.recover_lock_after_call(ENTITY_IDS[0])
    assert entity_ctx.validate_operation_transition(ENTITY_IDS[1], False) == (True, "")
    releases = list(entity_ctx.get_lock_release_messages())
    assert [release.entityUnlockSent.targetInstanceId.value for release in releases] == [
        str(ENTITY_IDS[0]), str(ENTITY_IDS[1]),
    ]
    assert all(release.entityUnlockSent.parentInstanceId.value == "owner" for release in releases)
    assert all(release.entityUnlockSent.criticalSectionId == "section" for release in releases)
    assert entity_ctx.available_locks == [ENTITY_IDS[0]]
    assert entity_ctx.critical_section_locks == [ENTITY_IDS[0]]
    assert entity_ctx.is_inside_critical_section


@pytest.mark.parametrize("legacy_protocol", [False, True])
@pytest.mark.parametrize("overlapping_call", [False, True])
def test_pre_lock_call_can_complete_while_acquisition_is_pending(legacy_protocol, overlapping_call):
    outside_entity = ENTITY_IDS[0] if overlapping_call else entities.EntityInstanceId("counter", "outside")

    def orchestrator(ctx, _):
        pending = ctx.call_entity(outside_entity, "get")
        with (yield ctx.lock_entities(ENTITY_IDS)):
            result = yield pending
        yield ctx.wait_for_external_event("next")
        return result

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)
    executor = worker._OrchestrationExecutor(registry, logging.getLogger("test"), JsonDataConverter())
    started = list(_request(name).newEvents)
    first = executor.execute("owner", [], started)
    call = first.actions[0].sendEntityMessage.entityOperationCalled
    lock = first.actions[1].sendEntityMessage.entityLockRequested
    if legacy_protocol:
        called = [
            ph.new_event_sent_event(1, str(outside_entity), json.dumps({"id": call.requestId})),
            ph.new_event_sent_event(2, str(ENTITY_IDS[0]), json.dumps({"id": lock.criticalSectionId})),
            ph.new_event_raised_event(call.requestId, json.dumps({"result": "42"})),
        ]
        granted = ph.new_event_raised_event(lock.criticalSectionId, None)
    else:
        called = [
            pb.HistoryEvent(eventId=1, entityOperationCalled=call),
            pb.HistoryEvent(eventId=2, entityLockRequested=lock),
            pb.HistoryEvent(entityOperationCompleted=pb.EntityOperationCompletedEvent(
                requestId=call.requestId, output=ph.get_string_value("42"))),
        ]
        granted = pb.HistoryEvent(entityLockGranted=pb.EntityLockGrantedEvent(
            criticalSectionId=lock.criticalSectionId))
    waiting = executor.execute("owner", started, called)
    assert not waiting.actions
    unlocked = executor.execute("owner", started + called, [granted])
    assert [action.id for action in unlocked.actions] == [3, 4]
    assert [
        action.sendEntityMessage.entityUnlockSent.targetInstanceId.value for action in unlocked.actions
    ] == [str(eid) for eid in ENTITY_IDS]
    if legacy_protocol:
        released = [ph.new_event_sent_event(action.id, str(eid), None)
                    for action, eid in zip(unlocked.actions, ENTITY_IDS)]
    else:
        released = [pb.HistoryEvent(eventId=action.id, entityUnlockSent=action.sendEntityMessage.entityUnlockSent)
                    for action in unlocked.actions]
    history = started + called + [granted] + released
    assert not executor.execute("owner", history, [ph.new_orchestrator_started_event()]).actions
    completed = executor.execute("owner", history, [ph.new_event_raised_event("next", None)])
    assert len(completed.actions) == 1
    assert completed.actions[-1].id == 5
    assert completed.actions[-1].completeOrchestration.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert completed.actions[-1].completeOrchestration.result.value == "42"


@pytest.mark.parametrize("mode", ["result", "status", "call", "continue"])
@pytest.mark.parametrize("completed_call", [False, True])
def test_permanent_payload_failure_releases_acquired_locks(mode, completed_call):
    def orchestrator(ctx, _):
        with (yield ctx.lock_entities(list(reversed(ENTITY_IDS)))):
            if completed_call:
                yield ctx.call_entity(ENTITY_IDS[0], "get")
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
        if completed_call:
            w._execute_orchestrator(req, stub, "call-token")
            call_action = stub.CompleteOrchestratorTask.call_args.args[0].actions[0]
            call_message = call_action.sendEntityMessage.entityOperationCalled
            req = pb.OrchestratorRequest(
                instanceId="owner",
                pastEvents=[
                    *req.pastEvents,
                    *req.newEvents,
                    pb.HistoryEvent(eventId=call_action.id, entityOperationCalled=call_message),
                ],
                newEvents=[
                    ph.new_orchestrator_started_event(),
                    pb.HistoryEvent(entityOperationCompleted=pb.EntityOperationCompletedEvent(
                        requestId=call_message.requestId, output=ph.get_string_value("42"))),
                ],
            )
            stub.reset_mock()
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
    release_order = list(reversed(ENTITY_IDS)) if completed_call or mode == "call" else ENTITY_IDS
    assert [release.targetInstanceId.value for release in releases] == [str(eid) for eid in release_order]
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
