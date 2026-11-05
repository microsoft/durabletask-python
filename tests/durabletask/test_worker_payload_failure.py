# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Permanent response payload failures complete activities and entity operations."""

import asyncio
from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from durabletask import client, entities, task, worker
from durabletask.internal.exceptions import PayloadValidationError
from durabletask.internal import helpers as ph
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.testing import create_test_backend
from tests.durabletask._port_utils import find_free_port
from tests.durabletask.test_large_payload import FakePayloadStore


@pytest.fixture(params=["path", "size"])
def permanent_store(request):
    if request.param == "size":
        yield FakePayloadStore(threshold_bytes=100, max_stored_payload_bytes=200), "PayloadSizeExceededError"
    else:
        pytest.importorskip("azure.storage.blob")
        from durabletask.extensions.azure_blob_payloads import BlobPayloadStore, BlobPayloadStoreOptions

        store = BlobPayloadStore(BlobPayloadStoreOptions(
            account_url="https://example.blob.core.windows.net", threshold_bytes=100))
        try:
            yield store, "PayloadValidationError"
        finally:
            store.close()
            asyncio.run(store.close_async())


def _activity_request(instance_id="."):
    return pb.ActivityRequest(
        name="produce", taskId=42,
        orchestrationInstance=pb.OrchestrationInstance(instanceId=instance_id),
        input=ph.get_string_value('"small"'),
    )


def _entity_request(legacy, operations, state="1", instance_id="@counter@folder/../key"):
    if legacy:
        return pb.EntityBatchRequest(
            instanceId=instance_id, entityState=ph.get_string_value(state),
            operations=[pb.OperationRequest(requestId=str(i), operation=op) for i, op in enumerate(operations)],
        )
    return pb.EntityRequest(
        instanceId=instance_id, entityState=ph.get_string_value(state),
        operationRequests=[pb.HistoryEvent(entityOperationCalled=pb.EntityOperationCalledEvent(
            requestId=str(i), operation=op, parentInstanceId=ph.get_string_value("caller"),
        )) for i, op in enumerate(operations)],
    )


def test_activity_payload_failure_completes_once(permanent_store):
    store, error_type = permanent_store
    executed = []
    stub = MagicMock()

    def produce(ctx, value):
        executed.append(value)
        return "x" * 1024

    with worker.TaskHubGrpcWorker(payload_store=store) as host:
        host.add_activity(produce)
        host._on_activity_execution_completed = MagicMock()
        host._execute_activity(_activity_request(), stub, "token")
        host._on_activity_execution_completed.assert_called_once()

    assert executed == ["small"]
    stub.CompleteActivityTask.assert_called_once()
    stub.AbandonTaskActivityWorkItem.assert_not_called()
    response = stub.CompleteActivityTask.call_args.args[0]
    assert response.instanceId == "." and response.taskId == 42 and response.completionToken == "token"
    assert not response.HasField("result")
    assert response.failureDetails.errorType == f"durabletask.internal.exceptions.{error_type}"
    assert response.failureDetails.isNonRetriable
    assert not response.failureDetails.HasField("stackTrace")


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("mode", ["result", "state", "action"])
def test_entity_payload_failure_restores_batch_state_and_fails_operations(permanent_store, legacy, mode):
    store, error_type = permanent_store
    executed = []
    stub = MagicMock()

    def counter(ctx, _):
        executed.append(ctx.operation)
        if ctx.operation == "fail":
            raise RuntimeError("operation failure")
        ctx.set_state(ctx.get_state(int) + 1)
        ctx.signal_entity(entities.EntityInstanceId("other", "key"), "signal",
                          input="x" * 1024 if ctx.operation == "large" and mode == "action" else "small")
        if ctx.operation == "large":
            if mode == "state":
                ctx.set_state("x" * 1024)
            return "x" * 1024 if mode == "result" else 1
        return 1

    with worker.TaskHubGrpcWorker(payload_store=store) as host:
        host.add_entity(counter)
        request = _entity_request(legacy, ["small", "large", "fail"])
        response = host._execute_entity_batch(request, stub, "token")

    assert executed == ["small", "large", "fail"]
    stub.CompleteEntityTask.assert_called_once_with(response)
    stub.AbandonTaskEntityWorkItem.assert_not_called()
    assert not response.HasField("failureDetails")
    assert response.entityState.value == "1"
    assert not response.actions and response.completionToken == "token"
    assert len(response.results) == 3
    for result in response.results[:2]:
        assert result.HasField("failure")
        assert result.failure.failureDetails.isNonRetriable
        assert result.failure.failureDetails.errorType == f"durabletask.internal.exceptions.{error_type}"
        assert result.failure.HasField("startTimeUtc") and result.failure.HasField("endTimeUtc")
    assert response.results[2].failure.failureDetails.errorType == "builtins.RuntimeError"
    assert [info.requestId for info in response.operationInfos] == ([] if legacy else ["0", "1", "2"])
    assert all(info.responseDestination.instanceId == "caller" for info in response.operationInfos)


def test_entity_payload_failure_preserves_original_state_reference(permanent_store):
    store, _ = permanent_store
    original_state = store.upload(b"1") if isinstance(store, FakePayloadStore) else "blob:v1:durabletask-payloads:seed"
    store.download = MagicMock(return_value=b"1")
    stub = MagicMock()

    def counter(ctx, _):
        ctx.set_state(ctx.get_state(int) + 1)
        return "x" * 1024

    with worker.TaskHubGrpcWorker(payload_store=store) as host:
        host.add_entity(counter)
        response = host._execute_entity_batch(
            _entity_request(False, ["large"], state=original_state), stub, "token")
    store.download.assert_called_once_with(original_state)
    assert response.entityState.value == original_state
    assert response.results[0].HasField("failure")
    assert not response.actions


@pytest.mark.parametrize("surface", ["activity", "entity"])
@pytest.mark.parametrize("error", [OSError("unavailable"), ValueError("store failure")])
def test_transient_response_failure_keeps_existing_retry_behavior(surface, error):
    store = FakePayloadStore(threshold_bytes=100)
    original_upload = store.upload
    store.upload = MagicMock(side_effect=error)
    executed = []
    stub = MagicMock()

    def produce(ctx, _):
        executed.append("called")
        return "x" * 1024

    with worker.TaskHubGrpcWorker(payload_store=store) as host:
        if surface == "activity":
            host.add_activity(produce)
            request = _activity_request("owner")
            execute = host._execute_activity
            complete = stub.CompleteActivityTask
        else:
            host.add_entity(produce)
            request = _entity_request(False, ["large"], instance_id="@produce@key")
            execute = host._execute_entity_batch
            complete = stub.CompleteEntityTask
        execute(request, stub, "first")
        complete.assert_not_called()
        store.upload = original_upload
        execute(request, stub, "retry")
    complete.assert_called_once()
    assert executed == ["called", "called"]


@pytest.mark.parametrize("surface", ["activity", "entity"])
def test_incoming_reference_validation_does_not_complete_as_response_failure(surface):
    store = FakePayloadStore()
    store.download = MagicMock(side_effect=PayloadValidationError("invalid input path"))
    token = f"{store.TOKEN_PREFIX}seed"
    stub = MagicMock()
    with worker.TaskHubGrpcWorker(payload_store=store) as host:
        if surface == "activity":
            request = _activity_request()
            request.input.value = token
            execute = host._execute_activity
        else:
            request = _entity_request(False, ["large"], state=token)
            execute = host._execute_entity_batch
        with pytest.raises(PayloadValidationError, match="input path"):
            execute(request, stub, "token")
    stub.CompleteActivityTask.assert_not_called()
    stub.CompleteEntityTask.assert_not_called()


def test_entity_permanent_failure_discards_partially_externalized_results():
    store = FakePayloadStore(threshold_bytes=10)
    original_upload = store.upload
    uploaded = []

    def upload(data, *, instance_id=None):
        if uploaded:
            raise PayloadValidationError("invalid output path")
        token = original_upload(data, instance_id=instance_id)
        uploaded.append(token)
        return token

    store.upload = upload
    stub = MagicMock()

    def counter(ctx, _):
        ctx.set_state(2)
        return "x" * 50

    with worker.TaskHubGrpcWorker(payload_store=store) as host:
        host.add_entity(counter)
        response = host._execute_entity_batch(
            _entity_request(False, ["first", "second"]), stub, "token")
    assert len(uploaded) == 1
    assert all(result.HasField("failure") for result in response.results)
    assert response.entityState.value == "1"
    assert uploaded[0] not in str(response)
    stub.CompleteEntityTask.assert_called_once_with(response)


@pytest.mark.parametrize("surface", ["activity", "entity"])
def test_permanent_response_failure_reaches_calling_orchestration(permanent_store, surface):
    store, _ = permanent_store
    executed = []

    def produce(ctx, _):
        executed.append("called")
        return "x" * 1024

    def orchestrator(ctx, _):
        if surface == "activity":
            return (yield ctx.call_activity(produce, retry_policy=task.RetryPolicy(
                first_retry_interval=timedelta(milliseconds=1), max_number_of_attempts=3)))
        return (yield ctx.call_entity(entities.EntityInstanceId("produce", "folder/../key"), "large"))

    port = find_free_port()
    address = f"localhost:{port}"
    backend = create_test_backend(port=port)
    try:
        with worker.TaskHubGrpcWorker(host_address=address, payload_store=store) as host:
            host.add_orchestrator(orchestrator)
            host.add_activity(produce)
            host.add_entity(produce)
            host.start()
            with client.TaskHubGrpcClient(host_address=address) as caller:
                instance_id = caller.schedule_new_orchestration(orchestrator, instance_id=".")
                state = caller.wait_for_orchestration_completion(instance_id, timeout=10)
        assert state is not None and state.runtime_status == client.OrchestrationStatus.FAILED
        assert executed == ["called"]
    finally:
        backend.stop()
        backend.reset()
