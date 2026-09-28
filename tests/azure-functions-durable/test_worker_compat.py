# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for :class:`DurableFunctionsWorker`.

The worker is the host-driven execution engine: it decodes the base64 protobuf
work item supplied by the Durable Functions host extension, registers the user
function, drives the inherited durabletask executor against an in-memory null
stub, and returns the base64-encoded protobuf response. These tests exercise
that path end-to-end without a sidecar or gRPC channel.
"""

import asyncio
import base64
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import durabletask.internal.helpers as helpers
import durabletask.internal.orchestrator_service_pb2 as pb

import azure.durable_functions as df
from azure.durable_functions.internal import invocation, payloads
from azure.durable_functions.worker import DurableFunctionsWorker
from durabletask.entities import EntityInstanceId
from durabletask.payload import PayloadStore

TEST_INSTANCE_ID = "inst-123"


def test_configure_large_payloads_reaches_workers(monkeypatch):
    monkeypatch.setattr(payloads, "_payload_store", None)
    assert DurableFunctionsWorker()._payload_store is None
    store = Mock(spec=PayloadStore)
    app = df.DFApp()
    app.configure_large_payloads(payload_store=store)
    app.configure_large_payloads(payload_store=store)
    assert DurableFunctionsWorker()._payload_store is None
    assert payloads.get_transport_payload_store()._store is store
    with pytest.raises(ValueError, match="different payload store"):
        app.configure_large_payloads(payload_store=Mock(spec=PayloadStore))
    assert payloads.get_payload_store() is store
    with pytest.raises(TypeError, match="must be a PayloadStore"):
        app.configure_large_payloads(payload_store=None)


def test_worker_uses_propagate_only_tracing():
    worker = DurableFunctionsWorker()

    assert worker.emit_trace_spans is False


def test_worker_created_before_configuration_hydrates_and_externalizes(monkeypatch, payload_store_factory):
    monkeypatch.setattr(payloads, "_payload_store", None)
    worker = DurableFunctionsWorker()
    store = payload_store_factory()
    df.DFApp().configure_large_payloads(payload_store=store)
    value = {"data": "x" * 200}
    token = store.upload(json.dumps(value).encode())

    def orchestrator(context):
        assert context.get_input() == value
        return value

    encoded = _encode_orchestrator_request("payload-orch", encoded_input=json.dumps(token))
    response = _decode_orchestrator_response(
        worker.execute_orchestration_request(orchestrator, encoded))
    completion = _get_completion_action(response)
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    result_token = json.loads(completion.result.value)
    assert store.is_known_token(result_token)
    assert json.loads(store.download(result_token)) == value


@pytest.mark.parametrize("entity", [False, True])
@pytest.mark.parametrize("storage_failure", [False, True])
@pytest.mark.parametrize("use_async", [False, True])
async def test_worker_preserves_output_error(monkeypatch, payload_store_factory, entity, storage_failure, use_async):
    store = payload_store_factory(max_stored_payload_bytes=150)
    error = OSError("payload storage unavailable")
    if storage_failure:
        store = payload_store_factory()
        monkeypatch.setattr(store, "upload", Mock(side_effect=error))
        monkeypatch.setattr(store, "upload_async", AsyncMock(side_effect=error))
    monkeypatch.setattr(payloads, "_payload_store", store)

    def orchestrator(context):
        return "x" * 200

    def counter(context):
        context.set_state("x" * 200)

    with pytest.raises(OSError if storage_failure else ValueError) as raised:
        worker = DurableFunctionsWorker()
        if entity:
            encoded = _encode_entity_batch_request("@counter@key", "set")
            if use_async:
                await worker.execute_entity_batch_request_async(counter, encoded)
            else:
                worker.execute_entity_batch_request(counter, encoded)
        else:
            encoded = _encode_orchestrator_request("oversized")
            if use_async:
                await worker.execute_orchestration_request_async(orchestrator, encoded)
            else:
                worker.execute_orchestration_request(orchestrator, encoded)
    if storage_failure:
        assert raised.value is error
    else:
        assert "202 bytes" in str(raised.value)
        assert "150 bytes" in str(raised.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("entity", [False, True])
@pytest.mark.parametrize("registered", [False, True])
async def test_async_worker_uses_only_async_storage(monkeypatch, payload_store_factory, entity, registered):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    value = {"data": "x" * 200}
    token = store.upload(json.dumps(value).encode())
    download = store.download
    monkeypatch.setattr(store, "download_async", AsyncMock(side_effect=download))
    monkeypatch.setattr(store, "upload_async", AsyncMock(side_effect=store.upload))
    monkeypatch.setattr(store, "download", Mock(side_effect=AssertionError("sync download")))
    monkeypatch.setattr(store, "upload", Mock(side_effect=AssertionError("sync upload")))
    host_thread = threading.get_ident()
    invocation = SimpleNamespace(invocation_id="worker-id", thread_local_storage=threading.local())

    def check_execution_thread():
        assert threading.get_ident() != host_thread
        if registered:
            assert invocation.thread_local_storage.invocation_id == invocation.invocation_id

    def orchestrator(context):
        check_execution_thread()
        assert context.get_input() == value
        return value

    def counter(context):
        check_execution_thread()
        assert context.get_input() == value
        context.set_state(value)

    worker = DurableFunctionsWorker()
    if registered:
        app = df.DFApp()
        builder = (app.entity_trigger(context_name="ctx")(counter) if entity
                   else app.orchestration_trigger(context_name="ctx")(orchestrator))
        encoded = (_encode_entity_batch_request("@counter@key", "set", json.dumps(token)) if entity
                   else _encode_orchestrator_request("async-payload", json.dumps(token)))
        result = await builder._function._func(ctx=encoded, context=invocation)
    if entity:
        if not registered:
            result = await worker.execute_entity_batch_request_async(
                counter, _encode_entity_batch_request("@counter@key", "set", json.dumps(token)))
        output = _decode_entity_response(result).entityState.value
    else:
        if not registered:
            result = await worker.execute_orchestration_request_async(
                orchestrator, _encode_orchestrator_request("async-payload", json.dumps(token)))
        output = _get_completion_action(_decode_orchestrator_response(result)).result.value
    assert json.loads(download(json.loads(output))) == value
    store.download_async.assert_awaited_once()
    store.upload_async.assert_awaited_once()


@pytest.mark.parametrize("entity", [False, True])
async def test_storage_downloads_do_not_wait_for_execution_thread(monkeypatch, payload_store_factory, entity):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    token = json.dumps(store.upload(b'"input"'))
    loop = asyncio.get_running_loop()
    executing = asyncio.Event()
    downloaded = asyncio.Event()
    release = threading.Event()
    downloads = []

    async def download(reference):
        downloads.append(reference)
        if len(downloads) == 2:
            downloaded.set()
        return store._blobs[reference]

    def execute(context):
        assert context.get_input() == "input"
        loop.call_soon_threadsafe(executing.set)
        assert release.wait(timeout=5)
        return "done"

    monkeypatch.setattr(store, "download_async", download)
    monkeypatch.setattr(store, "download", Mock(side_effect=AssertionError("sync download")))
    worker = DurableFunctionsWorker()
    encoded = (_encode_entity_batch_request("@execute@key", "get", token) if entity
               else _encode_orchestrator_request("execute", token))
    invoke = worker.execute_entity_batch_request_async if entity else worker.execute_orchestration_request_async
    with ThreadPoolExecutor(max_workers=1) as executor:
        monkeypatch.setattr(invocation, "_executor", lambda: executor)
        first = asyncio.create_task(invoke(execute, encoded))
        second = None
        try:
            await asyncio.wait_for(executing.wait(), timeout=5)
            second = asyncio.create_task(invoke(execute, encoded))
            await asyncio.wait_for(downloaded.wait(), timeout=5)
            assert not first.done()
            assert not second.done()
        finally:
            release.set()
            await asyncio.gather(first, *([second] if second is not None else []))
    assert len(downloads) == 2


@pytest.mark.parametrize("entity", [False, True])
async def test_storage_uploads_do_not_hold_execution_thread(monkeypatch, payload_store_factory, entity):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    uploading = asyncio.Event()
    release = asyncio.Event()
    upload = store.upload

    async def upload_async(data, *, instance_id=None):
        uploading.set()
        await release.wait()
        return upload(data, instance_id=instance_id)

    def orchestrator(context):
        return "x" * 200

    def counter(context):
        context.set_state("x" * 200)

    monkeypatch.setattr(store, "upload_async", upload_async)
    monkeypatch.setattr(store, "upload", Mock(side_effect=AssertionError("sync upload")))
    worker = DurableFunctionsWorker()
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=1) as executor:
        monkeypatch.setattr(invocation, "_executor", lambda: executor)
        execution = (worker.execute_entity_batch_request_async(
            counter, _encode_entity_batch_request("@counter@key", "set")) if entity
            else worker.execute_orchestration_request_async(
                orchestrator, _encode_orchestrator_request("upload")))
        pending = asyncio.create_task(execution)
        try:
            await asyncio.wait_for(uploading.wait(), timeout=5)
            assert not pending.done()
            assert await asyncio.wait_for(
                loop.run_in_executor(executor, lambda: "available"), timeout=5) == "available"
        finally:
            release.set()
            result = await pending
    if entity:
        value = _decode_entity_response(result).entityState.value
    else:
        completion = _get_completion_action(_decode_orchestrator_response(result))
        assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
        value = completion.result.value
    assert json.loads(store.download(json.loads(value))) == "x" * 200


@pytest.mark.parametrize("modern_request", [False, True])
@pytest.mark.parametrize("storage_failure", [False, True])
async def test_async_worker_hydrates_entity_envelopes_before_execution(
        monkeypatch, payload_store_factory, modern_request, storage_failure):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    value = '{"data":"hydrated"}'
    token = json.dumps(store.upload(value.encode()))
    error = OSError("nested download failed")
    monkeypatch.setattr(store, "download_async", AsyncMock(side_effect=error if storage_failure else store.download))
    monkeypatch.setattr(store, "download", Mock(side_effect=AssertionError("sync download")))
    request = pb.OrchestratorRequest(instanceId=TEST_INSTANCE_ID)
    request_id = "63c281d7-02d7-412c-9f66-1d6d26a83948"
    sent = request.pastEvents.add(eventId=1)
    if modern_request:
        sent.entityOperationCalled.requestId = request_id
    else:
        sent.eventSent.instanceId = "@counter@one"
        sent.eventSent.name = "op"
        sent.eventSent.input.value = json.dumps({
            "id": request_id, "op": "get", "parent": TEST_INSTANCE_ID, "input": token})
    reply = request.newEvents.add(eventId=2)
    reply.eventRaised.name = request_id
    reply.eventRaised.input.value = json.dumps({"result": token})
    unrelated = request.newEvents.add(eventId=3)
    unrelated.eventRaised.name = "application-event"
    unrelated.eventRaised.input.value = reply.eventRaised.input.value
    worker = DurableFunctionsWorker()
    execution = Mock(return_value=pb.OrchestratorResponse())
    monkeypatch.setattr(worker, "_run_orchestration", execution)
    encoded = base64.b64encode(request.SerializeToString()).decode()
    if storage_failure:
        with pytest.raises(OSError) as raised:
            await worker.execute_orchestration_request_async(Mock(), encoded)
        assert raised.value is error
        execution.assert_not_called()
        store.download_async.assert_awaited_once()
    else:
        await worker.execute_orchestration_request_async(Mock(), encoded)
        hydrated = execution.call_args.args[1]
        assert json.loads(hydrated.newEvents[0].eventRaised.input.value)["result"] == value
        assert hydrated.newEvents[1] == unrelated
        if not modern_request:
            assert hydrated.pastEvents[0] == sent
        store.download_async.assert_awaited_once()


@pytest.mark.parametrize("use_async", [False, True])
async def test_replay_skips_unavailable_historical_entity_input(monkeypatch, payload_store_factory, use_async):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    result_token = store.upload(b'"ok"')
    missing_input = json.dumps("blob:v1:test-container:missing")
    request_id = "63c281d7-02d7-412c-9f66-1d6d26a83948"
    sent = pb.HistoryEvent(eventId=1)
    sent.eventSent.instanceId = "@counter@one"
    sent.eventSent.name = "op"
    sent.eventSent.input.value = json.dumps({
        "id": request_id, "op": "set", "parent": TEST_INSTANCE_ID, "input": missing_input})
    reply = pb.HistoryEvent(eventId=2)
    reply.eventRaised.name = request_id
    reply.eventRaised.input.value = json.dumps({"result": json.dumps(result_token)})
    request = pb.OrchestratorRequest(instanceId=TEST_INSTANCE_ID)
    request.pastEvents.extend([
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event("entity-replay", TEST_INSTANCE_ID),
        sent,
    ])
    request.newEvents.extend([helpers.new_orchestrator_started_event(), reply])
    download = store.download
    monkeypatch.setattr(store, "download", Mock(side_effect=download))
    monkeypatch.setattr(store, "download_async", AsyncMock(side_effect=download))

    def orchestrator(context, value):
        return (yield context.call_entity(EntityInstanceId("counter", "one"), "set", input="x" * 200))

    worker = DurableFunctionsWorker()
    encoded = base64.b64encode(request.SerializeToString()).decode()
    result = (await worker.execute_orchestration_request_async(orchestrator, encoded) if use_async
              else worker.execute_orchestration_request(orchestrator, encoded))
    completion = _get_completion_action(_decode_orchestrator_response(result))
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(completion.result.value) == "ok"
    if use_async:
        store.download_async.assert_awaited_once_with(result_token)
        store.download.assert_not_called()
    else:
        store.download.assert_called_once_with(result_token)
        store.download_async.assert_not_called()


@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("missing_blobs", [False, True])
async def test_replay_skips_scheduled_inputs(monkeypatch, payload_store_factory, use_async, missing_blobs):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    request = pb.OrchestratorRequest(instanceId=TEST_INSTANCE_ID)
    request.pastEvents.extend([
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event("activity-replay", TEST_INSTANCE_ID),
    ])
    for task_id in range(1, 21):
        token = store.upload(json.dumps("x" * 300_000).encode())
        events = request.pastEvents if task_id < 20 else request.newEvents
        events.extend([
            helpers.new_orchestrator_started_event(),
            helpers.new_task_scheduled_event(task_id, "echo", encoded_input=json.dumps(token)),
            helpers.new_task_completed_event(task_id, json.dumps("ok")),
        ])
    if missing_blobs:
        store._blobs.clear()
    download = store.download
    monkeypatch.setattr(store, "download", Mock(side_effect=download))
    monkeypatch.setattr(store, "download_async", AsyncMock(side_effect=download))

    def orchestrator(context, value):
        for index in range(20):
            result = yield context.call_activity("echo", input="x" * 300_000)
            assert result == "ok"
        return "complete"

    worker = DurableFunctionsWorker()
    encoded = base64.b64encode(request.SerializeToString()).decode()
    result = (await worker.execute_orchestration_request_async(orchestrator, encoded) if use_async
              else worker.execute_orchestration_request(orchestrator, encoded))
    completion = _get_completion_action(_decode_orchestrator_response(result))
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(completion.result.value) == "complete"
    store.download.assert_not_called()
    store.download_async.assert_not_called()
    assert request.pastEvents[3].taskScheduled.HasField("input")


def _encode_orchestrator_request(name, encoded_input=None, instance_id=TEST_INSTANCE_ID):
    """Build a base64-encoded ``OrchestratorRequest`` for a single new dispatch."""
    request = pb.OrchestratorRequest(instanceId=instance_id)
    request.newEvents.append(helpers.new_orchestrator_started_event())
    request.newEvents.append(
        helpers.new_execution_started_event(name, instance_id, encoded_input=encoded_input))
    return base64.b64encode(request.SerializeToString()).decode("utf-8")


def _decode_orchestrator_response(encoded):
    response = pb.OrchestratorResponse()
    response.ParseFromString(base64.b64decode(encoded))
    return response


def _get_completion_action(response):
    completion_actions = [a for a in response.actions if a.HasField("completeOrchestration")]
    assert len(completion_actions) == 1
    return completion_actions[0].completeOrchestration


# ---------------------------------------------------------------------------
# execute_orchestration_request
# ---------------------------------------------------------------------------

def test_execute_orchestration_request_completes_and_returns_output():
    def orchestrator(context):
        return {"echo": context.get_input()}

    encoded = _encode_orchestrator_request("orch1", encoded_input=json.dumps({"n": 5}))
    result = DurableFunctionsWorker().execute_orchestration_request(orchestrator, encoded)

    response = _decode_orchestrator_response(result)
    completion = _get_completion_action(response)
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(completion.result.value) == {"echo": {"n": 5}}


def test_execute_orchestration_request_registers_under_event_name():
    """The orchestrator is registered under the name from the ExecutionStarted event."""
    def orchestrator(context):
        return context.instance_id

    encoded = _encode_orchestrator_request("named-orch")
    worker = DurableFunctionsWorker()
    result = worker.execute_orchestration_request(orchestrator, encoded)

    assert "named-orch" in worker._registry.orchestrators
    completion = _get_completion_action(_decode_orchestrator_response(result))
    assert json.loads(completion.result.value) == TEST_INSTANCE_ID


def test_execute_orchestration_request_accepts_context_with_body():
    """A transport context exposing ``.body`` is unwrapped before decoding."""
    def orchestrator(context):
        return "ok"

    encoded = _encode_orchestrator_request("orch-body")
    context = SimpleNamespace(body=encoded)
    result = DurableFunctionsWorker().execute_orchestration_request(orchestrator, context)

    completion = _get_completion_action(_decode_orchestrator_response(result))
    assert json.loads(completion.result.value) == "ok"


def test_execute_orchestration_request_uses_last_execution_started_name():
    """When multiple ExecutionStarted events exist, the last one wins (continue-as-new)."""
    def orchestrator(context):
        return "done"

    request = pb.OrchestratorRequest(instanceId=TEST_INSTANCE_ID)
    request.pastEvents.append(helpers.new_orchestrator_started_event())
    request.pastEvents.append(
        helpers.new_execution_started_event("old-name", TEST_INSTANCE_ID))
    request.newEvents.append(
        helpers.new_execution_started_event("current-name", TEST_INSTANCE_ID))
    encoded = base64.b64encode(request.SerializeToString()).decode("utf-8")

    worker = DurableFunctionsWorker()
    worker.execute_orchestration_request(orchestrator, encoded)
    assert "current-name" in worker._registry.orchestrators


def test_execute_orchestration_request_raises_without_execution_started():
    def orchestrator(context):
        return None

    request = pb.OrchestratorRequest(instanceId=TEST_INSTANCE_ID)
    request.newEvents.append(helpers.new_orchestrator_started_event())
    encoded = base64.b64encode(request.SerializeToString()).decode("utf-8")

    with pytest.raises(ValueError, match="No ExecutionStarted event"):
        DurableFunctionsWorker().execute_orchestration_request(orchestrator, encoded)


def test_execute_orchestration_request_captures_failure():
    def orchestrator(context):
        raise ValueError("boom")

    encoded = _encode_orchestrator_request("failing-orch")
    result = DurableFunctionsWorker().execute_orchestration_request(orchestrator, encoded)

    completion = _get_completion_action(_decode_orchestrator_response(result))
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert "boom" in completion.failureDetails.errorMessage


def test_activity_retry_then_fan_out_uses_distinct_task_ids():
    """Regression test for Azure/azure-functions-durable-python#603."""
    def orchestrator(context):
        options = df.RetryOptions(
            first_retry_interval_in_milliseconds=100,
            max_number_of_attempts=3,
        )
        yield context.call_activity_with_retry("flaky", options)
        tasks = [context.call_activity("square", value) for value in range(13)]
        return (yield context.task_all(tasks))

    worker = DurableFunctionsWorker()
    name = "retry-then-fan-out"
    started_at = datetime(2026, 1, 1)

    def execute(past_events, new_events):
        request = pb.OrchestratorRequest(instanceId=TEST_INSTANCE_ID)
        request.pastEvents.extend(past_events)
        request.newEvents.extend(new_events)
        encoded = base64.b64encode(request.SerializeToString()).decode("utf-8")
        return _decode_orchestrator_response(
            worker.execute_orchestration_request(orchestrator, encoded))

    initial_events = [
        helpers.new_orchestrator_started_event(started_at),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID),
    ]
    response = execute([], initial_events)
    assert len(response.actions) == 1
    assert response.actions[0].id == 1
    assert response.actions[0].scheduleTask.name == "flaky"

    past_events = initial_events + [
        helpers.new_task_scheduled_event(1, "flaky"),
    ]
    failure_events = [
        helpers.new_orchestrator_started_event(started_at),
        helpers.new_task_failed_event(1, ValueError("transient failure")),
    ]
    response = execute(past_events, failure_events)
    assert len(response.actions) == 1
    retry_timer = response.actions[0]
    assert retry_timer.id == 2
    assert retry_timer.HasField("createTimer")

    retry_at = retry_timer.createTimer.fireAt.ToDatetime()
    timer_events = [
        helpers.new_timer_created_event(2, retry_at),
        helpers.new_orchestrator_started_event(retry_at),
        helpers.new_timer_fired_event(2, retry_at),
    ]
    past_events += failure_events
    response = execute(past_events, timer_events)
    assert len(response.actions) == 1
    assert response.actions[0].id == 1
    assert response.actions[0].scheduleTask.name == "flaky"

    retry_completed_events = [
        helpers.new_orchestrator_started_event(retry_at),
        helpers.new_task_scheduled_event(1, "flaky"),
        helpers.new_task_completed_event(1, json.dumps("recovered")),
    ]
    past_events += timer_events
    response = execute(past_events, retry_completed_events)
    assert [action.id for action in response.actions] == list(range(3, 16))
    assert all(action.scheduleTask.name == "square" for action in response.actions)

    fan_out_events = [helpers.new_orchestrator_started_event(retry_at)]
    for task_id in range(3, 16):
        fan_out_events.append(helpers.new_task_scheduled_event(task_id, "square"))
        fan_out_events.append(
            helpers.new_task_completed_event(
                task_id, json.dumps((task_id - 3) ** 2)))

    past_events += retry_completed_events
    response = execute(past_events, fan_out_events)
    completion = _get_completion_action(response)
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(completion.result.value) == [
        value ** 2 for value in range(13)]


def test_execute_orchestration_request_supports_concurrent_reinvocation():
    def orchestrator(context):
        return context.instance_id

    worker = DurableFunctionsWorker()
    encoded = _encode_orchestrator_request("concurrent-orch")
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(
            lambda _: worker.execute_orchestration_request(orchestrator, encoded),
            range(8),
        ))

    assert all(
        json.loads(_get_completion_action(
            _decode_orchestrator_response(result)).result.value) == TEST_INSTANCE_ID
        for result in results
    )


def test_execute_orchestration_request_rejects_different_function_with_same_name():
    def first_orchestrator(context):
        return "first"

    def second_orchestrator(context):
        return "second"

    worker = DurableFunctionsWorker()
    encoded = _encode_orchestrator_request("same-name")
    worker.execute_orchestration_request(first_orchestrator, encoded)

    with pytest.raises(ValueError, match="A 'same-name' orchestrator already exists"):
        worker.execute_orchestration_request(second_orchestrator, encoded)


# ---------------------------------------------------------------------------
# execute_entity_batch_request
# ---------------------------------------------------------------------------

def _encode_entity_batch_request(entity_id, operation, encoded_input=None, encoded_state=None):
    request = pb.EntityBatchRequest(instanceId=entity_id)
    if encoded_state is not None:
        request.entityState.value = encoded_state
    request.operations.append(
        pb.OperationRequest(
            requestId="req-1",
            operation=operation,
            input=helpers.get_string_value(encoded_input)))
    return base64.b64encode(request.SerializeToString()).decode("utf-8")


def _decode_entity_response(encoded):
    result = pb.EntityBatchResult()
    result.ParseFromString(base64.b64decode(encoded))
    return result


def test_execute_entity_batch_request_runs_operation_and_updates_state():
    def counter(context):
        current = context.get_state(initializer=lambda: 0)
        new_value = current + context.get_input()
        context.set_state(new_value)
        context.set_result(new_value)

    counter.__name__ = "counter"

    encoded = _encode_entity_batch_request(
        "@counter@key1", "add", encoded_input=json.dumps(5), encoded_state=json.dumps(10))
    result = DurableFunctionsWorker().execute_entity_batch_request(counter, encoded)

    response = _decode_entity_response(result)
    assert len(response.results) == 1
    assert response.results[0].HasField("success")
    assert json.loads(response.results[0].success.result.value) == 15
    assert json.loads(response.entityState.value) == 15


def test_execute_entity_batch_request_accepts_context_with_body():
    def entity(context):
        context.set_result("handled")

    entity.__name__ = "counter"
    encoded = _encode_entity_batch_request("@counter@key1", "op")
    context = SimpleNamespace(body=encoded)
    result = DurableFunctionsWorker().execute_entity_batch_request(entity, context)

    response = _decode_entity_response(result)
    assert json.loads(response.results[0].success.result.value) == "handled"


def test_execute_entity_batch_request_captures_operation_failure():
    def entity(context):
        raise RuntimeError("entity failed")

    entity.__name__ = "counter"
    encoded = _encode_entity_batch_request("@counter@key1", "op")
    result = DurableFunctionsWorker().execute_entity_batch_request(entity, encoded)

    response = _decode_entity_response(result)
    assert response.results[0].HasField("failure")
    assert "entity failed" in response.results[0].failure.failureDetails.errorMessage


def test_execute_entity_batch_request_supports_concurrent_reinvocation():
    def entity(context):
        context.set_result("handled")

    entity.__durable_entity_name__ = "Counter"
    worker = DurableFunctionsWorker()
    encoded = _encode_entity_batch_request("@counter@key1", "op")
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(
            lambda _: worker.execute_entity_batch_request(entity, encoded),
            range(8),
        ))

    assert all(
        json.loads(_decode_entity_response(
            result).results[0].success.result.value) == "handled"
        for result in results
    )


def test_execute_entity_batch_request_rejects_different_function_with_same_name():
    def first_entity(context):
        context.set_result("first")

    def second_entity(context):
        context.set_result("second")

    first_entity.__durable_entity_name__ = "Counter"
    second_entity.__durable_entity_name__ = "Counter"
    worker = DurableFunctionsWorker()
    encoded = _encode_entity_batch_request("@counter@key1", "op")
    worker.execute_entity_batch_request(first_entity, encoded)

    with pytest.raises(ValueError, match="A 'counter' entity already exists"):
        worker.execute_entity_batch_request(second_entity, encoded)
