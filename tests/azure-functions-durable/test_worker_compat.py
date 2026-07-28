# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for :class:`DurableFunctionsWorker`.

The worker is the host-driven execution engine: it decodes the base64 protobuf
work item supplied by the Durable Functions host extension, registers the user
function, drives the inherited durabletask executor against an in-memory null
stub, and returns the base64-encoded protobuf response. These tests exercise
that path end-to-end without a sidecar or gRPC channel.
"""

import base64
import json
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

import durabletask.internal.helpers as helpers
import durabletask.internal.orchestrator_service_pb2 as pb

from azure.durable_functions.worker import DurableFunctionsWorker

TEST_INSTANCE_ID = "inst-123"


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
