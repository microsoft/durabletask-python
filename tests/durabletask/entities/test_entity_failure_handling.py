# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
E2E tests for entity failure handling using the in-memory backend.
"""

import json

import pytest

from durabletask import client, entities, task, worker
from durabletask.testing import create_test_backend

HOST = "localhost:50057"


@pytest.fixture(autouse=True)
def backend():
    """Create an in-memory backend for entity testing."""
    b = create_test_backend(port=50057)
    yield b
    b.stop()
    b.reset()


def test_class_entity_unhandled_failure_fails():
    """Test that an unhandled exception in a class entity causes the orchestration to fail."""
    class FailingEntity(entities.DurableEntity):
        def fail(self, _):
            raise ValueError("Something went wrong!")

    def test_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("FailingEntity", "testEntity")
        yield ctx.call_entity(entity_id, "fail")

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(test_orchestrator)
        w.add_entity(FailingEntity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        id = c.schedule_new_orchestration(test_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(test_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is not None
    assert state.failure_details.error_type == "TaskFailedError"
    assert "Something went wrong!" in state.failure_details.message
    assert state.runtime_status == client.OrchestrationStatus.FAILED


def test_function_entity_unhandled_failure_fails():
    """Test that an unhandled exception in a function entity causes the orchestration to fail."""
    def failing_entity(ctx: entities.EntityContext, _):
        raise ValueError("Something went wrong!")

    def test_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("failing_entity", "testEntity")
        yield ctx.call_entity(entity_id, "fail")

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(test_orchestrator)
        w.add_entity(failing_entity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        id = c.schedule_new_orchestration(test_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(test_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is not None
    assert state.failure_details.error_type == "TaskFailedError"
    assert "Something went wrong!" in state.failure_details.message
    assert state.runtime_status == client.OrchestrationStatus.FAILED


def test_class_entity_handled_failure_succeeds():
    """Test that a handled exception in a class entity allows the orchestration to succeed."""
    class FailingEntity(entities.DurableEntity):
        def fail(self, _):
            raise ValueError("Something went wrong!")

    def test_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("FailingEntity", "testEntity")
        try:
            yield ctx.call_entity(entity_id, "fail")
        except task.TaskFailedError as e:
            return e.details.message

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(test_orchestrator)
        w.add_entity(FailingEntity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        id = c.schedule_new_orchestration(test_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(test_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None

    assert state.serialized_output is not None
    output = json.loads(state.serialized_output)
    assert "Something went wrong!" in output
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED


def test_function_entity_handled_failure_succeeds():
    """Test that a handled exception in a function entity allows the orchestration to succeed."""
    def failing_entity(ctx: entities.EntityContext, _):
        raise ValueError("Something went wrong!")

    def test_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("failing_entity", "testEntity")
        try:
            yield ctx.call_entity(entity_id, "fail")
        except task.TaskFailedError as e:
            return e.details.message

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(test_orchestrator)
        w.add_entity(failing_entity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        id = c.schedule_new_orchestration(test_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(test_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None

    assert state.serialized_output is not None
    output = json.loads(state.serialized_output)
    assert "Something went wrong!" in output
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED


def test_entity_failure_unlocks_entity():
    """Test that an entity failure properly unlocks the entity for subsequent operations."""
    def failing_entity(ctx: entities.EntityContext, _):
        raise ValueError("Something went wrong!")

    def test_orchestrator(ctx: task.OrchestrationContext, _):
        exception_count = 0
        entity_id = entities.EntityInstanceId("failing_entity", "testEntity")
        with (yield ctx.lock_entities([entity_id])):
            try:
                yield ctx.call_entity(entity_id, "fail")
            except task.TaskFailedError:
                exception_count += 1
        try:
            yield ctx.call_entity(entity_id, "fail")
        except task.TaskFailedError:
            exception_count += 1
        return exception_count

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(test_orchestrator)
        w.add_entity(failing_entity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        id = c.schedule_new_orchestration(test_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(test_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None

    assert state.serialized_output is not None
    output = json.loads(state.serialized_output)
    assert output == 2
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
