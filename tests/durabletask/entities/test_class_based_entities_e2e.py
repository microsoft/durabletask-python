# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
E2E tests for class-based durable entities using the in-memory backend.
"""

import time

import pytest

from durabletask import client, entities, task, worker
from durabletask.testing import create_test_backend

HOST = "localhost:50059"


@pytest.fixture(autouse=True)
def backend():
    """Create an in-memory backend for entity testing."""
    b = create_test_backend(port=50059)
    yield b
    b.stop()
    b.reset()


def test_client_signal_class_entity_and_custom_name():
    """Test signaling a class-based entity with a custom registration name from the client."""
    invoked = False

    class EmptyEntity(entities.DurableEntity):
        def do_nothing(self, _):
            nonlocal invoked  # don't do this in a real app!
            invoked = True

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_entity(EmptyEntity, name="EntityNameCustom")
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        entity_id = entities.EntityInstanceId("EntityNameCustom", "testEntity")
        c.signal_entity(entity_id, "do_nothing")
        time.sleep(2)  # wait for the signal to be processed

    assert invoked


def test_client_get_class_entity():
    """Test signaling a class-based entity and reading its state via the client."""
    invoked = False

    class EmptyEntity(entities.DurableEntity):
        def do_nothing(self, _):
            self.set_state(1)
            nonlocal invoked  # don't do this in a real app!
            invoked = True

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_entity(EmptyEntity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        entity_id = entities.EntityInstanceId("EmptyEntity", "testEntity")
        c.signal_entity(entity_id, "do_nothing")
        time.sleep(2)  # wait for the signal to be processed
        state = c.get_entity(entity_id, include_state=True)
        assert state is not None
        assert state.id == entity_id
        assert state.get_state(int) == 1

    assert invoked


def test_orchestration_signal_class_entity_and_custom_name():
    """Test signaling a class-based entity with a custom name from an orchestration."""
    invoked = False

    class EmptyEntity(entities.DurableEntity):
        def do_nothing(self, _):
            nonlocal invoked  # don't do this in a real app!
            invoked = True

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("EntityNameCustom", "testEntity")
        ctx.signal_entity(entity_id, "do_nothing")

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(EmptyEntity, name="EntityNameCustom")
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        id = c.schedule_new_orchestration(empty_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)
        time.sleep(2)  # wait for the signal to be processed

    assert invoked
    assert state is not None
    assert state.name == task.get_name(empty_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_input is None
    assert state.serialized_output is None
    assert state.serialized_custom_status is None


def test_orchestration_call_class_entity():
    """Test calling a class-based entity from an orchestration and awaiting the result."""
    invoked = False

    class EmptyEntity(entities.DurableEntity):
        def do_nothing(self, _):
            nonlocal invoked  # don't do this in a real app!
            invoked = True

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("EmptyEntity", "testEntity")
        yield ctx.call_entity(entity_id, "do_nothing")

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(EmptyEntity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        id = c.schedule_new_orchestration(empty_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert invoked
    assert state is not None
    assert state.name == task.get_name(empty_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_input is None
    assert state.serialized_output is None
    assert state.serialized_custom_status is None
