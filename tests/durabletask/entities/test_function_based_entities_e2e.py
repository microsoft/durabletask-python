# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
E2E tests for function-based durable entities using the in-memory backend.
"""

import time

import pytest

from durabletask import client, entities, task, worker
from durabletask.testing import create_test_backend

HOST = "localhost:50056"


@pytest.fixture(autouse=True)
def backend():
    """Create an in-memory backend for entity testing."""
    b = create_test_backend(port=50056)
    yield b
    b.stop()
    b.reset()


def test_client_signal_entity_and_custom_name():
    """Test signaling a function-based entity with a custom registration name from the client."""
    invoked = False

    def empty_entity(ctx: entities.EntityContext, _):
        nonlocal invoked  # don't do this in a real app!
        if ctx.operation == "do_nothing":
            invoked = True

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_entity(empty_entity, name="EntityNameCustom")
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        entity_id = entities.EntityInstanceId("EntityNameCustom", "testEntity")
        c.signal_entity(entity_id, "do_nothing")
        time.sleep(2)  # wait for the signal to be processed

    assert invoked


def test_client_get_entity():
    """Test signaling a function-based entity and reading its state via the client."""
    invoked = False

    def empty_entity(ctx: entities.EntityContext, _):
        nonlocal invoked  # don't do this in a real app!
        if ctx.operation == "do_nothing":
            invoked = True
            ctx.set_state(1)

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_entity(empty_entity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        entity_id = entities.EntityInstanceId("empty_entity", "testEntity")
        c.signal_entity(entity_id, "do_nothing")
        time.sleep(2)  # wait for the signal to be processed
        state = c.get_entity(entity_id, include_state=True)
        assert state is not None
        assert state.id == entity_id
        assert state.get_state(int) == 1

    assert invoked


def test_orchestration_signal_entity_and_custom_name():
    """Test signaling a function-based entity with a custom name from an orchestration."""
    invoked = False

    def empty_entity(ctx: entities.EntityContext, _):
        if ctx.operation == "do_nothing":
            nonlocal invoked  # don't do this in a real app!
            invoked = True

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId(
            "EntityNameCustom", f"{ctx.instance_id}_testEntity")
        ctx.signal_entity(entity_id, "do_nothing")

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(empty_entity, name="EntityNameCustom")
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


def test_orchestration_call_entity():
    """Test calling a function-based entity from an orchestration and awaiting the result."""
    invoked = False

    def empty_entity(ctx: entities.EntityContext, _):
        if ctx.operation == "do_nothing":
            nonlocal invoked  # don't do this in a real app!
            invoked = True

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId(
            "empty_entity", f"{ctx.instance_id}_testEntity")
        yield ctx.call_entity(entity_id, "do_nothing")

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(empty_entity)
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


def test_orchestration_call_entity_with_lock():
    """Test calling a function-based entity from an orchestration with entity locking."""
    invoked = False

    def empty_entity(ctx: entities.EntityContext, _):
        if ctx.operation == "do_nothing":
            nonlocal invoked  # don't do this in a real app!
            invoked = True

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId(
            "empty_entity", f"{ctx.instance_id}_testEntity")
        with (yield ctx.lock_entities([entity_id])):
            yield ctx.call_entity(entity_id, "do_nothing")

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(empty_entity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        id = c.schedule_new_orchestration(empty_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

        # Call a second time to ensure the entity is still responsive
        # after being locked and unlocked
        id_2 = c.schedule_new_orchestration(empty_orchestrator)
        state_2 = c.wait_for_orchestration_completion(id_2, timeout=30)

    assert invoked
    assert state is not None
    assert state.name == task.get_name(empty_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_input is None
    assert state.serialized_output is None
    assert state.serialized_custom_status is None

    assert state_2 is not None
    assert state_2.name == task.get_name(empty_orchestrator)
    assert state_2.instance_id == id_2
    assert state_2.failure_details is None
    assert state_2.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state_2.serialized_input is None
    assert state_2.serialized_output is None
    assert state_2.serialized_custom_status is None


def test_orchestration_entity_signals_entity():
    """Test that an entity can signal another entity during an orchestration call."""
    invoked = False

    def empty_entity(ctx: entities.EntityContext, _):
        if ctx.operation == "do_nothing":
            nonlocal invoked  # don't do this in a real app!
            invoked = True
        elif ctx.operation == "signal_other":
            entity_id = entities.EntityInstanceId(
                "empty_entity",
                ctx.entity_id.key.replace("_testEntity", "_otherEntity"))
            ctx.signal_entity(entity_id, "do_nothing")

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId(
            "empty_entity", f"{ctx.instance_id}_testEntity")
        yield ctx.call_entity(entity_id, "signal_other")

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(empty_entity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        id = c.schedule_new_orchestration(empty_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)
        time.sleep(2)  # wait for the entity-to-entity signal to be processed

    assert invoked
    assert state is not None
    assert state.name == task.get_name(empty_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_input is None
    assert state.serialized_output is None
    assert state.serialized_custom_status is None


def test_entity_starts_orchestration():
    """Test that an entity can start a new orchestration."""
    invoked = False

    def empty_entity(ctx: entities.EntityContext, _):
        if ctx.operation == "start_orchestration":
            ctx.schedule_new_orchestration("empty_orchestrator")

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        nonlocal invoked  # don't do this in a real app!
        invoked = True

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(empty_entity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        c.signal_entity(
            entities.EntityInstanceId("empty_entity", "testEntity"),
            "start_orchestration")
        time.sleep(3)  # wait for the signal and orchestration to be processed

    assert invoked


def test_entity_locking_behavior():
    """Test entity locking constraints: cannot signal locked entities or double-call them."""
    def empty_entity(ctx: entities.EntityContext, _):
        pass

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId(
            "empty_entity", f"{ctx.instance_id}_testEntity")
        with (yield ctx.lock_entities([entity_id])):
            # Cannot signal entities that have been locked
            assert pytest.raises(Exception, ctx.signal_entity, entity_id, "do_nothing")
            entity_call_task = ctx.call_entity(entity_id, "do_nothing")
            # Cannot call entities that have been locked and already called
            assert pytest.raises(Exception, ctx.call_entity, entity_id, "do_nothing")
            yield entity_call_task

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(empty_entity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        id = c.schedule_new_orchestration(empty_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(empty_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_input is None
    assert state.serialized_output is None
    assert state.serialized_custom_status is None


def test_entity_unlocks_when_user_code_throws():
    """Test that entities are unlocked when orchestrator user code throws an exception."""
    invoke_count = 0

    def empty_entity(ctx: entities.EntityContext, _):
        nonlocal invoke_count  # don't do this in a real app!
        invoke_count += 1

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId(
            "empty_entity", f"{ctx.instance_id}_testEntity")
        with (yield ctx.lock_entities([entity_id])):
            yield ctx.call_entity(entity_id, "do_nothing")
            raise Exception("Simulated exception")

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(empty_entity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        time.sleep(2)  # wait for initial setup
        id = c.schedule_new_orchestration(empty_orchestrator)
        c.wait_for_orchestration_completion(id, timeout=30)
        id = c.schedule_new_orchestration(empty_orchestrator)
        c.wait_for_orchestration_completion(id, timeout=30)

    assert invoke_count == 2


def test_entity_unlocks_when_user_mishandles_lock():
    """Test that entities are unlocked when the user yields lock but doesn't use context manager."""
    invoke_count = 0

    def empty_entity(ctx: entities.EntityContext, _):
        nonlocal invoke_count  # don't do this in a real app!
        invoke_count += 1

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId(
            "empty_entity", f"{ctx.instance_id}_testEntity")
        yield ctx.lock_entities([entity_id])
        yield ctx.call_entity(entity_id, "do_nothing")

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(empty_entity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        time.sleep(2)  # wait for initial setup
        id = c.schedule_new_orchestration(empty_orchestrator)
        c.wait_for_orchestration_completion(id, timeout=30)
        id = c.schedule_new_orchestration(empty_orchestrator)
        c.wait_for_orchestration_completion(id, timeout=30)

    assert invoke_count == 2


def test_get_entity_not_found():
    """Test that get_entity returns None for a non-existent entity."""
    c = client.TaskHubGrpcClient(host_address=HOST)
    entity_id = entities.EntityInstanceId("counter", "nonexistent")
    metadata = c.get_entity(entity_id, include_state=True)
    assert metadata is None
