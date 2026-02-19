# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for durable entity support in the in-memory backend.
"""

import json
import pytest
import time

from durabletask.testing import create_test_backend
from durabletask.client import TaskHubGrpcClient, OrchestrationStatus
from durabletask.worker import TaskHubGrpcWorker
from durabletask.entities import DurableEntity, EntityInstanceId

# Use a different port from other tests to avoid conflicts
ENTITY_TEST_PORT = 50053


@pytest.fixture
def backend():
    """Create an in-memory backend for entity testing."""
    backend = create_test_backend(port=ENTITY_TEST_PORT)
    yield backend
    backend.stop()
    backend.reset()


def test_signal_entity_with_function(backend):
    """Test signaling a function-based entity and reading its state."""
    client = TaskHubGrpcClient(host_address=f"localhost:{ENTITY_TEST_PORT}")
    worker = TaskHubGrpcWorker(host_address=f"localhost:{ENTITY_TEST_PORT}")

    def counter(ctx, inp):
        op = ctx.operation
        if op == "add":
            current = ctx.get_state(int, 0)
            ctx.set_state(current + (inp or 1))
        elif op == "get":
            return ctx.get_state(int, 0)

    worker.add_entity(counter)
    worker.start()

    try:
        entity_id = EntityInstanceId("counter", "myCounter")

        # Signal the entity to add values
        client.signal_entity(entity_id, "add", input=5)
        client.signal_entity(entity_id, "add", input=3)

        # Allow time for the entity signals to be processed
        time.sleep(3)

        # Check entity state via GetEntity
        metadata = client.get_entity(entity_id, include_state=True)

        assert metadata is not None
        assert metadata.id == entity_id
        assert metadata.includes_state is True
        # State should be 8 (5 + 3)
        state_value = json.loads(metadata._state)
        assert state_value == 8
    finally:
        worker.stop()


def test_signal_entity_with_class(backend):
    """Test signaling a class-based entity (DurableEntity subclass)."""
    client = TaskHubGrpcClient(host_address=f"localhost:{ENTITY_TEST_PORT}")
    worker = TaskHubGrpcWorker(host_address=f"localhost:{ENTITY_TEST_PORT}")

    class Counter(DurableEntity):
        def add(self, amount: int):
            current = self.get_state(int, 0)
            self.set_state(current + amount)

        def get(self):
            return self.get_state(int, 0)

    worker.add_entity(Counter)
    worker.start()

    try:
        entity_id = EntityInstanceId("Counter", "classCounter")

        # Signal the entity
        client.signal_entity(entity_id, "add", input=10)
        client.signal_entity(entity_id, "add", input=20)

        # Allow processing time
        time.sleep(3)

        # Check state
        metadata = client.get_entity(entity_id, include_state=True)
        assert metadata is not None
        state_value = json.loads(metadata._state)
        assert state_value == 30
    finally:
        worker.stop()


def test_call_entity_from_orchestrator(backend):
    """Test calling an entity from an orchestrator and getting the result."""
    client = TaskHubGrpcClient(host_address=f"localhost:{ENTITY_TEST_PORT}")
    worker = TaskHubGrpcWorker(host_address=f"localhost:{ENTITY_TEST_PORT}")

    class Counter(DurableEntity):
        def add(self, amount: int):
            current = self.get_state(int, 0)
            self.set_state(current + amount)
            return current + amount

        def get(self):
            return self.get_state(int, 0)

    def counter_orchestrator(ctx, _):
        entity_id = EntityInstanceId("Counter", "orchCounter")
        yield ctx.call_entity(entity_id, "add", input=5)
        yield ctx.call_entity(entity_id, "add", input=10)
        result = yield ctx.call_entity(entity_id, "get")
        return result

    worker.add_orchestrator(counter_orchestrator)
    worker.add_entity(Counter)
    worker.start()

    try:
        instance_id = client.schedule_new_orchestration(counter_orchestrator)
        state = client.wait_for_orchestration_completion(instance_id, timeout=30)

        assert state is not None
        assert state.runtime_status == OrchestrationStatus.COMPLETED
        assert state.serialized_output == "15"
    finally:
        worker.stop()


def test_signal_entity_from_orchestrator(backend):
    """Test signaling an entity from an orchestrator (fire-and-forget)."""
    client = TaskHubGrpcClient(host_address=f"localhost:{ENTITY_TEST_PORT}")
    worker = TaskHubGrpcWorker(host_address=f"localhost:{ENTITY_TEST_PORT}")

    class Counter(DurableEntity):
        def set(self, value: int):
            self.set_state(value)

        def get(self):
            return self.get_state(int, 0)

    def signal_orchestrator(ctx, _):
        entity_id = EntityInstanceId("Counter", "signalCounter")
        ctx.signal_entity(entity_id, "set", input=42)
        # call_entity to wait for a round-trip so we know the signal was processed
        result = yield ctx.call_entity(entity_id, "get")
        return result

    worker.add_orchestrator(signal_orchestrator)
    worker.add_entity(Counter)
    worker.start()

    try:
        instance_id = client.schedule_new_orchestration(signal_orchestrator)
        state = client.wait_for_orchestration_completion(instance_id, timeout=30)

        assert state is not None
        assert state.runtime_status == OrchestrationStatus.COMPLETED
        assert state.serialized_output == "42"
    finally:
        worker.stop()


def test_get_entity_not_found(backend):
    """Test that GetEntity returns None for a non-existent entity."""
    client = TaskHubGrpcClient(host_address=f"localhost:{ENTITY_TEST_PORT}")

    entity_id = EntityInstanceId("counter", "nonexistent")
    metadata = client.get_entity(entity_id, include_state=True)
    assert metadata is None


def test_entity_signal_from_entity(backend):
    """Test that an entity can signal another entity."""
    client = TaskHubGrpcClient(host_address=f"localhost:{ENTITY_TEST_PORT}")
    worker = TaskHubGrpcWorker(host_address=f"localhost:{ENTITY_TEST_PORT}")

    class Counter(DurableEntity):
        def set(self, value: int):
            self.set_state(value)

        def add(self, amount: int):
            current = self.get_state(int, 0)
            self.set_state(current + amount)

        def get(self):
            return self.get_state(int, 0)

        def copy_to(self, target_key: str):
            """Signal another Counter entity with our current value."""
            target = EntityInstanceId("Counter", target_key)
            self.signal_entity(target, "set", self.get_state(int, 0))

    def entity_signal_orchestrator(ctx, _):
        source_id = EntityInstanceId("Counter", "source")
        target_id = EntityInstanceId("Counter", "target")

        # Set source counter to 100
        yield ctx.call_entity(source_id, "set", input=100)

        # Tell source to copy its value to target
        yield ctx.call_entity(source_id, "copy_to", input="target")

        # Wait a bit for the signal to propagate, then read target
        # Use call_entity on target to read its state after propagation
        result = yield ctx.call_entity(target_id, "get")
        return result

    worker.add_orchestrator(entity_signal_orchestrator)
    worker.add_entity(Counter)
    worker.start()

    try:
        instance_id = client.schedule_new_orchestration(entity_signal_orchestrator)
        state = client.wait_for_orchestration_completion(instance_id, timeout=30)

        assert state is not None
        assert state.runtime_status == OrchestrationStatus.COMPLETED
        assert state.serialized_output == "100"
    finally:
        worker.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
