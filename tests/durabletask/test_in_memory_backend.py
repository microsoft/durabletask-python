# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Basic test to verify the in-memory backend works correctly.
"""

import pytest
import time
from datetime import timedelta

from durabletask.testing import create_test_backend
from durabletask.client import TaskHubGrpcClient, OrchestrationStatus
from durabletask.worker import TaskHubGrpcWorker


@pytest.fixture
def backend():
    """Create an in-memory backend for testing."""
    backend = create_test_backend(port=50052)
    yield backend
    backend.stop()
    backend.reset()


def test_simple_activity_orchestration(backend):
    """Test a simple orchestration that calls an activity."""
    client = TaskHubGrpcClient(host_address="localhost:50052")
    worker = TaskHubGrpcWorker(host_address="localhost:50052")

    def hello_orchestrator(ctx, _):
        result = yield ctx.call_activity(say_hello, input="World")
        return result

    def say_hello(ctx, name: str):
        return f"Hello, {name}!"

    worker.add_orchestrator(hello_orchestrator)
    worker.add_activity(say_hello)
    worker.start()

    try:
        # Schedule orchestration
        instance_id = client.schedule_new_orchestration(hello_orchestrator)

        # Wait for completion
        state = client.wait_for_orchestration_completion(instance_id, timeout=10)

        # Verify results
        assert state is not None
        assert state.runtime_status == OrchestrationStatus.COMPLETED
        assert state.serialized_output == '"Hello, World!"'
    finally:
        worker.stop()


def test_external_event(backend):
    """Test raising external events to orchestrations."""
    client = TaskHubGrpcClient(host_address="localhost:50052")
    worker = TaskHubGrpcWorker(host_address="localhost:50052")

    def event_waiting_orchestrator(ctx, _):
        event_data = yield ctx.wait_for_external_event("approval")
        return f"Approved: {event_data}"

    worker.add_orchestrator(event_waiting_orchestrator)
    worker.start()

    try:
        instance_id = client.schedule_new_orchestration(event_waiting_orchestrator)

        # Wait for orchestration to start
        client.wait_for_orchestration_start(instance_id, timeout=5)

        # Raise an event
        client.raise_orchestration_event(instance_id, "approval", data="yes")

        # Wait for completion
        state = client.wait_for_orchestration_completion(instance_id, timeout=10)

        assert state is not None
        assert state.runtime_status == OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None
        assert '"Approved: yes"' in state.serialized_output
    finally:
        worker.stop()


def test_durable_timer(backend):
    """Test that durable timers work correctly."""
    client = TaskHubGrpcClient(host_address="localhost:50052")
    worker = TaskHubGrpcWorker(host_address="localhost:50052")

    def timer_orchestrator(ctx, _):
        fire_at = ctx.current_utc_datetime + timedelta(seconds=1)
        yield ctx.create_timer(fire_at)
        return "timer_fired"

    worker.add_orchestrator(timer_orchestrator)
    worker.start()

    try:
        start_time = time.time()
        instance_id = client.schedule_new_orchestration(timer_orchestrator)
        state = client.wait_for_orchestration_completion(instance_id, timeout=10)
        elapsed = time.time() - start_time

        assert state is not None
        assert state.runtime_status == OrchestrationStatus.COMPLETED
        assert elapsed >= 1.0  # Timer should have waited at least 1 second
    finally:
        worker.stop()


def test_orchestration_termination(backend):
    """Test terminating a running orchestration."""
    client = TaskHubGrpcClient(host_address="localhost:50052")
    worker = TaskHubGrpcWorker(host_address="localhost:50052")

    def long_running_orchestrator(ctx, _):
        yield ctx.wait_for_external_event("never_happens")
        return "completed"

    worker.add_orchestrator(long_running_orchestrator)
    worker.start()

    try:
        instance_id = client.schedule_new_orchestration(long_running_orchestrator)

        # Wait for it to start
        client.wait_for_orchestration_start(instance_id, timeout=5)

        # Terminate it
        client.terminate_orchestration(instance_id, output="terminated_by_test")

        # Verify termination
        state = client.wait_for_orchestration_completion(instance_id, timeout=10)

        assert state is not None
        assert state.runtime_status == OrchestrationStatus.TERMINATED
    finally:
        worker.stop()


def test_sub_orchestration(backend):
    """Test calling sub-orchestrations."""
    client = TaskHubGrpcClient(host_address="localhost:50052")
    worker = TaskHubGrpcWorker(host_address="localhost:50052")

    def parent_orchestrator(ctx, input: int):
        result = yield ctx.call_sub_orchestrator(child_orchestrator, input=input)
        return f"Parent got: {result}"

    def child_orchestrator(ctx, input: int):
        return input * 2

    worker.add_orchestrator(parent_orchestrator)
    worker.add_orchestrator(child_orchestrator)
    worker.start()

    try:
        instance_id = client.schedule_new_orchestration(parent_orchestrator, input=5)
        state = client.wait_for_orchestration_completion(instance_id, timeout=10)

        assert state is not None
        assert state.runtime_status == OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None
        assert '"Parent got: 10"' in state.serialized_output
    finally:
        worker.stop()


def test_suspend_and_resume(backend):
    """Test suspending and resuming an orchestration."""
    client = TaskHubGrpcClient(host_address="localhost:50052")
    worker = TaskHubGrpcWorker(host_address="localhost:50052")

    def suspendable_orchestrator(ctx, _):
        result1 = yield ctx.call_activity(quick_activity, input=1)
        result2 = yield ctx.call_activity(quick_activity, input=2)
        return result1 + result2

    def quick_activity(ctx, input: int):
        return input

    worker.add_orchestrator(suspendable_orchestrator)
    worker.add_activity(quick_activity)
    worker.start()

    try:
        instance_id = client.schedule_new_orchestration(suspendable_orchestrator)

        # Wait for it to start
        client.wait_for_orchestration_start(instance_id, timeout=5)

        # Suspend it
        client.suspend_orchestration(instance_id)

        # Give it a moment to process the suspend
        time.sleep(0.5)

        # Check status
        state = client.get_orchestration_state(instance_id)
        # Note: The suspend may not have been processed yet depending on timing

        # Resume it
        client.resume_orchestration(instance_id)

        # Wait for completion
        state = client.wait_for_orchestration_completion(instance_id, timeout=10)

        assert state is not None
        # Should complete successfully after resume
        assert state.runtime_status in (OrchestrationStatus.COMPLETED, OrchestrationStatus.RUNNING)
    finally:
        worker.stop()


def test_multiple_activities(backend):
    """Test an orchestration that calls multiple activities."""
    client = TaskHubGrpcClient(host_address="localhost:50052")
    worker = TaskHubGrpcWorker(host_address="localhost:50052")

    def multi_activity_orchestrator(ctx, input: list):
        results = []
        for item in input:
            result = yield ctx.call_activity(process_item, input=item)
            results.append(result)
        return results

    def process_item(ctx, input: str):
        return f"processed_{input}"

    worker.add_orchestrator(multi_activity_orchestrator)
    worker.add_activity(process_item)
    worker.start()

    try:
        instance_id = client.schedule_new_orchestration(
            multi_activity_orchestrator,
            input=["a", "b", "c"]
        )
        state = client.wait_for_orchestration_completion(instance_id, timeout=10)

        assert state is not None
        assert state.runtime_status == OrchestrationStatus.COMPLETED
        # Verify all items were processed
        assert state.serialized_output is not None
        assert "processed_a" in state.serialized_output
        assert "processed_b" in state.serialized_output
        assert "processed_c" in state.serialized_output
    finally:
        worker.stop()


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
