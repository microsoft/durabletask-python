# Durable Task Testing Utilities

This package provides testing utilities for the Durable Task Python SDK,
including an in-memory backend that eliminates the need for external
dependencies during testing.

## In-Memory Backend

The `InMemoryOrchestrationBackend` is a lightweight, in-memory implementation
of the Durable Task backend that runs as a gRPC server. It's designed for
testing scenarios where you want to test orchestrations without requiring a
sidecar process or external storage.

### Features

- **In-memory state storage**: All orchestration state is stored in memory
- **Full gRPC compatibility**: Implements the same gRPC interface as the production backend
- **Thread-safe**: Safe for concurrent access from multiple threads
- **Work item streaming**: Supports streaming work items to workers
- **Event handling**: Supports raising events, timers, and sub-orchestrations
- **Entity support**: Supports function-based and class-based entities
- **Lifecycle management**: Supports suspend, resume, terminate, and restart operations
- **State waiting**: Built-in support for waiting on orchestration state changes

### Quick Start

```python
import pytest
from durabletask.testing import create_test_backend
from durabletask.client import TaskHubGrpcClient, OrchestrationStatus
from durabletask.worker import TaskHubGrpcWorker

@pytest.fixture
def backend():
    """Create an in-memory backend for testing."""
    backend = create_test_backend(port=50051)
    yield backend
    backend.stop()
    backend.reset()

def test_simple_orchestration(backend):
    # Create client and worker
    client = TaskHubGrpcClient(host_address="localhost:50051")
    worker = TaskHubGrpcWorker(host_address="localhost:50051")

    # Define orchestrator and activity
    def hello_orchestrator(ctx, _):
        result = yield ctx.call_activity(say_hello, input="World")
        return result

    def say_hello(ctx, name: str):
        return f"Hello, {name}!"

    # Register orchestrator and activity with the worker
    worker.add_orchestrator(hello_orchestrator)
    worker.add_activity(say_hello)

    # Start worker
    worker.start()

    try:
        # Schedule orchestration
        instance_id = client.schedule_new_orchestration(hello_orchestrator)

        # Wait for completion
        state = client.wait_for_orchestration_completion(instance_id, timeout=10)

        # Verify results
        assert state.runtime_status == OrchestrationStatus.COMPLETED
        assert state.serialized_output == '"Hello, World!"'
    finally:
        worker.stop()
```

### Advanced Usage

#### Testing with Multiple Ports

```python
import random
import pytest
from durabletask.testing import create_test_backend
from durabletask.client import TaskHubGrpcClient
from durabletask.worker import TaskHubGrpcWorker

@pytest.fixture
def backend():
    # Use a random port to avoid conflicts
    port = random.randint(50000, 60000)
    backend = create_test_backend(port=port)
    yield backend, port
    backend.stop()
    backend.reset()

def test_orchestration(backend):
    backend_instance, port = backend
    client = TaskHubGrpcClient(host_address=f"localhost:{port}")
    worker = TaskHubGrpcWorker(host_address=f"localhost:{port}")
    # ...
```

#### Testing Event Handling

```python
def test_external_events(backend):
    client = TaskHubGrpcClient(host_address="localhost:50051")
    worker = TaskHubGrpcWorker(host_address="localhost:50051")

    def wait_for_event_orchestrator(ctx, _):
        event_data = yield ctx.wait_for_external_event("approval")
        return event_data

    worker.add_orchestrator(wait_for_event_orchestrator)
    worker.start()

    try:
        instance_id = client.schedule_new_orchestration(wait_for_event_orchestrator)

        # Wait for orchestration to start
        client.wait_for_orchestration_start(instance_id, timeout=5)

        # Raise an event
        client.raise_orchestration_event(instance_id, "approval", data="approved")

        # Wait for completion
        state = client.wait_for_orchestration_completion(instance_id, timeout=10)

        assert state.runtime_status == OrchestrationStatus.COMPLETED
        assert state.serialized_output == '"approved"'
    finally:
        worker.stop()
```

#### Testing Sub-Orchestrations

```python
def test_sub_orchestrations(backend):
    client = TaskHubGrpcClient(host_address="localhost:50051")
    worker = TaskHubGrpcWorker(host_address="localhost:50051")

    def parent_orchestrator(ctx, _):
        result1 = yield ctx.call_sub_orchestrator(child_orchestrator, input=1)
        result2 = yield ctx.call_sub_orchestrator(child_orchestrator, input=2)
        return result1 + result2

    def child_orchestrator(ctx, input: int):
        return input * 2

    worker.add_orchestrator(parent_orchestrator)
    worker.add_orchestrator(child_orchestrator)
    worker.start()

    try:
        instance_id = client.schedule_new_orchestration(parent_orchestrator)
        state = client.wait_for_orchestration_completion(instance_id, timeout=10)

        assert state.runtime_status == OrchestrationStatus.COMPLETED
        assert state.serialized_output == "6"  # (1*2) + (2*2)
    finally:
        worker.stop()
```

#### Testing Timers

```python
def test_durable_timers(backend):
    import time
    from datetime import timedelta

    client = TaskHubGrpcClient(host_address="localhost:50051")
    worker = TaskHubGrpcWorker(host_address="localhost:50051")

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

        assert state.runtime_status == OrchestrationStatus.COMPLETED
        assert elapsed >= 1.0  # Timer should have waited at least 1 second
    finally:
        worker.stop()
```

#### Testing Termination

```python
def test_orchestration_termination(backend):
    client = TaskHubGrpcClient(host_address="localhost:50051")
    worker = TaskHubGrpcWorker(host_address="localhost:50051")

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

        assert state.runtime_status == OrchestrationStatus.TERMINATED
    finally:
        worker.stop()
```

### Configuration Options

The `InMemoryOrchestrationBackend` supports the following configuration options:

- **port** (int): Port to listen on for gRPC connections (default: 50051)
- **max_history_size** (int): Maximum number of history events per orchestration (default: 10000)

```python
backend = InMemoryOrchestrationBackend(
    port=50051,
    max_history_size=100000  # Support larger orchestrations
)
backend.start()
```

Or use the convenience factory, which starts the server automatically:

```python
backend = create_test_backend(port=50051, max_history_size=10000)
```

### Thread Safety

The in-memory backend is thread-safe and can be safely accessed from
multiple threads. All state mutations are protected by locks to ensure
consistency.

### Limitations

The in-memory backend is designed for testing and has some limitations compared to production backends:

1. **No persistence**: All state is lost when the backend is stopped
2. **No distributed execution**: Runs in a single process
3. **No history streaming**: StreamInstanceHistory is not implemented
4. **No rewind**: RewindInstance is not implemented
5. **No recursive termination**: Recursive termination is not supported

### Best Practices

1. **Use fixtures**: Create pytest fixtures to manage backend lifecycle
2. **Reset between tests**: Call `backend.reset()` to clear state between tests
3. **Use random ports**: When running tests in parallel, use random port assignments
4. **Set appropriate timeouts**: Use reasonable timeout values in wait operations
5. **Clean up workers**: Always stop workers in finally blocks to prevent resource leaks

### Troubleshooting

#### Connection Errors

If you see connection errors:

- Ensure the backend is started before creating clients/workers
- Verify the port is not already in use
- Check that the host address matches the backend port

#### Timeouts

If tests timeout:

- Increase timeout values in `wait_for_orchestration_completion`
- Check that workers are started and processing work items
- Verify orchestrators and activities are registered correctly

#### State Not Found

If orchestration state is not found:

- Ensure you're using the correct instance ID
- Verify the orchestration was successfully scheduled
- Check that the backend wasn't reset between operations
