# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json

import pytest

from durabletask import client, task, worker
from durabletask.testing import create_test_backend

HOST = "localhost:50055"


@pytest.fixture(autouse=True)
def backend():
    """Create an in-memory backend for testing."""
    b = create_test_backend(port=50055)
    yield b
    b.stop()
    b.reset()


def test_versioned_orchestration_succeeds():
    def plus_one(_: task.ActivityContext, input: int) -> int:
        return input + 1

    def sequence(ctx: task.OrchestrationContext, start_val: int):
        numbers = [start_val]
        current = start_val
        for _ in range(10):
            current = yield ctx.call_activity(plus_one, input=current, tags={'Activity': 'PlusOne'})
            numbers.append(current)
        return numbers

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(sequence)
        w.add_activity(plus_one)
        w.use_versioning(worker.VersioningOptions(
            version="1.0.0",
            default_version="1.0.0",
            match_strategy=worker.VersionMatchStrategy.CURRENT_OR_OLDER,
            failure_strategy=worker.VersionFailureStrategy.FAIL
        ))
        w.start()

        task_hub_client = client.TaskHubGrpcClient(host_address=HOST, default_version="1.0.0")
        id = task_hub_client.schedule_new_orchestration(sequence, input=1, tags={'Orchestration': 'Sequence'}, version="1.0.0")
        state = task_hub_client.wait_for_orchestration_completion(
            id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(sequence)
    assert state.instance_id == id
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.failure_details is None
    assert state.serialized_input == json.dumps(1)
    assert state.serialized_output == json.dumps([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
    assert state.serialized_custom_status is None
