# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json

import pytest

from durabletask import client, task, worker

# NOTE: These tests assume a sidecar process is running. Example command:
#       docker run --name durabletask-sidecar -p 4001:4001 --env 'DURABLETASK_SIDECAR_LOGLEVEL=Debug' --rm cgillum/durabletask-sidecar:latest start --backend Emulator
pytestmark = pytest.mark.e2e


def test_versioned_orchestration_succeeds():
    return  # Currently not passing as the sidecar does not support versioning yet
    # Remove these lines to run the test after the sidecar is updated

    def plus_one(_: task.ActivityContext, input: int) -> int:
        return input + 1

    def sequence(ctx: task.OrchestrationContext, start_val: int):
        numbers = [start_val]
        current = start_val
        for _ in range(10):
            current = yield ctx.call_activity(plus_one, input=current, tags={'Activity': 'PlusOne'})
            numbers.append(current)
        return numbers

    # Start a worker, which will connect to the sidecar in a background thread
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(sequence)
        w.add_activity(plus_one)
        w.use_versioning(worker.VersioningOptions(
            version="1.0.0",
            default_version="1.0.0",
            match_strategy=worker.VersionMatchStrategy.CURRENT_OR_OLDER,
            failure_strategy=worker.VersionFailureStrategy.FAIL
        ))
        w.start()

        task_hub_client = client.TaskHubGrpcClient(default_version="1.0.0")
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
