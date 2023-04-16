# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import threading

import pytest
import simplejson as json

from durabletask import client, task, worker

# NOTE: These tests assume a sidecar process is running. Example command:
#       docker run --name durabletask-sidecar -p 4001:4001 --env 'DURABLETASK_SIDECAR_LOGLEVEL=Debug' --rm cgillum/durabletask-sidecar:latest start --backend Emulator
pytestmark = pytest.mark.e2e


def test_empty_orchestration():

    invoked = False

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        nonlocal invoked  # don't do this in a real app!
        invoked = True

    # Start a worker, which will connect to the sidecar in a background thread
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(empty_orchestrator)
        w.start()

        c = client.TaskHubGrpcClient()
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


def test_activity_sequence():

    def plus_one(_: task.ActivityContext, input: int) -> int:
        return input + 1

    def sequence(ctx: task.OrchestrationContext, start_val: int):
        numbers = [start_val]
        current = start_val
        for _ in range(10):
            current = yield ctx.call_activity(plus_one, input=current)
            numbers.append(current)
        return numbers

    # Start a worker, which will connect to the sidecar in a background thread
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(sequence)
        w.add_activity(plus_one)
        w.start()

        task_hub_client = client.TaskHubGrpcClient()
        id = task_hub_client.schedule_new_orchestration(sequence, input=1)
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


def test_sub_orchestration_fan_out():
    threadLock = threading.Lock()
    activity_counter = 0

    def increment(ctx, _):
        with threadLock:
            nonlocal activity_counter
            activity_counter += 1

    def orchestrator_child(ctx: task.OrchestrationContext, activity_count: int):
        for _ in range(activity_count):
            yield ctx.call_activity(increment)

    def parent_orchestrator(ctx: task.OrchestrationContext, count: int):
        # Fan out to multiple sub-orchestrations
        tasks = []
        for _ in range(count):
            tasks.append(ctx.call_sub_orchestrator(
                orchestrator_child, input=3))
        # Wait for all sub-orchestrations to complete
        yield task.when_all(tasks)

    # Start a worker, which will connect to the sidecar in a background thread
    with worker.TaskHubGrpcWorker() as w:
        w.start()
        w.add_activity(increment)
        w.add_orchestrator(orchestrator_child)
        w.add_orchestrator(parent_orchestrator)

        task_hub_client = client.TaskHubGrpcClient()
        id = task_hub_client.schedule_new_orchestration(parent_orchestrator, input=10)
        state = task_hub_client.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.failure_details is None
    assert activity_counter == 30
