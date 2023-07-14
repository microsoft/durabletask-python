# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import threading
import time
from datetime import timedelta

import pytest

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
        w.add_activity(increment)
        w.add_orchestrator(orchestrator_child)
        w.add_orchestrator(parent_orchestrator)
        w.start()

        task_hub_client = client.TaskHubGrpcClient()
        id = task_hub_client.schedule_new_orchestration(parent_orchestrator, input=10)
        state = task_hub_client.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.failure_details is None
    assert activity_counter == 30


def test_wait_for_multiple_external_events():
    def orchestrator(ctx: task.OrchestrationContext, _):
        a = yield ctx.wait_for_external_event('A')
        b = yield ctx.wait_for_external_event('B')
        c = yield ctx.wait_for_external_event('C')
        return [a, b, c]

    # Start a worker, which will connect to the sidecar in a background thread
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(orchestrator)
        w.start()

        # Start the orchestration and immediately raise events to it.
        task_hub_client = client.TaskHubGrpcClient()
        id = task_hub_client.schedule_new_orchestration(orchestrator)
        task_hub_client.raise_orchestration_event(id, 'A', data='a')
        task_hub_client.raise_orchestration_event(id, 'B', data='b')
        task_hub_client.raise_orchestration_event(id, 'C', data='c')
        state = task_hub_client.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == json.dumps(['a', 'b', 'c'])


@pytest.mark.parametrize("raise_event", [True, False])
def test_wait_for_external_event_timeout(raise_event: bool):
    def orchestrator(ctx: task.OrchestrationContext, _):
        approval: task.Task[bool] = ctx.wait_for_external_event('Approval')
        timeout = ctx.create_timer(timedelta(seconds=3))
        winner = yield task.when_any([approval, timeout])
        if winner == approval:
            return "approved"
        else:
            return "timed out"

    # Start a worker, which will connect to the sidecar in a background thread
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(orchestrator)
        w.start()

        # Start the orchestration and immediately raise events to it.
        task_hub_client = client.TaskHubGrpcClient()
        id = task_hub_client.schedule_new_orchestration(orchestrator)
        if raise_event:
            task_hub_client.raise_orchestration_event(id, 'Approval')
        state = task_hub_client.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    if raise_event:
        assert state.serialized_output == json.dumps("approved")
    else:
        assert state.serialized_output == json.dumps("timed out")


def test_suspend_and_resume():
    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.wait_for_external_event("my_event")
        return result

    # Start a worker, which will connect to the sidecar in a background thread
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(orchestrator)
        w.start()

        task_hub_client = client.TaskHubGrpcClient()
        id = task_hub_client.schedule_new_orchestration(orchestrator)
        state = task_hub_client.wait_for_orchestration_start(id, timeout=30)
        assert state is not None

        # Suspend the orchestration and wait for it to go into the SUSPENDED state
        task_hub_client.suspend_orchestration(id)
        while state.runtime_status == client.OrchestrationStatus.RUNNING:
            time.sleep(0.1)
            state = task_hub_client.get_orchestration_state(id)
            assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.SUSPENDED

        # Raise an event to the orchestration and confirm that it does NOT complete
        task_hub_client.raise_orchestration_event(id, "my_event", data=42)
        try:
            state = task_hub_client.wait_for_orchestration_completion(id, timeout=3)
            assert False, "Orchestration should not have completed"
        except TimeoutError:
            pass

        # Resume the orchestration and wait for it to complete
        task_hub_client.resume_orchestration(id)
        state = task_hub_client.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output == json.dumps(42)


def test_terminate():
    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.wait_for_external_event("my_event")
        return result

    # Start a worker, which will connect to the sidecar in a background thread
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(orchestrator)
        w.start()

        task_hub_client = client.TaskHubGrpcClient()
        id = task_hub_client.schedule_new_orchestration(orchestrator)
        state = task_hub_client.wait_for_orchestration_start(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.RUNNING

        task_hub_client.terminate_orchestration(id, output="some reason for termination")
        state = task_hub_client.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.TERMINATED
        assert state.serialized_output == json.dumps("some reason for termination")


def test_continue_as_new():
    all_results = []

    def orchestrator(ctx: task.OrchestrationContext, input: int):
        result = yield ctx.wait_for_external_event("my_event")
        if not ctx.is_replaying:
            # NOTE: Real orchestrations should never interact with nonlocal variables like this.
            nonlocal all_results
            all_results.append(result)

        if len(all_results) <= 4:
            ctx.continue_as_new(max(all_results), save_events=True)
        else:
            return all_results

    # Start a worker, which will connect to the sidecar in a background thread
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(orchestrator)
        w.start()

        task_hub_client = client.TaskHubGrpcClient()
        id = task_hub_client.schedule_new_orchestration(orchestrator, input=0)
        task_hub_client.raise_orchestration_event(id, "my_event", data=1)
        task_hub_client.raise_orchestration_event(id, "my_event", data=2)
        task_hub_client.raise_orchestration_event(id, "my_event", data=3)
        task_hub_client.raise_orchestration_event(id, "my_event", data=4)
        task_hub_client.raise_orchestration_event(id, "my_event", data=5)

        state = task_hub_client.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output == json.dumps(all_results)
        assert state.serialized_input == json.dumps(4)
        assert all_results == [1, 2, 3, 4, 5]

def test_retry_policies():
    # This test verifies that the retry policies are working as expected.
    # It does this by creating an orchestration that calls a sub-orchestrator,
    # which in turn calls an activity that always fails. 
    # In the first setup, this setup is done without any retry policies, and
    # the orchestration should fail. Here, sub-orchestrator is called twice,
    # and the activity is called once.
    # In the second setup, the retry policies are added, and the orchestration
    # should still fail. But, number of times the sub-orchestrator and activity
    # is called should increase as per the retry policies.

    # First setup: No retry policies
    child_orch_counter = 0
    throw_activity_counter = 0

    def parent_orchestrator(ctx: task.OrchestrationContext, _):
        yield ctx.call_sub_orchestrator(child_orchestrator)

    def child_orchestrator(ctx: task.OrchestrationContext, _):
        nonlocal child_orch_counter
        child_orch_counter += 1
        yield ctx.call_activity(throw_activity)

    def throw_activity(ctx: task.ActivityContext, _):
        nonlocal throw_activity_counter
        throw_activity_counter += 1
        raise RuntimeError("Kah-BOOOOM!!!")

    # Start a worker, which will connect to the sidecar in a background thread
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(parent_orchestrator)
        w.add_orchestrator(child_orchestrator)
        w.add_activity(throw_activity)
        w.start()

        task_hub_client = client.TaskHubGrpcClient()
        id = task_hub_client.schedule_new_orchestration(parent_orchestrator)
        state = task_hub_client.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.FAILED
        assert state.failure_details is not None
        assert state.failure_details.error_type == "TaskFailedError"
        assert state.failure_details.message.startswith("Sub-orchestration task #1 failed:")
        assert state.failure_details.message.endswith("Activity task #1 failed: Kah-BOOOOM!!!")
        assert state.failure_details.stack_trace is not None
        # child orchestrator gets called twice, but underlying activity gets called only once.
        assert throw_activity_counter == 1
        assert child_orch_counter == 2

    # Second setup: With retry policies
    retry_policy=task.RetryPolicy(
                first_retry_interval=timedelta(seconds=1),
                max_number_of_attempts=3,
                backoff_coefficient=1,
                max_retry_interval=timedelta(seconds=10),
                retry_timeout=timedelta(seconds=30))

    def parent_orchestrator_with_retry(ctx: task.OrchestrationContext, _):
        yield ctx.call_sub_orchestrator(child_orchestrator_with_retry, retry_policy=retry_policy)

    def child_orchestrator_with_retry(ctx: task.OrchestrationContext, _):
        nonlocal child_orch_counter
        child_orch_counter += 1
        yield ctx.call_activity(throw_activity_with_retry, retry_policy=retry_policy)

    def throw_activity_with_retry(ctx: task.ActivityContext, _):
        nonlocal throw_activity_counter
        throw_activity_counter += 1
        raise RuntimeError("Kah-BOOOOM!!!")

    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(parent_orchestrator_with_retry)
        w.add_orchestrator(child_orchestrator_with_retry)
        w.add_activity(throw_activity_with_retry)
        w.start()

        task_hub_client = client.TaskHubGrpcClient()
        id = task_hub_client.schedule_new_orchestration(parent_orchestrator_with_retry)
        state = task_hub_client.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.FAILED
        assert state.failure_details is not None
        assert state.failure_details.error_type == "TaskFailedError"
        assert state.failure_details.message.startswith("Sub-orchestration task #1 failed:")
        assert state.failure_details.message.endswith("Activity task #1 failed: Kah-BOOOOM!!!")
        assert state.failure_details.stack_trace is not None
        assert throw_activity_counter == 10
        assert child_orch_counter == 20

def test_retry_timeout():
    # This test verifies that the retry timeout is working as expected.
    # Max number of attempts is 5 and retry timeout is 14 seconds.
    # Total seconds consumed till 4th attempt is 1 + 2 + 4 + 8 = 15 seconds.
    # So, the 5th attempt should not be made and the orchestration should fail.
    throw_activity_counter = 0
    retry_policy=task.RetryPolicy(
                first_retry_interval=timedelta(seconds=1),
                max_number_of_attempts=5,
                backoff_coefficient=2,
                max_retry_interval=timedelta(seconds=10),
                retry_timeout=timedelta(seconds=14))

    def mock_orchestrator(ctx: task.OrchestrationContext, _):
        yield ctx.call_activity(throw_activity, retry_policy=retry_policy)

    def throw_activity(ctx: task.ActivityContext, _):
        nonlocal throw_activity_counter
        throw_activity_counter += 1
        raise RuntimeError("Kah-BOOOOM!!!")

    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(mock_orchestrator)
        w.add_activity(throw_activity)
        w.start()

        task_hub_client = client.TaskHubGrpcClient()
        id = task_hub_client.schedule_new_orchestration(mock_orchestrator)
        state = task_hub_client.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.FAILED
        assert state.failure_details is not None
        assert state.failure_details.error_type == "TaskFailedError"
        assert state.failure_details.message.endswith("Activity task #1 failed: Kah-BOOOOM!!!")
        assert state.failure_details.stack_trace is not None
        assert throw_activity_counter == 4
