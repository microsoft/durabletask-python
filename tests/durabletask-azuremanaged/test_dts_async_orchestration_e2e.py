# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import os
import threading
from datetime import timedelta
import uuid

import pytest

from durabletask import client, task
from durabletask.azuremanaged.client import AsyncDurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker

# NOTE: These tests assume a sidecar process is running. Example command:
#       docker run -i -p 8080:8080 -p 8082:8082 -d mcr.microsoft.com/dts/dts-emulator:latest
pytestmark = [pytest.mark.dts, pytest.mark.asyncio]

# Read the environment variables
taskhub_name = os.getenv("TASKHUB", "default")
endpoint = os.getenv("ENDPOINT", "http://localhost:8080")


def _get_credential():
    """Returns DefaultAzureCredential if endpoint is https, otherwise None (for emulator)."""
    if endpoint.startswith("https://"):
        from azure.identity import DefaultAzureCredential
        return DefaultAzureCredential()
    return None


async def test_empty_orchestration():

    invoked = False

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        nonlocal invoked  # don't do this in a real app!
        invoked = True

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(empty_orchestrator)
        w.start()

        c = AsyncDurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                            taskhub=taskhub_name, token_credential=None)
        id = await c.schedule_new_orchestration(empty_orchestrator)
        state = await c.wait_for_orchestration_completion(id, timeout=30)

    assert invoked
    assert state is not None
    assert state.name == task.get_name(empty_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_input is None
    assert state.serialized_output is None
    assert state.serialized_custom_status is None


async def test_activity_sequence():

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
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(sequence)
        w.add_activity(plus_one)
        w.start()

        task_hub_client = AsyncDurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                          taskhub=taskhub_name, token_credential=None)
        id = await task_hub_client.schedule_new_orchestration(sequence, input=1)
        state = await task_hub_client.wait_for_orchestration_completion(
            id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(sequence)
    assert state.instance_id == id
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.failure_details is None
    assert state.serialized_input == json.dumps(1)
    assert state.serialized_output == json.dumps([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
    assert state.serialized_custom_status is None


async def test_activity_error_handling():

    def throw(_: task.ActivityContext, input: int) -> int:
        raise RuntimeError("Kah-BOOOOM!!!")

    compensation_counter = 0

    def increment_counter(ctx, _):
        nonlocal compensation_counter
        compensation_counter += 1

    def orchestrator(ctx: task.OrchestrationContext, input: int):
        error_msg = ""
        try:
            yield ctx.call_activity(throw, input=input)
        except task.TaskFailedError as e:
            error_msg = e.details.message

            # compensating actions
            yield ctx.call_activity(increment_counter)
            yield ctx.call_activity(increment_counter)

        return error_msg

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(orchestrator)
        w.add_activity(throw)
        w.add_activity(increment_counter)
        w.start()

        task_hub_client = AsyncDurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                          taskhub=taskhub_name, token_credential=None)
        id = await task_hub_client.schedule_new_orchestration(orchestrator, input=1)
        state = await task_hub_client.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(orchestrator)
    assert state.instance_id == id
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == json.dumps("Kah-BOOOOM!!!")
    assert state.failure_details is None
    assert state.serialized_custom_status is None
    assert compensation_counter == 2


async def test_sub_orchestration_fan_out():
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
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_activity(increment)
        w.add_orchestrator(orchestrator_child)
        w.add_orchestrator(parent_orchestrator)
        w.start()

        task_hub_client = AsyncDurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                          taskhub=taskhub_name, token_credential=None)
        id = await task_hub_client.schedule_new_orchestration(parent_orchestrator, input=10)
        state = await task_hub_client.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.failure_details is None
    assert activity_counter == 30


async def test_sub_orchestrator_by_name():
    sub_orchestrator_counter = 0

    def orchestrator_child(ctx: task.OrchestrationContext, _):
        nonlocal sub_orchestrator_counter
        sub_orchestrator_counter += 1

    def parent_orchestrator(ctx: task.OrchestrationContext, _):
        yield ctx.call_sub_orchestrator("orchestrator_child")

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(orchestrator_child)
        w.add_orchestrator(parent_orchestrator)
        w.start()

        task_hub_client = AsyncDurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                          taskhub=taskhub_name, token_credential=None)
        id = await task_hub_client.schedule_new_orchestration(parent_orchestrator, input=None)
        state = await task_hub_client.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.failure_details is None
    assert sub_orchestrator_counter == 1


async def test_wait_for_multiple_external_events():
    def orchestrator(ctx: task.OrchestrationContext, _):
        a = yield ctx.wait_for_external_event('A')
        b = yield ctx.wait_for_external_event('B')
        c = yield ctx.wait_for_external_event('C')
        return [a, b, c]

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(orchestrator)
        w.start()

        # Start the orchestration and immediately raise events to it.
        task_hub_client = AsyncDurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                          taskhub=taskhub_name, token_credential=None)
        id = await task_hub_client.schedule_new_orchestration(orchestrator)
        await task_hub_client.raise_orchestration_event(id, 'A', data='a')
        await task_hub_client.raise_orchestration_event(id, 'B', data='b')
        await task_hub_client.raise_orchestration_event(id, 'C', data='c')
        state = await task_hub_client.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == json.dumps(['a', 'b', 'c'])


async def test_terminate():
    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.wait_for_external_event("my_event")
        return result

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(orchestrator)
        w.start()

        task_hub_client = AsyncDurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                          taskhub=taskhub_name, token_credential=None)
        id = await task_hub_client.schedule_new_orchestration(orchestrator)
        state = await task_hub_client.wait_for_orchestration_start(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.RUNNING

        await task_hub_client.terminate_orchestration(id, output="some reason for termination")
        state = await task_hub_client.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.TERMINATED
        assert state.serialized_output == json.dumps("some reason for termination")


async def test_terminate_recursive():
    def root(ctx: task.OrchestrationContext, _):
        result = yield ctx.call_sub_orchestrator(child)
        return result

    def child(ctx: task.OrchestrationContext, _):
        result = yield ctx.wait_for_external_event("my_event")
        return result

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(root)
        w.add_orchestrator(child)
        w.start()

        task_hub_client = AsyncDurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                          taskhub=taskhub_name, token_credential=None)
        id = await task_hub_client.schedule_new_orchestration(root)
        state = await task_hub_client.wait_for_orchestration_start(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.RUNNING

        # Terminate root orchestration(recursive set to True by default)
        await task_hub_client.terminate_orchestration(id, output="some reason for termination")
        state = await task_hub_client.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.TERMINATED

        # Verify that child orchestration is also terminated
        await task_hub_client.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.TERMINATED

        await task_hub_client.purge_orchestration(id)
        state = await task_hub_client.get_orchestration_state(id)
        assert state is None


async def test_restart_with_same_instance_id():
    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.call_activity(say_hello, input="World")
        return result

    def say_hello(ctx: task.ActivityContext, input: str):
        return f"Hello, {input}!"

    credential = _get_credential()

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=credential) as w:
        w.add_orchestrator(orchestrator)
        w.add_activity(say_hello)
        w.start()

        task_hub_client = AsyncDurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                          taskhub=taskhub_name, token_credential=credential)
        id = await task_hub_client.schedule_new_orchestration(orchestrator)
        state = await task_hub_client.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output == json.dumps("Hello, World!")

        # Restart the orchestration with the same instance ID
        restarted_id = await task_hub_client.restart_orchestration(id)
        assert restarted_id == id

        state = await task_hub_client.wait_for_orchestration_completion(restarted_id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output == json.dumps("Hello, World!")


async def test_restart_with_new_instance_id():
    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.call_activity(say_hello, input="World")
        return result

    def say_hello(ctx: task.ActivityContext, input: str):
        return f"Hello, {input}!"

    credential = _get_credential()

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=credential) as w:
        w.add_orchestrator(orchestrator)
        w.add_activity(say_hello)
        w.start()

        task_hub_client = AsyncDurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                          taskhub=taskhub_name, token_credential=credential)
        id = await task_hub_client.schedule_new_orchestration(orchestrator)
        state = await task_hub_client.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED

        # Restart the orchestration with a new instance ID
        restarted_id = await task_hub_client.restart_orchestration(id, restart_with_new_instance_id=True)
        assert restarted_id != id

        state = await task_hub_client.wait_for_orchestration_completion(restarted_id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output == json.dumps("Hello, World!")


async def test_retry_policies():
    child_orch_counter = 0
    throw_activity_counter = 0

    retry_policy = task.RetryPolicy(
        first_retry_interval=timedelta(seconds=1),
        max_number_of_attempts=3,
        backoff_coefficient=1,
        max_retry_interval=timedelta(seconds=10),
        retry_timeout=timedelta(seconds=30))

    def parent_orchestrator_with_retry(ctx: task.OrchestrationContext, _):
        yield ctx.call_sub_orchestrator(child_orchestrator_with_retry, retry_policy=retry_policy)

    def child_orchestrator_with_retry(ctx: task.OrchestrationContext, _):
        nonlocal child_orch_counter
        if not ctx.is_replaying:
            child_orch_counter += 1
        yield ctx.call_activity(throw_activity_with_retry, retry_policy=retry_policy)

    def throw_activity_with_retry(ctx: task.ActivityContext, _):
        nonlocal throw_activity_counter
        throw_activity_counter += 1
        raise RuntimeError("Kah-BOOOOM!!!")

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(parent_orchestrator_with_retry)
        w.add_orchestrator(child_orchestrator_with_retry)
        w.add_activity(throw_activity_with_retry)
        w.start()

        task_hub_client = AsyncDurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                          taskhub=taskhub_name, token_credential=None)
        id = await task_hub_client.schedule_new_orchestration(parent_orchestrator_with_retry)
        state = await task_hub_client.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.FAILED
        assert state.failure_details is not None
        assert state.failure_details.error_type == "TaskFailedError"
        assert state.failure_details.message.startswith("Sub-orchestration task #1 failed:")
        assert state.failure_details.message.endswith("Activity task #1 failed: Kah-BOOOOM!!!")
        assert state.failure_details.stack_trace is not None
        assert throw_activity_counter == 9
        assert child_orch_counter == 3


async def test_retry_timeout():
    throw_activity_counter = 0
    retry_policy = task.RetryPolicy(
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

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(mock_orchestrator)
        w.add_activity(throw_activity)
        w.start()

        task_hub_client = AsyncDurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                          taskhub=taskhub_name, token_credential=None)
        id = await task_hub_client.schedule_new_orchestration(mock_orchestrator)
        state = await task_hub_client.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.FAILED
        assert state.failure_details is not None
        assert state.failure_details.error_type == "TaskFailedError"
        assert state.failure_details.message.endswith("Activity task #1 failed: Kah-BOOOOM!!!")
        assert state.failure_details.stack_trace is not None
        assert throw_activity_counter == 4


async def test_custom_status():

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        ctx.set_custom_status("foobaz")

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(empty_orchestrator)
        w.start()

        c = AsyncDurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                            taskhub=taskhub_name, token_credential=None)
        id = await c.schedule_new_orchestration(empty_orchestrator)
        state = await c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(empty_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_input is None
    assert state.serialized_output is None
    assert state.serialized_custom_status == "\"foobaz\""


async def test_new_uuid():
    def noop(_: task.ActivityContext, _1):
        pass

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        # Assert that two new_uuid calls return different values
        results = [ctx.new_uuid(), ctx.new_uuid()]
        yield ctx.call_activity("noop")
        # Assert that new_uuid still returns a unique value after replay
        results.append(ctx.new_uuid())
        return results

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_activity(noop)
        w.start()

        c = AsyncDurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                            taskhub=taskhub_name, token_credential=None)
        id = await c.schedule_new_orchestration(empty_orchestrator)
        state = await c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(empty_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    results = json.loads(state.serialized_output or "\"\"")
    assert isinstance(results, list) and len(results) == 3
    assert uuid.UUID(results[0]) != uuid.UUID(results[1])
    assert uuid.UUID(results[0]) != uuid.UUID(results[2])
    assert uuid.UUID(results[1]) != uuid.UUID(results[2])


async def test_orchestration_with_unparsable_output_fails():
    def test_orchestrator(ctx: task.OrchestrationContext, _):
        return Exception("This is not JSON serializable")

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(test_orchestrator)
        w.start()

        c = AsyncDurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                            taskhub=taskhub_name, token_credential=None)
        id = await c.schedule_new_orchestration(test_orchestrator)
        state = await c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(test_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is not None
    assert state.failure_details.error_type == "JsonEncodeOutputException"
    assert state.failure_details.message.startswith("The orchestration result could not be encoded. Object details:")
    assert state.failure_details.message.find("This is not JSON serializable") != -1
    assert state.runtime_status == client.OrchestrationStatus.FAILED
