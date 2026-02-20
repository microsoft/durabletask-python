# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import asyncio
import json

import pytest

from durabletask import client, task, worker

# NOTE: These tests assume a sidecar process is running. Example command:
#       go install github.com/microsoft/durabletask-go@main
#       durabletask-go --port 4001
pytestmark = pytest.mark.e2e


@pytest.mark.asyncio
async def test_async_empty_orchestration():

    invoked = False

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        nonlocal invoked  # don't do this in a real app!
        invoked = True

    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(empty_orchestrator)
        w.start()

        c = client.AsyncTaskHubGrpcClient()
        id = await c.schedule_new_orchestration(empty_orchestrator, tags={'Tagged': 'true'})
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


@pytest.mark.asyncio
async def test_async_activity_sequence():

    def plus_one(_: task.ActivityContext, input: int) -> int:
        return input + 1

    def sequence(ctx: task.OrchestrationContext, start_val: int):
        numbers = [start_val]
        current = start_val
        for _ in range(10):
            current = yield ctx.call_activity(plus_one, input=current)
            numbers.append(current)
        return numbers

    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(sequence)
        w.add_activity(plus_one)
        w.start()

        c = client.AsyncTaskHubGrpcClient()
        id = await c.schedule_new_orchestration(sequence, input=1)
        state = await c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(sequence)
    assert state.instance_id == id
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.failure_details is None
    assert state.serialized_input == json.dumps(1)
    assert state.serialized_output == json.dumps([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])


@pytest.mark.asyncio
async def test_async_wait_for_multiple_external_events():
    def orchestrator(ctx: task.OrchestrationContext, _):
        a = yield ctx.wait_for_external_event('A')
        b = yield ctx.wait_for_external_event('B')
        c = yield ctx.wait_for_external_event('C')
        return [a, b, c]

    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(orchestrator)
        w.start()

        c = client.AsyncTaskHubGrpcClient()
        id = await c.schedule_new_orchestration(orchestrator)
        await c.raise_orchestration_event(id, 'A', data='a')
        await c.raise_orchestration_event(id, 'B', data='b')
        await c.raise_orchestration_event(id, 'C', data='c')
        state = await c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == json.dumps(['a', 'b', 'c'])


@pytest.mark.asyncio
async def test_async_suspend_and_resume():
    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.wait_for_external_event("my_event")
        return result

    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(orchestrator)
        w.start()

        c = client.AsyncTaskHubGrpcClient()
        id = await c.schedule_new_orchestration(orchestrator)
        state = await c.wait_for_orchestration_start(id, timeout=30)
        assert state is not None

        # Suspend the orchestration and wait for it to go into the SUSPENDED state
        await c.suspend_orchestration(id)
        while state.runtime_status == client.OrchestrationStatus.RUNNING:
            await asyncio.sleep(0.1)
            state = await c.get_orchestration_state(id)
            assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.SUSPENDED

        # Raise an event and confirm that it does NOT complete while suspended
        await c.raise_orchestration_event(id, "my_event", data=42)
        try:
            state = await c.wait_for_orchestration_completion(id, timeout=3)
            assert False, "Orchestration should not have completed"
        except TimeoutError:
            pass

        # Resume the orchestration and wait for it to complete
        await c.resume_orchestration(id)
        state = await c.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output == json.dumps(42)


@pytest.mark.asyncio
async def test_async_terminate():
    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.wait_for_external_event("my_event")
        return result

    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(orchestrator)
        w.start()

        c = client.AsyncTaskHubGrpcClient()
        id = await c.schedule_new_orchestration(orchestrator)
        state = await c.wait_for_orchestration_start(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.RUNNING

        await c.terminate_orchestration(id, output="some reason for termination")
        state = await c.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.TERMINATED
        assert state.serialized_output == json.dumps("some reason for termination")


@pytest.mark.asyncio
async def test_async_purge_orchestration():
    def orchestrator(ctx: task.OrchestrationContext, _):
        pass

    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(orchestrator)
        w.start()

        c = client.AsyncTaskHubGrpcClient()
        id = await c.schedule_new_orchestration(orchestrator)
        await c.wait_for_orchestration_completion(id, timeout=30)

        result = await c.purge_orchestration(id)
        assert result.deleted_instance_count == 1

        state = await c.get_orchestration_state(id)
        assert state is None


@pytest.mark.asyncio
@pytest.mark.skip(reason="durabletask-go does not yet support RestartInstance")
async def test_async_restart_with_same_instance_id():
    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.call_activity(say_hello, input="World")
        return result

    def say_hello(ctx: task.ActivityContext, input: str):
        return f"Hello, {input}!"

    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(orchestrator)
        w.add_activity(say_hello)
        w.start()

        c = client.AsyncTaskHubGrpcClient()
        id = await c.schedule_new_orchestration(orchestrator)
        state = await c.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output == json.dumps("Hello, World!")

        # Restart the orchestration with the same instance ID
        restarted_id = await c.restart_orchestration(id)
        assert restarted_id == id

        state = await c.wait_for_orchestration_completion(restarted_id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output == json.dumps("Hello, World!")


@pytest.mark.asyncio
@pytest.mark.skip(reason="durabletask-go does not yet support RestartInstance")
async def test_async_restart_with_new_instance_id():
    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.call_activity(say_hello, input="World")
        return result

    def say_hello(ctx: task.ActivityContext, input: str):
        return f"Hello, {input}!"

    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(orchestrator)
        w.add_activity(say_hello)
        w.start()

        c = client.AsyncTaskHubGrpcClient()
        id = await c.schedule_new_orchestration(orchestrator)
        state = await c.wait_for_orchestration_completion(id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED

        # Restart the orchestration with a new instance ID
        restarted_id = await c.restart_orchestration(id, restart_with_new_instance_id=True)
        assert restarted_id != id

        state = await c.wait_for_orchestration_completion(restarted_id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output == json.dumps("Hello, World!")
