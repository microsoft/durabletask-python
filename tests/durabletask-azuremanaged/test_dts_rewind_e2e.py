# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import os

import pytest

from durabletask import client, task
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker

# NOTE: These tests assume a sidecar process is running. Example command:
#       docker run -i -p 8080:8080 -p 8082:8082 -d mcr.microsoft.com/dts/dts-emulator:latest
pytestmark = [
    pytest.mark.dts,
    pytest.mark.skip(reason="Rewind support is not yet available in the public DTS emulator"),
]

# Read the environment variables
taskhub_name = os.getenv("TASKHUB", "default")
endpoint = os.getenv("ENDPOINT", "http://localhost:8080")


def _get_credential():
    """Returns DefaultAzureCredential if endpoint is https, otherwise None (for emulator)."""
    if endpoint.startswith("https://"):
        from azure.identity import DefaultAzureCredential
        return DefaultAzureCredential()
    return None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_rewind_failed_activity():
    """Rewind a failed orchestration whose single activity failed.

    After rewind the activity succeeds and the orchestration completes.
    """
    activity_call_count = 0
    should_fail = True

    def failing_activity(_: task.ActivityContext, input: str) -> str:
        nonlocal activity_call_count
        activity_call_count += 1
        if should_fail:
            raise RuntimeError("Simulated failure")
        return f"Hello, {input}!"

    def orchestrator(ctx: task.OrchestrationContext, input: str):
        result = yield ctx.call_activity(failing_activity, input=input)
        return result

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(orchestrator)
        w.add_activity(failing_activity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        instance_id = c.schedule_new_orchestration(orchestrator, input="World")
        state = c.wait_for_orchestration_completion(instance_id, timeout=30)

        # The orchestration should have failed.
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.FAILED

        # Fix the activity so it now succeeds, then rewind.
        should_fail = False
        c.rewind_orchestration(instance_id, reason="retry after fix")

        state = c.wait_for_orchestration_completion(instance_id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == json.dumps("Hello, World!")
    assert state.failure_details is None
    # Activity was called twice (once failed, once succeeded after rewind).
    assert activity_call_count == 2


def test_rewind_preserves_successful_results():
    """When an orchestration has a mix of successful and failed activities,
    rewind should re-execute only the failed activity while the successful
    result is replayed from history."""
    call_tracker: dict[str, int] = {"first": 0, "second": 0}
    should_fail_second = True

    def first_activity(_: task.ActivityContext, input: str) -> str:
        call_tracker["first"] += 1
        return f"first:{input}"

    def second_activity(_: task.ActivityContext, input: str) -> str:
        call_tracker["second"] += 1
        if should_fail_second:
            raise RuntimeError("Temporary failure")
        return f"second:{input}"

    def orchestrator(ctx: task.OrchestrationContext, input: str):
        r1 = yield ctx.call_activity(first_activity, input=input)
        r2 = yield ctx.call_activity(second_activity, input=input)
        return [r1, r2]

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(orchestrator)
        w.add_activity(first_activity)
        w.add_activity(second_activity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        instance_id = c.schedule_new_orchestration(orchestrator, input="test")
        state = c.wait_for_orchestration_completion(instance_id, timeout=30)

        # The orchestration should have failed (second_activity fails).
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.FAILED

        # Fix second_activity so it now succeeds, then rewind.
        should_fail_second = False
        c.rewind_orchestration(instance_id, reason="retry")
        state = c.wait_for_orchestration_completion(instance_id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == json.dumps(["first:test", "second:test"])
    assert state.failure_details is None
    # first_activity should NOT be re-executed – its result is replayed.
    assert call_tracker["first"] == 1
    # second_activity was called at least twice (once failed, once succeeded).
    assert call_tracker["second"] >= 2


def test_rewind_not_found():
    """Rewinding a non-existent instance should raise an RPC error."""
    c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                   taskhub=taskhub_name, token_credential=None)
    with pytest.raises(Exception):
        c.rewind_orchestration("nonexistent-id")


def test_rewind_non_failed_instance():
    """Rewinding a completed (non-failed) instance should raise an error."""
    def orchestrator(ctx: task.OrchestrationContext, _):
        return "done"

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(orchestrator)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        instance_id = c.schedule_new_orchestration(orchestrator)
        state = c.wait_for_orchestration_completion(instance_id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED

        with pytest.raises(Exception):
            c.rewind_orchestration(instance_id)


def test_rewind_with_sub_orchestration():
    """Rewind should recursively rewind failed sub-orchestrations."""
    sub_call_count = 0

    def child_activity(_: task.ActivityContext, input: str) -> str:
        nonlocal sub_call_count
        sub_call_count += 1
        if sub_call_count == 1:
            raise RuntimeError("Child failure")
        return f"child:{input}"

    def child_orchestrator(ctx: task.OrchestrationContext, input: str):
        result = yield ctx.call_activity(child_activity, input=input)
        return result

    def parent_orchestrator(ctx: task.OrchestrationContext, input: str):
        result = yield ctx.call_sub_orchestrator(
            child_orchestrator, input=input)
        return f"parent:{result}"

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(parent_orchestrator)
        w.add_orchestrator(child_orchestrator)
        w.add_activity(child_activity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        instance_id = c.schedule_new_orchestration(
            parent_orchestrator, input="data")
        state = c.wait_for_orchestration_completion(instance_id, timeout=30)

        # Parent should fail because child failed.
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.FAILED

        # Rewind – child_activity will succeed on retry.
        c.rewind_orchestration(instance_id, reason="sub-orch fix")
        state = c.wait_for_orchestration_completion(instance_id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == json.dumps("parent:child:data")
    assert sub_call_count == 2


def test_rewind_purged_sub_orchestration():
    """A purged sub-orchestration is re-run when the parent is rewound.

    Flow: parent orchestrator -> calls sub-orchestrator -> sub-orchestrator
    fails -> parent fails -> client purges the sub-orchestration -> client
    rewinds the parent -> parent re-schedules the sub-orchestration which
    now succeeds -> parent completes.
    """
    child_call_count = 0

    def child_activity(_: task.ActivityContext, input: str) -> str:
        nonlocal child_call_count
        child_call_count += 1
        if child_call_count == 1:
            raise RuntimeError("Child failure")
        return f"child:{input}"

    def child_orchestrator(ctx: task.OrchestrationContext, input: str):
        result = yield ctx.call_activity(child_activity, input=input)
        return result

    def parent_orchestrator(ctx: task.OrchestrationContext, input: str):
        result = yield ctx.call_sub_orchestrator(
            child_orchestrator, input=input, instance_id="sub-orch-to-purge")
        return f"parent:{result}"

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(parent_orchestrator)
        w.add_orchestrator(child_orchestrator)
        w.add_activity(child_activity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        instance_id = c.schedule_new_orchestration(
            parent_orchestrator, input="data")
        state = c.wait_for_orchestration_completion(instance_id, timeout=30)

        # Parent should fail because child failed.
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.FAILED

        # Purge the sub-orchestration so it must be completely re-run.
        c.purge_orchestration("sub-orch-to-purge")

        # Rewind the parent – child will be re-scheduled and succeed.
        c.rewind_orchestration(instance_id, reason="purge and retry")
        state = c.wait_for_orchestration_completion(instance_id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == json.dumps("parent:child:data")
    assert child_call_count == 2


def test_rewind_without_reason():
    """Rewind should work when no reason is provided."""
    call_count = 0

    def flaky_activity(_: task.ActivityContext, _1) -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("Boom")
        return "ok"

    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.call_activity(flaky_activity)
        return result

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(orchestrator)
        w.add_activity(flaky_activity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        instance_id = c.schedule_new_orchestration(orchestrator)
        state = c.wait_for_orchestration_completion(instance_id, timeout=30)
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.FAILED

        # Rewind without a reason
        c.rewind_orchestration(instance_id)
        state = c.wait_for_orchestration_completion(instance_id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == json.dumps("ok")


def test_rewind_twice():
    """Rewind the same orchestration twice after it fails a second time.

    The first rewind cleans up the initial failure. The activity then
    fails again. A second rewind should clean up the new failure and
    the orchestration should eventually complete.
    """
    call_count = 0

    def flaky_activity(_: task.ActivityContext, input: str) -> str:
        nonlocal call_count
        call_count += 1
        # Fail on the 1st and 2nd calls; succeed on the 3rd.
        if call_count <= 2:
            raise RuntimeError(f"Failure #{call_count}")
        return f"Hello, {input}!"

    def orchestrator(ctx: task.OrchestrationContext, input: str):
        result = yield ctx.call_activity(flaky_activity, input=input)
        return result

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(orchestrator)
        w.add_activity(flaky_activity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        instance_id = c.schedule_new_orchestration(orchestrator, input="World")
        state = c.wait_for_orchestration_completion(instance_id, timeout=30)

        # First failure.
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.FAILED

        # First rewind — activity will fail again (call_count == 2).
        c.rewind_orchestration(instance_id, reason="first rewind")
        state = c.wait_for_orchestration_completion(instance_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.FAILED

        # Second rewind — activity will succeed (call_count == 3).
        c.rewind_orchestration(instance_id, reason="second rewind")
        state = c.wait_for_orchestration_completion(instance_id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == json.dumps("Hello, World!")
    assert call_count == 3
