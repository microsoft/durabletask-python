# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import threading
import time

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# These counters live at module level so that orchestrators and
# activities can mutate them via ``nonlocal``.
_activity_call_count = 0
_should_fail = True


def _reset_counters():
    global _activity_call_count, _should_fail
    _activity_call_count = 0
    _should_fail = True


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_rewind_failed_activity():
    """Rewind a failed orchestration whose single activity failed.

    After rewind the activity succeeds and the orchestration completes.
    """
    _reset_counters()

    def failing_activity(_: task.ActivityContext, input: str) -> str:
        global _activity_call_count, _should_fail
        _activity_call_count += 1
        if _should_fail:
            raise RuntimeError("Simulated failure")
        return f"Hello, {input}!"

    def orchestrator(ctx: task.OrchestrationContext, input: str):
        result = yield ctx.call_activity(failing_activity, input=input)
        return result

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(orchestrator)
        w.add_activity(failing_activity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        instance_id = c.schedule_new_orchestration(orchestrator, input="World")
        state = c.wait_for_orchestration_completion(instance_id, timeout=30)

        # The orchestration should have failed.
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.FAILED

        # Fix the activity so it now succeeds, then rewind.
        global _should_fail
        _should_fail = False
        c.rewind_orchestration(instance_id, reason="retry after fix")

        state = c.wait_for_orchestration_completion(instance_id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == json.dumps("Hello, World!")
    assert state.failure_details is None
    # Activity was called twice (once failed, once succeeded after rewind).
    assert _activity_call_count == 2


def test_rewind_preserves_successful_results():
    """When an orchestration has a mix of successful and failed activities,
    rewind should re-execute only the failed activity while the successful
    result is replayed from history."""
    _reset_counters()

    call_tracker: dict[str, int] = {"first": 0, "second": 0}

    def first_activity(_: task.ActivityContext, input: str) -> str:
        call_tracker["first"] += 1
        return f"first:{input}"

    def second_activity(_: task.ActivityContext, input: str) -> str:
        call_tracker["second"] += 1
        if call_tracker["second"] == 1:
            raise RuntimeError("Temporary failure")
        return f"second:{input}"

    def orchestrator(ctx: task.OrchestrationContext, input: str):
        r1 = yield ctx.call_activity(first_activity, input=input)
        r2 = yield ctx.call_activity(second_activity, input=input)
        return [r1, r2]

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(orchestrator)
        w.add_activity(first_activity)
        w.add_activity(second_activity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        instance_id = c.schedule_new_orchestration(orchestrator, input="test")
        state = c.wait_for_orchestration_completion(instance_id, timeout=30)

        # The orchestration should have failed (second_activity fails).
        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.FAILED

        # Rewind – second_activity will now succeed on retry.
        c.rewind_orchestration(instance_id, reason="retry")
        state = c.wait_for_orchestration_completion(instance_id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == json.dumps(["first:test", "second:test"])
    assert state.failure_details is None
    # first_activity should NOT be re-executed – its result is replayed.
    assert call_tracker["first"] == 1
    # second_activity was called twice (once failed, once succeeded).
    assert call_tracker["second"] == 2


def test_rewind_not_found():
    """Rewinding a non-existent instance should raise an RPC error."""
    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.start()
        c = client.TaskHubGrpcClient(host_address=HOST)
        with pytest.raises(Exception):
            c.rewind_orchestration("nonexistent-id")


def test_rewind_non_failed_instance():
    """Rewinding a completed (non-failed) instance should raise an error."""
    def orchestrator(ctx: task.OrchestrationContext, _):
        return "done"

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(orchestrator)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
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

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(parent_orchestrator)
        w.add_orchestrator(child_orchestrator)
        w.add_activity(child_activity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
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


def test_rewind_without_reason():
    """Rewind should work when no reason is provided."""
    _reset_counters()
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

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(orchestrator)
        w.add_activity(flaky_activity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
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

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(orchestrator)
        w.add_activity(flaky_activity)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
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
