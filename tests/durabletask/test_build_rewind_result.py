# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for _OrchestrationExecutor._build_rewind_result.

These tests directly invoke the rewind history-rewriting logic and
verify that the clean history produced by the worker matches the
expected semantics:

* The ``executionStarted`` event gets a new execution ID.
* When a ``parentExecutionId`` is present on the rewind event,
  the ``parentInstance.orchestrationInstance.executionId`` on the
  ``executionStarted`` copy is updated accordingly.
* ``taskFailed`` events and their corresponding ``taskScheduled``
  events are removed.
* ``subOrchestrationInstanceFailed`` events and their corresponding
  ``taskScheduled`` events are removed.
* ``subOrchestrationInstanceCreated`` events for failed sub-orchestrations
  are kept so the backend can recursively rewind them.
* ``executionCompleted`` events are removed.
* Successful activity/sub-orchestration results are preserved.
"""

import json
import logging

import durabletask.internal.helpers as helpers
import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask import task, worker
from google.protobuf import wrappers_pb2

logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.DEBUG)
TEST_LOGGER = logging.getLogger("tests")

TEST_INSTANCE_ID = "rewind-test-instance"
ORIGINAL_EXECUTION_ID = "original-exec-id"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_execution_started(
        name: str,
        instance_id: str = TEST_INSTANCE_ID,
        execution_id: str = ORIGINAL_EXECUTION_ID,
        parent_instance: pb.ParentInstanceInfo | None = None,
) -> pb.HistoryEvent:
    """Create an executionStarted event with an explicit execution ID."""
    event = pb.HistoryEvent(
        eventId=-1,
        executionStarted=pb.ExecutionStartedEvent(
            name=name,
            orchestrationInstance=pb.OrchestrationInstance(
                instanceId=instance_id,
                executionId=wrappers_pb2.StringValue(value=execution_id),
            ),
        ),
    )
    if parent_instance is not None:
        event.executionStarted.parentInstance.CopyFrom(parent_instance)
    return event


def _make_execution_rewound(
        reason: str = "test rewind",
        parent_execution_id: str | None = None,
) -> pb.HistoryEvent:
    """Create an executionRewound history event."""
    rewound = pb.ExecutionRewoundEvent(
        reason=wrappers_pb2.StringValue(value=reason),
    )
    if parent_execution_id is not None:
        rewound.parentExecutionId.CopyFrom(
            wrappers_pb2.StringValue(value=parent_execution_id))
    return pb.HistoryEvent(
        eventId=-1,
        executionRewound=rewound,
    )


def _dummy_orchestrator(ctx: task.OrchestrationContext, _):
    return None


def _make_executor() -> worker._OrchestrationExecutor:
    """Create a minimal _OrchestrationExecutor (no registered functions needed)."""
    registry = worker._Registry()
    registry.add_orchestrator(_dummy_orchestrator)
    return worker._OrchestrationExecutor(registry, TEST_LOGGER)


def _get_clean_history(result: worker.ExecutionResults) -> list[pb.HistoryEvent]:
    """Extract the clean history from a RewindOrchestrationAction result."""
    assert len(result.actions) == 1
    action = result.actions[0]
    assert action.HasField("rewindOrchestration")
    return list(action.rewindOrchestration.newHistory)


# ---------------------------------------------------------------------------
# Tests: execution ID changes
# ---------------------------------------------------------------------------


def test_rewind_assigns_new_execution_id():
    """The executionStarted event in the clean history must have a new,
    different execution ID."""
    executor = _make_executor()

    old_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_started("my_orch"),
        helpers.new_task_scheduled_event(1, "my_activity"),
        helpers.new_task_failed_event(1, RuntimeError("boom")),
        helpers.new_orchestrator_completed_event(),
        helpers.new_execution_completed_event(
            pb.ORCHESTRATION_STATUS_FAILED,
            failure_details=helpers.new_failure_details(RuntimeError("boom"))),
    ]
    new_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_rewound("retry"),
    ]

    result = executor._build_rewind_result(
        TEST_INSTANCE_ID, "my_orch", old_events, new_events)
    clean = _get_clean_history(result)

    # Find the executionStarted event
    started_events = [e for e in clean if e.HasField("executionStarted")]
    assert len(started_events) == 1

    new_exec_id = started_events[0].executionStarted.orchestrationInstance.executionId.value
    assert new_exec_id != ORIGINAL_EXECUTION_ID
    assert len(new_exec_id) > 0  # must be a non-empty string


def test_rewind_preserves_execution_started_fields():
    """The executionStarted copy should preserve the original fields
    (name, instance ID) while changing only the execution ID."""
    executor = _make_executor()

    old_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_started("preserve_me"),
        helpers.new_task_scheduled_event(1, "act"),
        helpers.new_task_failed_event(1, RuntimeError("fail")),
        helpers.new_orchestrator_completed_event(),
        helpers.new_execution_completed_event(
            pb.ORCHESTRATION_STATUS_FAILED,
            failure_details=helpers.new_failure_details(RuntimeError("fail"))),
    ]
    new_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_rewound("retry"),
    ]

    result = executor._build_rewind_result(
        TEST_INSTANCE_ID, "preserve_me", old_events, new_events)
    clean = _get_clean_history(result)

    started = [e for e in clean if e.HasField("executionStarted")][0]
    assert started.executionStarted.name == "preserve_me"
    assert started.executionStarted.orchestrationInstance.instanceId == TEST_INSTANCE_ID


def test_rewind_updates_parent_execution_id():
    """When the rewind event carries a parentExecutionId, the
    executionStarted copy must update
    parentInstance.orchestrationInstance.executionId to match."""
    executor = _make_executor()

    parent_exec_id = "parent-old-exec-id"
    parent_new_exec_id = "parent-new-exec-id"

    parent_info = pb.ParentInstanceInfo(
        taskScheduledId=5,
        name=wrappers_pb2.StringValue(value="parent_orch"),
        orchestrationInstance=pb.OrchestrationInstance(
            instanceId="parent-instance",
            executionId=wrappers_pb2.StringValue(value=parent_exec_id),
        ),
    )

    old_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_started("child_orch", parent_instance=parent_info),
        helpers.new_task_scheduled_event(1, "child_act"),
        helpers.new_task_failed_event(1, RuntimeError("child fail")),
        helpers.new_orchestrator_completed_event(),
        helpers.new_execution_completed_event(
            pb.ORCHESTRATION_STATUS_FAILED,
            failure_details=helpers.new_failure_details(RuntimeError("child fail"))),
    ]
    new_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_rewound("parent rewind", parent_execution_id=parent_new_exec_id),
    ]

    result = executor._build_rewind_result(
        TEST_INSTANCE_ID, "child_orch", old_events, new_events)
    clean = _get_clean_history(result)

    started = [e for e in clean if e.HasField("executionStarted")][0]
    # Parent execution ID should be updated to the new one.
    actual_parent_exec_id = (
        started.executionStarted.parentInstance
        .orchestrationInstance.executionId.value
    )
    assert actual_parent_exec_id == parent_new_exec_id


def test_rewind_no_parent_execution_id_leaves_parent_unchanged():
    """When the rewind event has no parentExecutionId, the executionStarted
    copy should leave the parentInstance untouched (if any)."""
    executor = _make_executor()

    parent_exec_id = "parent-exec-id-unchanged"
    parent_info = pb.ParentInstanceInfo(
        taskScheduledId=5,
        name=wrappers_pb2.StringValue(value="parent_orch"),
        orchestrationInstance=pb.OrchestrationInstance(
            instanceId="parent-instance",
            executionId=wrappers_pb2.StringValue(value=parent_exec_id),
        ),
    )

    old_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_started("child_orch", parent_instance=parent_info),
        helpers.new_task_scheduled_event(1, "child_act"),
        helpers.new_task_failed_event(1, RuntimeError("fail")),
        helpers.new_orchestrator_completed_event(),
        helpers.new_execution_completed_event(
            pb.ORCHESTRATION_STATUS_FAILED,
            failure_details=helpers.new_failure_details(RuntimeError("fail"))),
    ]
    new_events = [
        helpers.new_orchestrator_started_event(),
        # No parentExecutionId on the rewind event.
        _make_execution_rewound("top-level rewind"),
    ]

    result = executor._build_rewind_result(
        TEST_INSTANCE_ID, "child_orch", old_events, new_events)
    clean = _get_clean_history(result)

    started = [e for e in clean if e.HasField("executionStarted")][0]
    actual_parent_exec_id = (
        started.executionStarted.parentInstance
        .orchestrationInstance.executionId.value
    )
    # Should remain unchanged.
    assert actual_parent_exec_id == parent_exec_id


# ---------------------------------------------------------------------------
# Tests: failed activity cleanup
# ---------------------------------------------------------------------------


def test_rewind_removes_failed_activity_events():
    """taskFailed and its corresponding taskScheduled should be removed."""
    executor = _make_executor()

    old_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_started("orch"),
        helpers.new_task_scheduled_event(1, "my_act"),
        helpers.new_task_failed_event(1, RuntimeError("boom")),
        helpers.new_orchestrator_completed_event(),
        helpers.new_execution_completed_event(
            pb.ORCHESTRATION_STATUS_FAILED,
            failure_details=helpers.new_failure_details(RuntimeError("boom"))),
    ]
    new_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_rewound("retry"),
    ]

    result = executor._build_rewind_result(
        TEST_INSTANCE_ID, "orch", old_events, new_events)
    clean = _get_clean_history(result)

    # No taskFailed or taskScheduled for the failed activity.
    assert not any(e.HasField("taskFailed") for e in clean)
    assert not any(
        e.HasField("taskScheduled") and e.eventId == 1 for e in clean
    )


def test_rewind_preserves_successful_activity():
    """Successful taskScheduled + taskCompleted should remain in clean history."""
    executor = _make_executor()

    old_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_started("orch"),
        # Activity 1 succeeds
        helpers.new_task_scheduled_event(1, "good_act"),
        helpers.new_task_completed_event(1, json.dumps("ok")),
        helpers.new_orchestrator_completed_event(),
        # Activity 2 fails
        helpers.new_orchestrator_started_event(),
        helpers.new_task_scheduled_event(2, "bad_act"),
        helpers.new_task_failed_event(2, RuntimeError("fail")),
        helpers.new_orchestrator_completed_event(),
        helpers.new_execution_completed_event(
            pb.ORCHESTRATION_STATUS_FAILED,
            failure_details=helpers.new_failure_details(RuntimeError("fail"))),
    ]
    new_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_rewound("retry"),
    ]

    result = executor._build_rewind_result(
        TEST_INSTANCE_ID, "orch", old_events, new_events)
    clean = _get_clean_history(result)

    # Activity 1's taskScheduled and taskCompleted should still be present.
    assert any(
        e.HasField("taskScheduled") and e.eventId == 1 for e in clean
    )
    assert any(
        e.HasField("taskCompleted") and e.taskCompleted.taskScheduledId == 1
        for e in clean
    )
    # Activity 2's taskScheduled and taskFailed should be removed.
    assert not any(
        e.HasField("taskScheduled") and e.eventId == 2 for e in clean
    )
    assert not any(e.HasField("taskFailed") for e in clean)


# ---------------------------------------------------------------------------
# Tests: failed sub-orchestration cleanup
# ---------------------------------------------------------------------------


def test_rewind_removes_failed_sub_orch_events():
    """subOrchestrationInstanceFailed and its corresponding taskScheduled
    should be removed, but subOrchestrationInstanceCreated is kept."""
    executor = _make_executor()

    old_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_started("parent_orch"),
        helpers.new_sub_orchestration_created_event(1, "child_orch", "child-id"),
        helpers.new_orchestrator_completed_event(),
        helpers.new_orchestrator_started_event(),
        helpers.new_sub_orchestration_failed_event(1, RuntimeError("child exploded")),
        helpers.new_orchestrator_completed_event(),
        helpers.new_execution_completed_event(
            pb.ORCHESTRATION_STATUS_FAILED,
            failure_details=helpers.new_failure_details(RuntimeError("child exploded"))),
    ]
    new_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_rewound("rewind child"),
    ]

    result = executor._build_rewind_result(
        TEST_INSTANCE_ID, "parent_orch", old_events, new_events)
    clean = _get_clean_history(result)

    # subOrchestrationInstanceFailed is removed.
    assert not any(
        e.HasField("subOrchestrationInstanceFailed") for e in clean
    )
    # The corresponding taskScheduled (eventId == 1) used by
    # subOrchestrationInstanceFailed should also be removed, since
    # _build_rewind_result collects the taskScheduledId from
    # subOrchestrationInstanceFailed too.
    # Note: subOrchestrationInstanceCreated uses a separate event with
    # its own eventId and is NOT removed.
    assert not any(
        e.HasField("taskScheduled") and e.eventId == 1 for e in clean
    )
    # The subOrchestrationInstanceCreated event should be preserved
    # so the backend can identify which sub-orchestration to rewind.
    assert any(
        e.HasField("subOrchestrationInstanceCreated") for e in clean
    )


def test_rewind_preserves_successful_sub_orchestration():
    """Successful sub-orchestration events should be preserved."""
    executor = _make_executor()

    old_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_started("parent_orch"),
        # Sub-orch 1 succeeds
        helpers.new_sub_orchestration_created_event(1, "child_ok", "child-ok-id"),
        helpers.new_orchestrator_completed_event(),
        helpers.new_orchestrator_started_event(),
        helpers.new_sub_orchestration_completed_event(1, json.dumps("child result")),
        # Sub-orch 2 fails
        helpers.new_sub_orchestration_created_event(2, "child_fail", "child-fail-id"),
        helpers.new_orchestrator_completed_event(),
        helpers.new_orchestrator_started_event(),
        helpers.new_sub_orchestration_failed_event(2, RuntimeError("fail")),
        helpers.new_orchestrator_completed_event(),
        helpers.new_execution_completed_event(
            pb.ORCHESTRATION_STATUS_FAILED,
            failure_details=helpers.new_failure_details(RuntimeError("fail"))),
    ]
    new_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_rewound("retry"),
    ]

    result = executor._build_rewind_result(
        TEST_INSTANCE_ID, "parent_orch", old_events, new_events)
    clean = _get_clean_history(result)

    # Sub-orch 1's created + completed should be present.
    created_ids = [
        e.subOrchestrationInstanceCreated.instanceId
        for e in clean if e.HasField("subOrchestrationInstanceCreated")
    ]
    assert "child-ok-id" in created_ids
    completed_sub_ids = [
        e.subOrchestrationInstanceCompleted.taskScheduledId
        for e in clean if e.HasField("subOrchestrationInstanceCompleted")
    ]
    assert 1 in completed_sub_ids
    # Sub-orch 2's failed event should be removed.
    assert not any(
        e.HasField("subOrchestrationInstanceFailed") for e in clean
    )
    # Sub-orch 2's created event should be kept (for backend recursive rewind).
    assert "child-fail-id" in created_ids


# ---------------------------------------------------------------------------
# Tests: executionCompleted removal
# ---------------------------------------------------------------------------


def test_rewind_removes_execution_completed():
    """executionCompleted events should be stripped from the clean history."""
    executor = _make_executor()

    old_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_started("orch"),
        helpers.new_task_scheduled_event(1, "act"),
        helpers.new_task_failed_event(1, RuntimeError("fail")),
        helpers.new_orchestrator_completed_event(),
        helpers.new_execution_completed_event(
            pb.ORCHESTRATION_STATUS_FAILED,
            failure_details=helpers.new_failure_details(RuntimeError("fail"))),
    ]
    new_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_rewound("retry"),
    ]

    result = executor._build_rewind_result(
        TEST_INSTANCE_ID, "orch", old_events, new_events)
    clean = _get_clean_history(result)

    assert not any(e.HasField("executionCompleted") for e in clean)


# ---------------------------------------------------------------------------
# Tests: orchestratorStarted/Completed preservation
# ---------------------------------------------------------------------------


def test_rewind_keeps_orchestrator_started_and_completed():
    """orchestratorStarted and orchestratorCompleted bookend events
    should be preserved in clean history."""
    executor = _make_executor()

    old_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_started("orch"),
        helpers.new_task_scheduled_event(1, "act"),
        helpers.new_orchestrator_completed_event(),
        helpers.new_orchestrator_started_event(),
        helpers.new_task_failed_event(1, RuntimeError("fail")),
        helpers.new_orchestrator_completed_event(),
        helpers.new_execution_completed_event(
            pb.ORCHESTRATION_STATUS_FAILED,
            failure_details=helpers.new_failure_details(RuntimeError("fail"))),
    ]
    new_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_rewound("retry"),
    ]

    result = executor._build_rewind_result(
        TEST_INSTANCE_ID, "orch", old_events, new_events)
    clean = _get_clean_history(result)

    orch_started_count = sum(
        1 for e in clean if e.HasField("orchestratorStarted")
    )
    orch_completed_count = sum(
        1 for e in clean if e.HasField("orchestratorCompleted")
    )
    # old_events has 2 orchestratorStarted + 2 orchestratorCompleted,
    # new_events adds 1 orchestratorStarted.  All should be kept.
    assert orch_started_count >= 2
    assert orch_completed_count >= 2


# ---------------------------------------------------------------------------
# Tests: executionRewound event preserved
# ---------------------------------------------------------------------------


def test_rewind_keeps_execution_rewound_event():
    """The executionRewound event itself should remain in the clean
    history so it is visible in the audit trail."""
    executor = _make_executor()

    old_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_started("orch"),
        helpers.new_task_scheduled_event(1, "act"),
        helpers.new_task_failed_event(1, RuntimeError("fail")),
        helpers.new_orchestrator_completed_event(),
        helpers.new_execution_completed_event(
            pb.ORCHESTRATION_STATUS_FAILED,
            failure_details=helpers.new_failure_details(RuntimeError("fail"))),
    ]
    new_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_rewound("rewind reason"),
    ]

    result = executor._build_rewind_result(
        TEST_INSTANCE_ID, "orch", old_events, new_events)
    clean = _get_clean_history(result)

    rewound_events = [e for e in clean if e.HasField("executionRewound")]
    assert len(rewound_events) == 1
    assert rewound_events[0].executionRewound.reason.value == "rewind reason"


# ---------------------------------------------------------------------------
# Tests: mixed scenario
# ---------------------------------------------------------------------------


def test_rewind_mixed_activities_and_sub_orchestrations():
    """A complex scenario with successful activities, failed activities,
    successful sub-orchestrations, and failed sub-orchestrations.
    Verifies that only failed items are cleaned and execution ID is updated."""
    executor = _make_executor()

    old_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_started("complex_orch"),
        # Activity 1 succeeds (eventId=1)
        helpers.new_task_scheduled_event(1, "good_activity"),
        helpers.new_task_completed_event(1, json.dumps("good")),
        # Sub-orch A succeeds (eventId=2)
        helpers.new_sub_orchestration_created_event(2, "child_ok", "child-ok-id"),
        helpers.new_orchestrator_completed_event(),
        helpers.new_orchestrator_started_event(),
        helpers.new_sub_orchestration_completed_event(2, json.dumps("child ok result")),
        # Activity 3 fails (eventId=3)
        helpers.new_task_scheduled_event(3, "bad_activity"),
        helpers.new_task_failed_event(3, RuntimeError("act fail")),
        helpers.new_orchestrator_completed_event(),
        # Sub-orch B fails (eventId=4)
        helpers.new_orchestrator_started_event(),
        helpers.new_sub_orchestration_created_event(4, "child_fail", "child-fail-id"),
        helpers.new_orchestrator_completed_event(),
        helpers.new_orchestrator_started_event(),
        helpers.new_sub_orchestration_failed_event(4, RuntimeError("sub orch fail")),
        helpers.new_orchestrator_completed_event(),
        helpers.new_execution_completed_event(
            pb.ORCHESTRATION_STATUS_FAILED,
            failure_details=helpers.new_failure_details(RuntimeError("overall fail"))),
    ]
    new_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_rewound("fix everything"),
    ]

    result = executor._build_rewind_result(
        TEST_INSTANCE_ID, "complex_orch", old_events, new_events)
    clean = _get_clean_history(result)

    # --- Execution ID changed ---
    started = [e for e in clean if e.HasField("executionStarted")]
    assert len(started) == 1
    assert (started[0].executionStarted.orchestrationInstance
            .executionId.value != ORIGINAL_EXECUTION_ID)

    # --- Successful activity 1 preserved ---
    assert any(
        e.HasField("taskScheduled") and e.eventId == 1 for e in clean
    )
    assert any(
        e.HasField("taskCompleted") and e.taskCompleted.taskScheduledId == 1
        for e in clean
    )

    # --- Failed activity 3 removed ---
    assert not any(
        e.HasField("taskScheduled") and e.eventId == 3 for e in clean
    )
    assert not any(e.HasField("taskFailed") for e in clean)

    # --- Successful sub-orch A preserved ---
    created_ids = [
        e.subOrchestrationInstanceCreated.instanceId
        for e in clean if e.HasField("subOrchestrationInstanceCreated")
    ]
    assert "child-ok-id" in created_ids
    completed_sub_ids = [
        e.subOrchestrationInstanceCompleted.taskScheduledId
        for e in clean if e.HasField("subOrchestrationInstanceCompleted")
    ]
    assert 2 in completed_sub_ids

    # --- Failed sub-orch B: failed event removed, created kept ---
    assert not any(
        e.HasField("subOrchestrationInstanceFailed") for e in clean
    )
    assert "child-fail-id" in created_ids

    # --- executionCompleted removed ---
    assert not any(e.HasField("executionCompleted") for e in clean)

    # --- executionRewound preserved ---
    assert any(e.HasField("executionRewound") for e in clean)


def test_rewind_does_not_mutate_original_events():
    """Verify that the original history events are not modified in place."""
    executor = _make_executor()

    es_event = _make_execution_started("orch")
    original_exec_id = (
        es_event.executionStarted.orchestrationInstance.executionId.value
    )

    old_events = [
        helpers.new_orchestrator_started_event(),
        es_event,
        helpers.new_task_scheduled_event(1, "act"),
        helpers.new_task_failed_event(1, RuntimeError("fail")),
        helpers.new_orchestrator_completed_event(),
        helpers.new_execution_completed_event(
            pb.ORCHESTRATION_STATUS_FAILED,
            failure_details=helpers.new_failure_details(RuntimeError("fail"))),
    ]
    new_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_rewound("retry"),
    ]

    executor._build_rewind_result(
        TEST_INSTANCE_ID, "orch", old_events, new_events)

    # The original executionStarted event should NOT be mutated.
    actual = es_event.executionStarted.orchestrationInstance.executionId.value
    assert actual == original_exec_id


def test_rewind_result_action_structure():
    """The result should contain exactly one OrchestratorAction with id=-1
    and a rewindOrchestration field."""
    executor = _make_executor()

    old_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_started("orch"),
        helpers.new_task_scheduled_event(1, "act"),
        helpers.new_task_failed_event(1, RuntimeError("fail")),
        helpers.new_orchestrator_completed_event(),
        helpers.new_execution_completed_event(
            pb.ORCHESTRATION_STATUS_FAILED,
            failure_details=helpers.new_failure_details(RuntimeError("fail"))),
    ]
    new_events = [
        helpers.new_orchestrator_started_event(),
        _make_execution_rewound("retry"),
    ]

    result = executor._build_rewind_result(
        TEST_INSTANCE_ID, "orch", old_events, new_events)

    assert len(result.actions) == 1
    action = result.actions[0]
    assert action.id == -1
    assert action.HasField("rewindOrchestration")
    assert result.encoded_custom_status is None
