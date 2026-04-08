# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import logging
from datetime import datetime, timedelta

import pytest

import durabletask.internal.helpers as helpers
import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask import task, worker, entities

logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.DEBUG)
TEST_LOGGER = logging.getLogger("tests")

TEST_INSTANCE_ID = "abc123"


def test_orchestrator_inputs():
    """Validates orchestrator function input population"""

    def orchestrator(ctx: task.OrchestrationContext, my_input: int):
        return my_input, ctx.instance_id, str(ctx.current_utc_datetime), ctx.is_replaying

    test_input = 42

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    start_time = datetime.now()
    new_events = [
        helpers.new_orchestrator_started_event(start_time),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=json.dumps(test_input)),
    ]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, [], new_events)
    actions = result.actions

    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result is not None

    expected_output = [test_input, TEST_INSTANCE_ID, str(start_time), False]
    assert complete_action.result.value == json.dumps(expected_output)


def test_complete_orchestration_actions():
    """Tests the actions output for a completed orchestration"""

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        return "done"

    registry = worker._Registry()
    name = registry.add_orchestrator(empty_orchestrator)

    new_events = [helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, [], new_events)
    actions = result.actions

    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result.value == '"done"'  # results are JSON-encoded


def test_orchestrator_not_registered():
    """Tests the effect of scheduling an unregistered orchestrator"""

    registry = worker._Registry()
    name = "Bogus"
    new_events = [helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, [], new_events)
    actions = result.actions

    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == "OrchestratorNotRegisteredError"
    assert complete_action.failureDetails.errorMessage


def test_create_timer_actions():
    """Tests the actions output for the create_timer orchestrator method"""

    def delay_orchestrator(ctx: task.OrchestrationContext, _):
        due_time = ctx.current_utc_datetime + timedelta(seconds=1)
        yield ctx.create_timer(due_time)
        return "done"

    registry = worker._Registry()
    name = registry.add_orchestrator(delay_orchestrator)

    start_time = datetime(2020, 1, 1, 12, 0, 0)
    expected_fire_at = start_time + timedelta(seconds=1)

    new_events = [
        helpers.new_orchestrator_started_event(start_time),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, [], new_events)
    actions = result.actions

    assert actions is not None
    assert len(actions) == 1
    assert type(actions[0]) is pb.OrchestratorAction
    assert actions[0].id == 1
    assert actions[0].HasField("createTimer")
    assert actions[0].createTimer.fireAt.ToDatetime() == expected_fire_at


def test_timer_fired_completion():
    """Tests the resumption of task using a timer_fired event"""

    def delay_orchestrator(ctx: task.OrchestrationContext, _):
        due_time = ctx.current_utc_datetime + timedelta(seconds=1)
        yield ctx.create_timer(due_time)
        return "done"

    registry = worker._Registry()
    name = registry.add_orchestrator(delay_orchestrator)

    start_time = datetime(2020, 1, 1, 12, 0, 0)
    expected_fire_at = start_time + timedelta(seconds=1)

    old_events = [
        helpers.new_orchestrator_started_event(start_time),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_timer_created_event(1, expected_fire_at)]
    new_events = [
        helpers.new_timer_fired_event(1, expected_fire_at)]

    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result is not None
    assert complete_action.result.value == '"done"'  # results are JSON-encoded


def test_long_timer_is_chunked_by_maximum_timer_interval():
    """Tests that long timers are scheduled in chunks when exceeding max timer interval."""

    def orchestrator(ctx: task.OrchestrationContext, _):
        due_time = ctx.current_utc_datetime + timedelta(days=10)
        yield ctx.create_timer(due_time)
        return "done"

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    start_time = datetime(2020, 1, 1, 12, 0, 0)
    first_chunk_fire_at = start_time + timedelta(days=3)

    new_events = [
        helpers.new_orchestrator_started_event(start_time),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, [], new_events)
    actions = result.actions

    assert len(actions) == 1
    assert actions[0].HasField("createTimer")
    assert actions[0].id == 1
    assert actions[0].createTimer.fireAt.ToDatetime() == first_chunk_fire_at


def test_long_timer_progresses_and_completes_on_final_chunk():
    """Tests that long timers schedule intermediate chunks and complete on the final timerFired."""

    def orchestrator(ctx: task.OrchestrationContext, _):
        due_time = ctx.current_utc_datetime + timedelta(days=10)
        yield ctx.create_timer(due_time)
        return "done"

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)

    start_time = datetime(2020, 1, 1, 12, 0, 0)
    t1 = start_time + timedelta(days=3)
    t2 = start_time + timedelta(days=6)
    t3 = start_time + timedelta(days=9)
    t4 = start_time + timedelta(days=10)

    # 1) Initial execution schedules first chunk.
    first = executor.execute(
        TEST_INSTANCE_ID,
        [],
        [
            helpers.new_orchestrator_started_event(start_time),
            helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        ],
    )
    assert len(first.actions) == 1
    assert first.actions[0].HasField("createTimer")
    assert first.actions[0].id == 1
    assert first.actions[0].createTimer.fireAt.ToDatetime() == t1

    # 2) First chunk fires -> schedule second chunk.
    second_old_events = [
        helpers.new_orchestrator_started_event(start_time),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_timer_created_event(1, t1),
    ]
    second = executor.execute(
        TEST_INSTANCE_ID,
        second_old_events,
        [helpers.new_timer_fired_event(1, t1)],
    )
    assert len(second.actions) == 1
    assert second.actions[0].HasField("createTimer")
    assert second.actions[0].id == 2
    assert second.actions[0].createTimer.fireAt.ToDatetime() == t2

    # 3) Second chunk fires -> schedule third chunk.
    third_old_events = second_old_events + [
        helpers.new_timer_fired_event(1, t1),
        helpers.new_timer_created_event(2, t2),
    ]
    third = executor.execute(
        TEST_INSTANCE_ID,
        third_old_events,
        [helpers.new_timer_fired_event(2, t2)],
    )
    assert len(third.actions) == 1
    assert third.actions[0].HasField("createTimer")
    assert third.actions[0].id == 3
    assert third.actions[0].createTimer.fireAt.ToDatetime() == t3

    # 4) Third chunk fires -> schedule final short chunk.
    fourth_old_events = third_old_events + [
        helpers.new_timer_fired_event(2, t2),
        helpers.new_timer_created_event(3, t3),
    ]
    fourth = executor.execute(
        TEST_INSTANCE_ID,
        fourth_old_events,
        [helpers.new_timer_fired_event(3, t3)],
    )
    assert len(fourth.actions) == 1
    assert fourth.actions[0].HasField("createTimer")
    assert fourth.actions[0].id == 4
    assert fourth.actions[0].createTimer.fireAt.ToDatetime() == t4

    # 5) Final chunk fires -> orchestration completes.
    fifth_old_events = fourth_old_events + [
        helpers.new_timer_fired_event(3, t3),
        helpers.new_timer_created_event(4, t4),
    ]
    fifth = executor.execute(
        TEST_INSTANCE_ID,
        fifth_old_events,
        [helpers.new_timer_fired_event(4, t4)],
    )
    complete_action = get_and_validate_complete_orchestration_action_list(1, fifth.actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result.value == '"done"'


def test_long_timer_can_be_cancelled_after_when_any_winner():
    """Tests cancellation of a long timer after an external event wins when_any."""

    def orchestrator(ctx: task.OrchestrationContext, _):
        approval = ctx.wait_for_external_event("approval")
        timeout = ctx.create_timer(timedelta(days=10))
        winner = yield task.when_any([approval, timeout])
        if winner == approval:
            timeout.cancel()
            return "approved"
        return "timed out"

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)

    start_time = datetime(2020, 1, 1, 12, 0, 0)
    first_chunk_fire_at = start_time + timedelta(days=3)

    # Initial execution schedules first long-timer chunk.
    first = executor.execute(
        TEST_INSTANCE_ID,
        [],
        [
            helpers.new_orchestrator_started_event(start_time),
            helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        ],
    )
    assert len(first.actions) == 1
    assert first.actions[0].HasField("createTimer")
    assert first.actions[0].createTimer.fireAt.ToDatetime() == first_chunk_fire_at

    # External event arrives before timeout -> long timer is cancelled and orchestration completes.
    old_events = [
        helpers.new_orchestrator_started_event(start_time),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_timer_created_event(1, first_chunk_fire_at),
    ]
    second = executor.execute(
        TEST_INSTANCE_ID,
        old_events,
        [helpers.new_event_raised_event("approval", json.dumps(True))],
    )
    complete_action = get_and_validate_complete_orchestration_action_list(1, second.actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result.value == '"approved"'


def test_timer_can_be_cancelled_after_when_any_winner():
    """Tests cancellation of an outstanding timer task after another task wins when_any."""

    def orchestrator(ctx: task.OrchestrationContext, _):
        approval = ctx.wait_for_external_event("approval")
        timeout = ctx.create_timer(timedelta(hours=1))
        winner = yield task.when_any([approval, timeout])
        if winner == approval:
            timeout.cancel()
            return "approved"
        return "timed out"

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)

    start_time = datetime(2020, 1, 1, 12, 0, 0)
    timeout_fire_at = start_time + timedelta(hours=1)

    result = executor.execute(
        TEST_INSTANCE_ID,
        [],
        [
            helpers.new_orchestrator_started_event(start_time),
            helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        ],
    )
    assert len(result.actions) == 1
    assert result.actions[0].HasField("createTimer")
    assert result.actions[0].createTimer.fireAt.ToDatetime() == timeout_fire_at

    old_events = [
        helpers.new_orchestrator_started_event(start_time),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_timer_created_event(1, timeout_fire_at),
    ]
    result = executor.execute(
        TEST_INSTANCE_ID,
        old_events,
        [helpers.new_event_raised_event("approval", json.dumps(True))],
    )
    complete_action = get_and_validate_complete_orchestration_action_list(1, result.actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result.value == '"approved"'


def test_only_cancellable_tasks_expose_cancel():
    """Tests that only timer and external-event tasks expose cancellation state and operations."""

    def dummy_activity(ctx, _):
        pass

    ctx = worker._RuntimeOrchestrationContext(TEST_INSTANCE_ID, worker._Registry())

    timer_task = ctx.create_timer(timedelta(minutes=5))
    external_event_task = ctx.wait_for_external_event("approval")
    activity_task = ctx.call_activity(dummy_activity)

    assert isinstance(timer_task, task.CancellableTask)
    assert isinstance(external_event_task, task.CancellableTask)
    assert not isinstance(activity_task, task.CancellableTask)
    assert hasattr(timer_task, "cancel")
    assert hasattr(external_event_task, "cancel")
    assert not hasattr(activity_task, "cancel")
    assert hasattr(timer_task, "is_cancelled")
    assert hasattr(external_event_task, "is_cancelled")
    assert not hasattr(activity_task, "is_cancelled")


def test_cancelled_task_get_result_raises_task_cancelled_error():
    """Tests that cancelled cancellable tasks raise TaskCancelledError from get_result."""

    cancellable_task = task.CancellableTask()

    assert cancellable_task.cancel() is True
    assert cancellable_task.is_cancelled is True

    with pytest.raises(task.TaskCancelledError):
        cancellable_task.get_result()


def test_schedule_activity_actions():
    """Test the actions output for the call_activity orchestrator method"""
    def dummy_activity(ctx, _):
        pass

    def orchestrator(ctx: task.OrchestrationContext, orchestrator_input):
        yield ctx.call_activity(dummy_activity, input=orchestrator_input)

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    # TODO: Test several different input types (int, bool, str, dict, etc.)
    encoded_input = json.dumps(42)
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, [], new_events)
    actions = result.actions

    assert len(actions) == 1
    assert type(actions[0]) is pb.OrchestratorAction
    assert actions[0].id == 1
    assert actions[0].HasField("scheduleTask")
    assert actions[0].scheduleTask.name == task.get_name(dummy_activity)
    assert actions[0].scheduleTask.input.value == encoded_input


def test_activity_task_completion():
    """Tests the successful completion of an activity task"""

    def dummy_activity(ctx, _):
        pass

    def orchestrator(ctx: task.OrchestrationContext, orchestrator_input):
        result = yield ctx.call_activity(dummy_activity, input=orchestrator_input)
        return result

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, task.get_name(dummy_activity))]

    encoded_output = json.dumps("done!")
    new_events = [helpers.new_task_completed_event(1, encoded_output)]

    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result.value == encoded_output


def test_activity_task_failed():
    """Tests the failure of an activity task"""
    def dummy_activity(ctx, _):
        pass

    def orchestrator(ctx: task.OrchestrationContext, orchestrator_input):
        result = yield ctx.call_activity(dummy_activity, input=orchestrator_input)
        return result

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, task.get_name(dummy_activity))]

    ex = Exception("Kah-BOOOOM!!!")
    new_events = [helpers.new_task_failed_event(1, ex)]

    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == 'TaskFailedError'  # TODO: Should this be the specific error?
    assert str(ex) in complete_action.failureDetails.errorMessage

    # Make sure the line of code where the exception was raised is included in the stack trace
    user_code_statement = "ctx.call_activity(dummy_activity, input=orchestrator_input)"
    assert user_code_statement in complete_action.failureDetails.stackTrace.value


def test_activity_retry_policies():
    """Tests the retry policy logic for activity tasks"""

    def dummy_activity(ctx, _):
        raise ValueError("Kah-BOOOOM!!!")

    def orchestrator(ctx: task.OrchestrationContext, orchestrator_input):
        result = yield ctx.call_activity(
            dummy_activity,
            retry_policy=task.RetryPolicy(
                first_retry_interval=timedelta(seconds=1),
                max_number_of_attempts=6,
                backoff_coefficient=2,
                max_retry_interval=timedelta(seconds=10),
                retry_timeout=timedelta(seconds=50)),
            input=orchestrator_input)
        return result

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    current_timestamp = datetime.utcnow()
    # Simulate the task failing for the first time and confirm that a timer is scheduled for 1 second in the future
    old_events = [
        helpers.new_orchestrator_started_event(timestamp=current_timestamp),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, task.get_name(dummy_activity))]
    expected_fire_at = current_timestamp + timedelta(seconds=1)

    new_events = [
        helpers.new_orchestrator_started_event(timestamp=current_timestamp),
        helpers.new_task_failed_event(1, ValueError("Kah-BOOOOM!!!"))]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 1
    assert actions[0].HasField("createTimer")
    assert actions[0].createTimer.fireAt.ToDatetime() == expected_fire_at
    assert actions[0].id == 2

    # Simulate the timer firing at the expected time and confirm that another activity task is scheduled
    current_timestamp = expected_fire_at
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_timer_fired_event(2, current_timestamp)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 2
    assert actions[1].HasField("scheduleTask")
    assert actions[1].id == 1

    # Simulate the task failing for the second time and confirm that a timer is scheduled for 2 seconds in the future
    old_events = old_events + new_events
    expected_fire_at = current_timestamp + timedelta(seconds=2)
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_task_failed_event(1, ValueError("Kah-BOOOOM!!!"))]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 3
    assert actions[2].HasField("createTimer")
    assert actions[2].createTimer.fireAt.ToDatetime() == expected_fire_at
    assert actions[2].id == 3

    # Simulate the timer firing at the expected time and confirm that another activity task is scheduled
    current_timestamp = expected_fire_at
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_timer_fired_event(3, current_timestamp)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 3
    assert actions[1].HasField("scheduleTask")
    assert actions[1].id == 1

    # Simulate the task failing for a third time and confirm that a timer is scheduled for 4 seconds in the future
    expected_fire_at = current_timestamp + timedelta(seconds=4)
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_task_failed_event(1, ValueError("Kah-BOOOOM!!!"))]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 4
    assert actions[3].HasField("createTimer")
    assert actions[3].createTimer.fireAt.ToDatetime() == expected_fire_at
    assert actions[3].id == 4

    # Simulate the timer firing at the expected time and confirm that another activity task is scheduled
    current_timestamp = expected_fire_at
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_timer_fired_event(4, current_timestamp)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 4
    assert actions[1].HasField("scheduleTask")
    assert actions[1].id == 1

    # Simulate the task failing for a fourth time and confirm that a timer is scheduled for 8 seconds in the future
    expected_fire_at = current_timestamp + timedelta(seconds=8)
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_task_failed_event(1, ValueError("Kah-BOOOOM!!!"))]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 5
    assert actions[4].HasField("createTimer")
    assert actions[4].createTimer.fireAt.ToDatetime() == expected_fire_at
    assert actions[4].id == 5

    # Simulate the timer firing at the expected time and confirm that another activity task is scheduled
    current_timestamp = expected_fire_at
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_timer_fired_event(5, current_timestamp)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 5
    assert actions[1].HasField("scheduleTask")
    assert actions[1].id == 1

    # Simulate the task failing for a fifth time and confirm that a timer is scheduled for 10 seconds in the future.
    # This time, the timer will fire after 10 seconds, instead of 16, as max_retry_interval is set to 10 seconds.
    expected_fire_at = current_timestamp + timedelta(seconds=10)
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_task_failed_event(1, ValueError("Kah-BOOOOM!!!"))]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 6
    assert actions[5].HasField("createTimer")
    assert actions[5].createTimer.fireAt.ToDatetime() == expected_fire_at
    assert actions[5].id == 6

    # Simulate the timer firing at the expected time and confirm that another activity task is scheduled
    current_timestamp = expected_fire_at
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_timer_fired_event(6, current_timestamp)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 6
    assert actions[1].HasField("scheduleTask")
    assert actions[1].id == 1

    # Simulate the task failing for a sixth time and confirm that orchestration is marked as failed finally.
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_task_failed_event(1, ValueError("Kah-BOOOOM!!!"))]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 7
    assert actions[-1].completeOrchestration.failureDetails.errorMessage.__contains__("Activity task #1 failed: Kah-BOOOOM!!!")
    assert actions[-1].id == 7


def test_activity_retry_without_max_retry_interval():
    """Tests that retry logic works correctly when max_retry_interval is not set.

    This is a regression test for a bug where compute_next_delay() returned None
    instead of the computed delay when max_retry_interval was not specified,
    causing retries to silently fail.
    """

    def dummy_activity(ctx, _):
        raise ValueError("Kah-BOOOOM!!!")

    def orchestrator(ctx: task.OrchestrationContext, orchestrator_input):
        result = yield ctx.call_activity(
            dummy_activity,
            retry_policy=task.RetryPolicy(
                first_retry_interval=timedelta(seconds=1),
                max_number_of_attempts=3,
                backoff_coefficient=2),
            input=orchestrator_input)
        return result

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    current_timestamp = datetime.utcnow()

    # Simulate the task failing for the first time — retry timer should be created at 1 second
    old_events = [
        helpers.new_orchestrator_started_event(timestamp=current_timestamp),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, task.get_name(dummy_activity))]
    expected_fire_at = current_timestamp + timedelta(seconds=1)

    new_events = [
        helpers.new_orchestrator_started_event(timestamp=current_timestamp),
        helpers.new_task_failed_event(1, ValueError("Kah-BOOOOM!!!"))]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 1
    assert actions[0].HasField("createTimer")
    assert actions[0].createTimer.fireAt.ToDatetime() == expected_fire_at
    assert actions[0].id == 2

    # Simulate the timer firing and a second failure — retry timer should be at 2 seconds (backoff)
    current_timestamp = expected_fire_at
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_timer_fired_event(2, current_timestamp)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 2
    assert actions[1].HasField("scheduleTask")
    assert actions[1].id == 1

    expected_fire_at = current_timestamp + timedelta(seconds=2)
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_task_failed_event(1, ValueError("Kah-BOOOOM!!!"))]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 3
    assert actions[2].HasField("createTimer")
    assert actions[2].createTimer.fireAt.ToDatetime() == expected_fire_at
    assert actions[2].id == 3

    # Simulate the timer firing and a third failure — should now fail (max_number_of_attempts=3)
    current_timestamp = expected_fire_at
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_timer_fired_event(3, current_timestamp)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 3
    assert actions[1].HasField("scheduleTask")

    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_task_failed_event(1, ValueError("Kah-BOOOOM!!!"))]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 4
    assert actions[-1].completeOrchestration.failureDetails.errorMessage.__contains__("Activity task #1 failed: Kah-BOOOOM!!!")


def test_activity_retry_with_default_backoff():
    """Tests retry with default backoff_coefficient (1.0) and no max_retry_interval.

    Verifies that retry delays remain constant when backoff_coefficient defaults to 1.0.
    """

    def dummy_activity(ctx, _):
        raise ValueError("Fail!")

    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.call_activity(
            dummy_activity,
            retry_policy=task.RetryPolicy(
                first_retry_interval=timedelta(seconds=5),
                max_number_of_attempts=3))
        return result

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    current_timestamp = datetime.utcnow()

    # First failure — retry timer at 5 seconds (default backoff=1.0, so 5 * 1^0 = 5)
    old_events = [
        helpers.new_orchestrator_started_event(timestamp=current_timestamp),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, task.get_name(dummy_activity))]
    expected_fire_at = current_timestamp + timedelta(seconds=5)

    new_events = [
        helpers.new_orchestrator_started_event(timestamp=current_timestamp),
        helpers.new_task_failed_event(1, ValueError("Fail!"))]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 1
    assert actions[0].HasField("createTimer")
    assert actions[0].createTimer.fireAt.ToDatetime() == expected_fire_at

    # Second failure — retry timer still at 5 seconds (5 * 1^1 = 5, no backoff growth)
    current_timestamp = expected_fire_at
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_timer_fired_event(2, current_timestamp)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)

    expected_fire_at = current_timestamp + timedelta(seconds=5)
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_task_failed_event(1, ValueError("Fail!"))]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 3
    assert actions[2].HasField("createTimer")
    assert actions[2].createTimer.fireAt.ToDatetime() == expected_fire_at


def test_activity_retry_with_long_timer_preserves_retryable_parent():
    """Tests that long retry timers keep retryable parent state until the final chunk fires."""

    def dummy_activity(ctx, _):
        raise ValueError("Kah-BOOOOM!!!")

    def orchestrator(ctx: task.OrchestrationContext, orchestrator_input):
        result = yield ctx.call_activity(
            dummy_activity,
            retry_policy=task.RetryPolicy(
                first_retry_interval=timedelta(days=10),
                max_number_of_attempts=2,
                backoff_coefficient=1,
            ),
            input=orchestrator_input,
        )
        return result

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)

    start = datetime.utcnow()
    t1 = start + timedelta(days=3)
    t2 = start + timedelta(days=6)
    t3 = start + timedelta(days=9)
    t4 = start + timedelta(days=10)

    old_events = [
        helpers.new_orchestrator_started_event(timestamp=start),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, task.get_name(dummy_activity)),
    ]

    # First activity failure should create the first long-timer chunk.
    new_events = [
        helpers.new_orchestrator_started_event(timestamp=start),
        helpers.new_task_failed_event(1, ValueError("Kah-BOOOOM!!!")),
    ]
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert actions[-1].HasField("createTimer")
    assert actions[-1].id == 2
    assert actions[-1].createTimer.fireAt.ToDatetime() == t1

    old_events = old_events + new_events

    # Intermediate chunk 1 fires -> schedule next chunk, not activity retry yet.
    new_events = [
        helpers.new_orchestrator_started_event(t1),
        helpers.new_timer_fired_event(2, t1),
    ]
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert actions[-1].HasField("createTimer")
    assert actions[-1].id == 3
    assert actions[-1].createTimer.fireAt.ToDatetime() == t2
    assert not actions[-1].HasField("scheduleTask")

    old_events = old_events + new_events

    # Intermediate chunk 2 fires -> schedule next chunk, still no activity retry.
    new_events = [
        helpers.new_orchestrator_started_event(t2),
        helpers.new_timer_fired_event(3, t2),
    ]
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert actions[-1].HasField("createTimer")
    assert actions[-1].id == 4
    assert actions[-1].createTimer.fireAt.ToDatetime() == t3
    assert not actions[-1].HasField("scheduleTask")

    old_events = old_events + new_events

    # Intermediate chunk 3 fires -> schedule final chunk, still no activity retry.
    new_events = [
        helpers.new_orchestrator_started_event(t3),
        helpers.new_timer_fired_event(4, t3),
    ]
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert actions[-1].HasField("createTimer")
    assert actions[-1].id == 5
    assert actions[-1].createTimer.fireAt.ToDatetime() == t4
    assert not actions[-1].HasField("scheduleTask")

    old_events = old_events + new_events

    # Final chunk fires -> retry activity should be rescheduled with original task ID.
    new_events = [
        helpers.new_orchestrator_started_event(t4),
        helpers.new_timer_fired_event(5, t4),
    ]
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert actions[-1].HasField("scheduleTask")
    assert actions[-1].id == 1


def test_nondeterminism_expected_timer():
    """Tests the non-determinism detection logic when call_timer is expected but some other method (call_activity) is called instead"""
    def dummy_activity(ctx, _):
        pass

    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.call_activity(dummy_activity)
        return result

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    fire_at = datetime.now()
    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_timer_created_event(1, fire_at)]
    new_events = [helpers.new_timer_fired_event(timer_id=1, fire_at=fire_at)]

    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == 'NonDeterminismError'
    assert "1" in complete_action.failureDetails.errorMessage  # task ID
    assert "create_timer" in complete_action.failureDetails.errorMessage  # expected method name
    assert "call_activity" in complete_action.failureDetails.errorMessage  # actual method name


def test_nondeterminism_expected_activity_call_no_task_id():
    """Tests the non-determinism detection logic when invoking activity functions"""
    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield task.CompletableTask()  # dummy task
        return result

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, "bogus_activity")]

    new_events = [helpers.new_task_completed_event(1)]

    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == 'NonDeterminismError'
    assert "1" in complete_action.failureDetails.errorMessage  # task ID
    assert "call_activity" in complete_action.failureDetails.errorMessage  # expected method name


def test_nondeterminism_expected_activity_call_wrong_task_type():
    """Tests the non-determinism detection when an activity exists in the history but a non-activity is in the code"""
    def dummy_activity(ctx, _):
        pass

    def orchestrator(ctx: task.OrchestrationContext, _):
        # create a timer when the history expects an activity call
        yield ctx.create_timer(datetime.now())

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, task.get_name(dummy_activity))]

    new_events = [helpers.new_task_completed_event(1)]

    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == 'NonDeterminismError'
    assert "1" in complete_action.failureDetails.errorMessage  # task ID
    assert "call_activity" in complete_action.failureDetails.errorMessage  # expected method name
    assert "create_timer" in complete_action.failureDetails.errorMessage  # unexpected method name


def test_nondeterminism_wrong_activity_name():
    """Tests the non-determinism detection when calling an activity with a name that differs from the name in the history"""
    def dummy_activity(ctx, _):
        pass

    def orchestrator(ctx: task.OrchestrationContext, _):
        # create a timer when the history expects an activity call
        yield ctx.call_activity(dummy_activity)

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, "original_activity")]

    new_events = [helpers.new_task_completed_event(1)]

    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == 'NonDeterminismError'
    assert "1" in complete_action.failureDetails.errorMessage  # task ID
    assert "call_activity" in complete_action.failureDetails.errorMessage  # expected method name
    assert "original_activity" in complete_action.failureDetails.errorMessage  # expected activity name
    assert "dummy_activity" in complete_action.failureDetails.errorMessage  # unexpected activity name


def test_sub_orchestration_task_completion():
    """Tests that a sub-orchestration task is completed when the sub-orchestration completes"""
    def suborchestrator(ctx: task.OrchestrationContext, _):
        pass

    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.call_sub_orchestrator(suborchestrator)
        return result

    registry = worker._Registry()
    suborchestrator_name = registry.add_orchestrator(suborchestrator)
    orchestrator_name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_sub_orchestration_created_event(1, suborchestrator_name, "sub-orch-123", encoded_input=None)]

    new_events = [
        helpers.new_sub_orchestration_completed_event(1, encoded_output="42")]

    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result.value == "42"


def test_sub_orchestration_task_failed():
    """Tests that a sub-orchestration task is completed when the sub-orchestration fails"""
    def suborchestrator(ctx: task.OrchestrationContext, _):
        pass

    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.call_sub_orchestrator(suborchestrator)
        return result

    registry = worker._Registry()
    suborchestrator_name = registry.add_orchestrator(suborchestrator)
    orchestrator_name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_sub_orchestration_created_event(1, suborchestrator_name, "sub-orch-123", encoded_input=None)]

    ex = Exception("Kah-BOOOOM!!!")
    new_events = [helpers.new_sub_orchestration_failed_event(1, ex)]

    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == 'TaskFailedError'  # TODO: Should this be the specific error?
    assert str(ex) in complete_action.failureDetails.errorMessage

    # Make sure the line of code where the exception was raised is included in the stack trace
    user_code_statement = "ctx.call_sub_orchestrator(suborchestrator)"
    assert user_code_statement in complete_action.failureDetails.stackTrace.value


def test_nondeterminism_expected_sub_orchestration_task_completion_no_task():
    """Tests the non-determinism detection when a sub-orchestration action is encounteed when it shouldn't be"""
    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield task.CompletableTask()  # dummy task
        return result

    registry = worker._Registry()
    orchestrator_name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_sub_orchestration_created_event(1, "some_sub_orchestration", "sub-orch-123", encoded_input=None)]

    new_events = [
        helpers.new_sub_orchestration_completed_event(1, encoded_output="42")]

    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == 'NonDeterminismError'
    assert "1" in complete_action.failureDetails.errorMessage  # task ID
    assert "call_sub_orchestrator" in complete_action.failureDetails.errorMessage  # expected method name


def test_nondeterminism_expected_sub_orchestration_task_completion_wrong_task_type():
    """Tests the non-determinism detection when a sub-orchestration action is encounteed when it shouldn't be.
    This variation tests the case where the expected task type is wrong (e.g. the code schedules a timer task
    but the history contains a sub-orchestration completed task)."""
    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.create_timer(datetime.utcnow())  # created timer but history expects sub-orchestration
        return result

    registry = worker._Registry()
    orchestrator_name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_sub_orchestration_created_event(1, "some_sub_orchestration", "sub-orch-123", encoded_input=None)]

    new_events = [
        helpers.new_sub_orchestration_completed_event(1, encoded_output="42")]

    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == 'NonDeterminismError'
    assert "1" in complete_action.failureDetails.errorMessage  # task ID
    assert "call_sub_orchestrator" in complete_action.failureDetails.errorMessage  # expected method name


def test_raise_event():
    """Tests that an orchestration can wait for and process an external event sent by a client"""
    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.wait_for_external_event("my_event")
        return result

    registry = worker._Registry()
    orchestrator_name = registry.add_orchestrator(orchestrator)

    old_events = []
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID)]

    # Execute the orchestration until it is waiting for an external event. The result
    # should be an empty list of actions because the orchestration didn't schedule any work.
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 0

    # Now send an external event to the orchestration and execute it again. This time
    # the orchestration should complete.
    old_events = new_events
    new_events = [helpers.new_event_raised_event("my_event", encoded_input="42")]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result.value == "42"


def test_raise_event_buffered():
    """Tests that an orchestration can receive an event that arrives earlier than expected"""
    def orchestrator(ctx: task.OrchestrationContext, _):
        yield ctx.create_timer(ctx.current_utc_datetime + timedelta(days=1))
        result = yield ctx.wait_for_external_event("my_event")
        return result

    registry = worker._Registry()
    orchestrator_name = registry.add_orchestrator(orchestrator)

    old_events = []
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID),
        helpers.new_event_raised_event("my_event", encoded_input="42")]

    # Execute the orchestration. It should be in a running state waiting for the timer to fire
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 1
    assert actions[0].HasField("createTimer")

    # Complete the timer task. The orchestration should move to the wait_for_external_event step, which
    # should then complete immediately because the event was buffered in the old event history.
    timer_due_time = datetime.utcnow() + timedelta(days=1)
    old_events = new_events + [helpers.new_timer_created_event(1, timer_due_time)]
    new_events = [helpers.new_timer_fired_event(1, timer_due_time)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result.value == "42"


def test_suspend_resume():
    """Tests that an orchestration can be suspended and resumed"""

    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.wait_for_external_event("my_event")
        return result

    registry = worker._Registry()
    orchestrator_name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID)]
    new_events = [
        helpers.new_suspend_event(),
        helpers.new_event_raised_event("my_event", encoded_input="42")]

    # Execute the orchestration. It should remain in a running state because it was suspended prior
    # to processing the event raised event.
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 0

    # Resume the orchestration. It should complete successfully.
    old_events = old_events + new_events
    new_events = [helpers.new_resume_event()]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result.value == "42"


def test_terminate():
    """Tests that an orchestration can be terminated before it completes"""

    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.wait_for_external_event("my_event")
        return result

    registry = worker._Registry()
    orchestrator_name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID)]
    new_events = [
        helpers.new_terminated_event(encoded_output=json.dumps("terminated!")),
        helpers.new_event_raised_event("my_event", encoded_input="42")]

    # Execute the orchestration. It should be in a running state waiting for an external event
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_TERMINATED
    assert complete_action.result.value == json.dumps("terminated!")


@pytest.mark.parametrize("save_events", [True, False])
def test_continue_as_new(save_events: bool):
    """Tests the behavior of the continue-as-new API"""
    def orchestrator(ctx: task.OrchestrationContext, input: int):
        yield ctx.create_timer(ctx.current_utc_datetime + timedelta(days=1))
        ctx.continue_as_new(input + 1, save_events=save_events)

    registry = worker._Registry()
    orchestrator_name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID, encoded_input="1"),
        helpers.new_event_raised_event("my_event", encoded_input="42"),
        helpers.new_event_raised_event("my_event", encoded_input="43"),
        helpers.new_event_raised_event("my_event", encoded_input="44"),
        helpers.new_timer_created_event(1, datetime.utcnow() + timedelta(days=1))]
    new_events = [
        helpers.new_timer_fired_event(1, datetime.utcnow() + timedelta(days=1))]

    # Execute the orchestration. It should be in a running state waiting for the timer to fire
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_CONTINUED_AS_NEW
    assert complete_action.result.value == json.dumps(2)
    assert len(complete_action.carryoverEvents) == (3 if save_events else 0)
    for i in range(len(complete_action.carryoverEvents)):
        event = complete_action.carryoverEvents[i]
        assert type(event) is pb.HistoryEvent
        assert event.HasField("eventRaised")
        assert event.eventRaised.name.casefold() == "my_event".casefold()  # event names are case-insensitive
        assert event.eventRaised.input.value == json.dumps(42 + i)


def test_fan_out():
    """Tests that a fan-out pattern correctly schedules N tasks"""
    def hello(_, name: str):
        return f"Hello {name}!"

    def orchestrator(ctx: task.OrchestrationContext, count: int):
        tasks = []
        for i in range(count):
            tasks.append(ctx.call_activity(hello, input=str(i)))
        results = yield task.when_all(tasks)
        return results

    registry = worker._Registry()
    orchestrator_name = registry.add_orchestrator(orchestrator)
    activity_name = registry.add_activity(hello)

    old_events = []
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID, encoded_input="10")]

    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    # The result should be 10 "taskScheduled" actions with inputs from 0 to 9
    assert len(actions) == 10
    for i in range(10):
        assert actions[i].HasField("scheduleTask")
        assert actions[i].scheduleTask.name == activity_name
        assert actions[i].scheduleTask.input.value == f'"{i}"'


def test_fan_in():
    """Tests that a fan-in pattern works correctly"""
    def print_int(_, val: int):
        return str(val)

    def orchestrator(ctx: task.OrchestrationContext, _):
        tasks = []
        for i in range(10):
            tasks.append(ctx.call_activity(print_int, input=i))
        results = yield task.when_all(tasks)
        return results

    registry = worker._Registry()
    orchestrator_name = registry.add_orchestrator(orchestrator)
    activity_name = registry.add_activity(print_int)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID, encoded_input=None)]
    for i in range(10):
        old_events.append(helpers.new_task_scheduled_event(
            i + 1, activity_name, encoded_input=str(i)))

    new_events = []
    for i in range(10):
        new_events.append(helpers.new_task_completed_event(
            i + 1, encoded_output=print_int(None, i)))

    # First, test with only the first 5 events. We expect the orchestration to be running
    # but return zero actions since its still waiting for the other 5 tasks to complete.
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events[:5])
    actions = result.actions
    assert len(actions) == 0

    # Now test with the full set of new events. We expect the orchestration to complete.
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result.value == "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]"


def test_fan_in_with_single_failure():
    """Tests that a fan-in pattern works correctly when one of the tasks fails"""
    def print_int(_, val: int):
        return str(val)

    def orchestrator(ctx: task.OrchestrationContext, _):
        tasks = []
        for i in range(10):
            tasks.append(ctx.call_activity(print_int, input=i))
        results = yield task.when_all(tasks)
        return results

    registry = worker._Registry()
    orchestrator_name = registry.add_orchestrator(orchestrator)
    activity_name = registry.add_activity(print_int)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID, encoded_input=None)]
    for i in range(10):
        old_events.append(helpers.new_task_scheduled_event(
            i + 1, activity_name, encoded_input=str(i)))

    # 5 of the tasks complete successfully, 1 fails, and 4 are still running.
    # The expectation is that the orchestration will fail immediately.
    new_events = []
    for i in range(5):
        new_events.append(helpers.new_task_completed_event(
            i + 1, encoded_output=print_int(None, i)))
    ex = Exception("Kah-BOOOOM!!!")
    new_events.append(helpers.new_task_failed_event(6, ex))

    # Now test with the full set of new events. We expect the orchestration to complete.
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == 'TaskFailedError'  # TODO: Is this the right error type?
    assert str(ex) in complete_action.failureDetails.errorMessage


def test_when_any():
    """Tests that a when_any pattern works correctly"""
    def hello(_, name: str):
        return f"Hello {name}!"

    def orchestrator(ctx: task.OrchestrationContext, _):
        t1 = ctx.call_activity(hello, input="Tokyo")
        t2 = ctx.call_activity(hello, input="Seattle")
        winner = yield task.when_any([t1, t2])
        if winner == t1:
            return t1.get_result()
        else:
            return t2.get_result()

    registry = worker._Registry()
    orchestrator_name = registry.add_orchestrator(orchestrator)
    activity_name = registry.add_activity(hello)

    # Test 1: Start the orchestration and let it yield on the when_any. We expect the orchestration
    # to return two actions: one to schedule the "Tokyo" task and one to schedule the "Seattle" task.
    old_events = []
    new_events = [helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID, encoded_input=None)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 2
    assert actions[0].HasField('scheduleTask')
    assert actions[1].HasField('scheduleTask')

    # The next tests assume that the orchestration has already awaited at the task.when_any()
    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, activity_name, encoded_input=json.dumps("Tokyo")),
        helpers.new_task_scheduled_event(2, activity_name, encoded_input=json.dumps("Seattle"))]

    # Test 2: Complete the "Tokyo" task. We expect the orchestration to complete with output "Hello, Tokyo!"
    encoded_output = json.dumps(hello(None, "Tokyo"))
    new_events = [helpers.new_task_completed_event(1, encoded_output)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result.value == encoded_output

    # Test 3: Complete the "Seattle" task. We expect the orchestration to complete with output "Hello, Seattle!"
    encoded_output = json.dumps(hello(None, "Seattle"))
    new_events = [helpers.new_task_completed_event(2, encoded_output)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    complete_action = get_and_validate_complete_orchestration_action_list(1, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result.value == encoded_output


def test_replay_safe_logger_suppresses_during_replay():
    """Validates that the replay-safe logger suppresses log messages during replay."""
    log_calls: list[str] = []

    class _RecordingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            log_calls.append(record.getMessage())

    inner_logger = logging.getLogger("test_replay_safe_logger")
    inner_logger.setLevel(logging.DEBUG)
    inner_logger.addHandler(_RecordingHandler())

    activity_name = "say_hello"

    def say_hello(_, name: str) -> str:
        return f"Hello, {name}!"

    def orchestrator(ctx: task.OrchestrationContext, _):
        replay_logger = ctx.create_replay_safe_logger(inner_logger)
        replay_logger.info("Starting orchestration")
        result = yield ctx.call_activity(say_hello, input="World")
        replay_logger.info("Activity completed: %s", result)
        return result

    registry = worker._Registry()
    activity_name = registry.add_activity(say_hello)
    orchestrator_name = registry.add_orchestrator(orchestrator)

    # First execution: starts the orchestration. The orchestrator runs without
    # replay, so both log calls should be emitted.
    new_events = [
        helpers.new_orchestrator_started_event(datetime.now()),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, [], new_events)
    assert result.actions  # should have scheduled the activity

    assert log_calls == ["Starting orchestration"]
    log_calls.clear()

    # Second execution: the orchestrator replays from history and then processes the
    # activity completion. The "Starting orchestration" message is emitted during
    # replay and should be suppressed; "Activity completed" is emitted after replay
    # ends and should appear exactly once.
    old_events = new_events + [
        helpers.new_task_scheduled_event(1, activity_name),
    ]
    encoded_output = json.dumps(say_hello(None, "World"))
    new_events = [helpers.new_task_completed_event(1, encoded_output)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    complete_action = get_and_validate_complete_orchestration_action_list(1, result.actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED

    assert log_calls == ["Activity completed: Hello, World!"]


def test_replay_safe_logger_all_levels():
    """Validates that all log levels are suppressed during replay and emitted otherwise."""
    log_levels: list[str] = []

    class _LevelRecorder(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            log_levels.append(record.levelname)

    inner_logger = logging.getLogger("test_replay_safe_logger_levels")
    inner_logger.setLevel(logging.DEBUG)
    inner_logger.addHandler(_LevelRecorder())

    def orchestrator(ctx: task.OrchestrationContext, _):
        replay_logger = ctx.create_replay_safe_logger(inner_logger)
        replay_logger.debug("debug msg")
        replay_logger.info("info msg")
        replay_logger.warning("warning msg")
        replay_logger.error("error msg")
        replay_logger.critical("critical msg")
        return "done"

    registry = worker._Registry()
    orchestrator_name = registry.add_orchestrator(orchestrator)

    new_events = [
        helpers.new_orchestrator_started_event(datetime.now()),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, [], new_events)
    complete_action = get_and_validate_complete_orchestration_action_list(1, result.actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED

    assert log_levels == ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def test_replay_safe_logger_direct():
    """Unit test for ReplaySafeLogger — verifies suppression based on is_replaying flag."""
    log_calls: list[str] = []

    class _RecordingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            log_calls.append(record.getMessage())

    inner_logger = logging.getLogger("test_replay_safe_logger_direct")
    inner_logger.setLevel(logging.DEBUG)
    inner_logger.addHandler(_RecordingHandler())

    replaying = True
    replay_logger = task.ReplaySafeLogger(inner_logger, lambda: replaying)

    replay_logger.info("should be suppressed")
    assert log_calls == []

    replaying = False
    replay_logger.info("should appear")
    assert log_calls == ["should appear"]


def test_replay_safe_logger_log_method():
    """Validates the generic log() method respects the replay flag."""
    log_calls: list[str] = []

    class _RecordingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            log_calls.append(record.getMessage())

    inner_logger = logging.getLogger("test_replay_safe_logger_log_method")
    inner_logger.setLevel(logging.DEBUG)
    inner_logger.addHandler(_RecordingHandler())

    replaying = True
    replay_logger = task.ReplaySafeLogger(inner_logger, lambda: replaying)

    replay_logger.log(logging.WARNING, "suppressed warning")
    assert log_calls == []

    replaying = False
    replay_logger.log(logging.WARNING, "visible warning")
    assert log_calls == ["visible warning"]


def test_replay_safe_logger_is_enabled_for():
    """Validates isEnabledFor returns False during replay."""
    inner_logger = logging.getLogger("test_replay_safe_logger_enabled")
    inner_logger.setLevel(logging.DEBUG)

    replaying = True
    replay_logger = task.ReplaySafeLogger(inner_logger, lambda: replaying)

    # During replay, isEnabledFor should always return False
    assert replay_logger.isEnabledFor(logging.DEBUG) is False
    assert replay_logger.isEnabledFor(logging.INFO) is False
    assert replay_logger.isEnabledFor(logging.CRITICAL) is False

    # After replay, delegates to the inner logger
    replaying = False
    assert replay_logger.isEnabledFor(logging.DEBUG) is True
    assert replay_logger.isEnabledFor(logging.INFO) is True

    # If a level is below the inner logger's level, should return False
    inner_logger.setLevel(logging.WARNING)
    assert replay_logger.isEnabledFor(logging.DEBUG) is False
    assert replay_logger.isEnabledFor(logging.WARNING) is True


def test_when_any_with_retry():
    """Tests that a when_any pattern works correctly with retries"""
    def dummy_activity(_, inp: str):
        if inp == "Tokyo":
            raise ValueError("Kah-BOOOOM!!!")
        return f"Hello {inp}!"

    def orchestrator(ctx: task.OrchestrationContext, _):
        t1 = ctx.call_activity(dummy_activity,
                               retry_policy=task.RetryPolicy(
                                   first_retry_interval=timedelta(seconds=1),
                                   max_number_of_attempts=6,
                                   backoff_coefficient=2,
                                   max_retry_interval=timedelta(seconds=10),
                                   retry_timeout=timedelta(seconds=50)),
                               input="Tokyo")
        t2 = ctx.call_activity(dummy_activity, input="Seattle")
        winner = yield task.when_any([t1, t2])
        if winner == t1:
            return t1.get_result()
        else:
            return t2.get_result()

    registry = worker._Registry()
    orchestrator_name = registry.add_orchestrator(orchestrator)
    registry.add_activity(dummy_activity)

    current_timestamp = datetime.utcnow()
    # Simulate the task failing for the first time and confirm that a timer is scheduled for 1 second in the future
    old_events = [
        helpers.new_orchestrator_started_event(timestamp=current_timestamp),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, task.get_name(dummy_activity)),
        helpers.new_task_scheduled_event(2, task.get_name(dummy_activity))]
    expected_fire_at = current_timestamp + timedelta(seconds=1)

    new_events = [
        helpers.new_orchestrator_started_event(timestamp=current_timestamp),
        helpers.new_task_failed_event(1, ValueError("Kah-BOOOOM!!!"))]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 1
    assert actions[0].HasField("createTimer")
    assert actions[0].createTimer.fireAt.ToDatetime() == expected_fire_at
    assert actions[0].id == 3

    # Simulate the timer firing at the expected time and confirm that another activity task is scheduled
    current_timestamp = expected_fire_at
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_timer_fired_event(3, current_timestamp)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 2
    assert actions[1].HasField("scheduleTask")
    assert actions[1].id == 1

    # Simulate the task failing for the second time and confirm that a timer is scheduled for 2 seconds in the future
    old_events = old_events + new_events
    expected_fire_at = current_timestamp + timedelta(seconds=2)
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_task_failed_event(1, ValueError("Kah-BOOOOM!!!"))]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 3
    assert actions[2].HasField("createTimer")
    assert actions[2].createTimer.fireAt.ToDatetime() == expected_fire_at
    assert actions[2].id == 4

    # Complete the "Seattle" task. We expect the orchestration to complete with output "Hello, Seattle!"
    encoded_output = json.dumps(dummy_activity(None, "Seattle"))
    new_events = [helpers.new_task_completed_event(2, encoded_output)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    complete_action = get_and_validate_complete_orchestration_action_list(3, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result.value == encoded_output


def test_when_all_with_retry():
    """Tests that a when_all pattern works correctly with retries"""
    def dummy_activity(ctx, inp: str):
        if inp == "Tokyo":
            raise ValueError("Kah-BOOOOM!!!")
        return f"Hello {inp}!"

    def orchestrator(ctx: task.OrchestrationContext, _):
        t1 = ctx.call_activity(dummy_activity,
                               retry_policy=task.RetryPolicy(
                                   first_retry_interval=timedelta(seconds=2),
                                   max_number_of_attempts=3,
                                   backoff_coefficient=4,
                                   max_retry_interval=timedelta(seconds=5),
                                   retry_timeout=timedelta(seconds=50)),
                               input="Tokyo")
        t2 = ctx.call_activity(dummy_activity, input="Seattle")
        results = yield task.when_all([t1, t2])
        return results

    registry = worker._Registry()
    orchestrator_name = registry.add_orchestrator(orchestrator)
    registry.add_activity(dummy_activity)

    current_timestamp = datetime.utcnow()
    # Simulate the task failing for the first time and confirm that a timer is scheduled for 2 seconds in the future
    old_events = [
        helpers.new_orchestrator_started_event(timestamp=current_timestamp),
        helpers.new_execution_started_event(orchestrator_name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, task.get_name(dummy_activity)),
        helpers.new_task_scheduled_event(2, task.get_name(dummy_activity))]
    expected_fire_at = current_timestamp + timedelta(seconds=2)

    new_events = [
        helpers.new_orchestrator_started_event(timestamp=current_timestamp),
        helpers.new_task_failed_event(1, ValueError("Kah-BOOOOM!!!"))]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 1
    assert actions[0].HasField("createTimer")
    assert actions[0].createTimer.fireAt.ToDatetime() == expected_fire_at
    assert actions[0].id == 3

    # Simulate the timer firing at the expected time and confirm that another activity task is scheduled
    current_timestamp = expected_fire_at
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_timer_fired_event(3, current_timestamp)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 2
    assert actions[1].HasField("scheduleTask")
    assert actions[1].id == 1

    # Simulate the task failing for the second time and confirm that a timer is scheduled for 5 seconds in the future
    old_events = old_events + new_events
    expected_fire_at = current_timestamp + timedelta(seconds=5)
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_task_failed_event(1, ValueError("Kah-BOOOOM!!!"))]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 3
    assert actions[2].HasField("createTimer")
    assert actions[2].createTimer.fireAt.ToDatetime() == expected_fire_at
    assert actions[2].id == 4

    # Complete the "Seattle" task.
    # And, Simulate the timer firing at the expected time and confirm that another activity task is scheduled
    encoded_output = json.dumps(dummy_activity(None, "Seattle"))
    old_events = old_events + new_events
    new_events = [helpers.new_task_completed_event(2, encoded_output),
                  helpers.new_timer_fired_event(4, expected_fire_at)]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    assert len(actions) == 3
    assert actions[1].HasField("scheduleTask")
    assert actions[1].id == 1

    ex = ValueError("Kah-BOOOOM!!!")

    # Simulate the task failing for the third time. Overall workflow should fail at this point.
    old_events = old_events + new_events
    new_events = [
        helpers.new_orchestrator_started_event(current_timestamp),
        helpers.new_task_failed_event(1, ValueError("Kah-BOOOOM!!!"))]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions
    complete_action = get_and_validate_complete_orchestration_action_list(4, actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == 'TaskFailedError'  # TODO: Should this be the specific error?
    assert str(ex) in complete_action.failureDetails.errorMessage


def test_orchestrator_completed_no_effect():
    def dummy_activity(ctx, _):
        pass

    def orchestrator(ctx: task.OrchestrationContext, orchestrator_input):
        yield ctx.call_activity(dummy_activity, input=orchestrator_input)

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    encoded_input = json.dumps(42)
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input),
        helpers.new_orchestrator_completed_event()]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, [], new_events)
    actions = result.actions

    assert len(actions) == 1
    assert type(actions[0]) is pb.OrchestratorAction
    assert actions[0].id == 1
    assert actions[0].HasField("scheduleTask")
    assert actions[0].scheduleTask.name == task.get_name(dummy_activity)
    assert actions[0].scheduleTask.input.value == encoded_input


def test_entity_lock_created_as_event():
    test_entity_id = entities.EntityInstanceId("Counter", "myCounter")

    def orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = test_entity_id
        with (yield ctx.lock_entities([entity_id])):
            return (yield ctx.call_entity(entity_id, "set", 1))

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, None),
    ]

    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result1 = executor.execute(TEST_INSTANCE_ID, [], new_events)
    actions = result1.actions
    assert len(actions) == 1
    assert type(actions[0]) is pb.OrchestratorAction
    assert actions[0].id == 1
    assert actions[0].HasField("sendEntityMessage")
    assert actions[0].sendEntityMessage.HasField("entityLockRequested")

    old_events = new_events
    event_sent_input = {
        "id": actions[0].sendEntityMessage.entityLockRequested.criticalSectionId,
    }
    new_events = [
        helpers.new_event_sent_event(1, str(test_entity_id), json.dumps(event_sent_input)),
        helpers.new_event_raised_event(event_sent_input["id"], None),
    ]
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    assert len(actions) == 1
    assert type(actions[0]) is pb.OrchestratorAction
    assert actions[0].id == 2
    assert actions[0].HasField("sendEntityMessage")
    assert actions[0].sendEntityMessage.HasField("entityOperationCalled")
    assert actions[0].sendEntityMessage.entityOperationCalled.targetInstanceId.value == str(test_entity_id)


def get_and_validate_complete_orchestration_action_list(expected_action_count: int, actions: list[pb.OrchestratorAction]) -> pb.CompleteOrchestrationAction:
    assert len(actions) == expected_action_count
    assert type(actions[-1]) is pb.OrchestratorAction
    assert actions[-1].HasField("completeOrchestration")
    return actions[-1].completeOrchestration
