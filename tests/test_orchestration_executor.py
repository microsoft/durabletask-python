from typing import List
import durabletask.protos.helpers as helpers
import durabletask.protos.orchestrator_service_pb2 as pb
import logging
import simplejson as json

from datetime import datetime, timedelta
from durabletask.task.execution import OrchestrationExecutor
from durabletask.task.orchestration import OrchestrationContext
from durabletask.task.registry import Registry, get_name
from durabletask.task.task import CompletableTask

logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.DEBUG)
TEST_LOGGER = logging.getLogger("tests")

TEST_INSTANCE_ID = "abc123"


def test_orchestrator_inputs():
    """Validates orchestrator function input population"""

    def orchestrator(ctx: OrchestrationContext, my_input: int):
        return my_input, ctx.instance_id, str(ctx.current_utc_datetime), ctx.is_replaying

    test_input = 42

    registry = Registry()
    name = registry.add_orchestrator(orchestrator)

    start_time = datetime.now()
    new_events = [
        helpers.new_orchestrator_started_event(start_time),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=json.dumps(test_input)),
    ]
    executor = OrchestrationExecutor(registry, TEST_LOGGER)
    actions = executor.execute(TEST_INSTANCE_ID, [], new_events)

    complete_action = get_and_validate_single_complete_orchestration_action(actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result is not None

    expected_output = [test_input, TEST_INSTANCE_ID, str(start_time), False]
    assert complete_action.result.value == json.dumps(expected_output)


def test_complete_orchestration_actions():
    """Tests the actions output for a completed orchestration"""

    def empty_orchestrator(ctx: OrchestrationContext, _):
        return "done"

    registry = Registry()
    name = registry.add_orchestrator(empty_orchestrator)

    new_events = [helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None)]
    executor = OrchestrationExecutor(registry, TEST_LOGGER)
    actions = executor.execute(TEST_INSTANCE_ID, [], new_events)

    complete_action = get_and_validate_single_complete_orchestration_action(actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result.value == '"done"'  # results are JSON-encoded


def test_orchestrator_not_registered():
    """Tests the effect of scheduling an unregistered orchestrator"""

    registry = Registry()
    name = "Bogus"
    new_events = [helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None)]
    executor = OrchestrationExecutor(registry, TEST_LOGGER)
    actions = executor.execute(TEST_INSTANCE_ID, [], new_events)

    complete_action = get_and_validate_single_complete_orchestration_action(actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == "OrchestratorNotRegisteredError"
    assert complete_action.failureDetails.errorMessage


def test_create_timer_actions():
    """Tests the actions output for the create_timer orchestrator method"""

    def delay_orchestrator(ctx: OrchestrationContext, _):
        due_time = ctx.current_utc_datetime + timedelta(seconds=1)
        yield ctx.create_timer(due_time)
        return "done"

    registry = Registry()
    name = registry.add_orchestrator(delay_orchestrator)

    start_time = datetime(2020, 1, 1, 12, 0, 0)
    expected_fire_at = start_time + timedelta(seconds=1)

    new_events = [
        helpers.new_orchestrator_started_event(start_time),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None)]
    executor = OrchestrationExecutor(registry, TEST_LOGGER)
    actions = executor.execute(TEST_INSTANCE_ID, [], new_events)

    assert actions is not None
    assert len(actions) == 1
    assert type(actions[0]) is pb.OrchestratorAction
    assert actions[0].id == 1
    assert actions[0].HasField("createTimer")
    assert actions[0].createTimer.fireAt.ToDatetime() == expected_fire_at


def test_timer_fired_completion():
    """Tests the resumption of task using a timer_fired event"""

    def delay_orchestrator(ctx: OrchestrationContext, _):
        due_time = ctx.current_utc_datetime + timedelta(seconds=1)
        yield ctx.create_timer(due_time)
        return "done"

    registry = Registry()
    name = registry.add_orchestrator(delay_orchestrator)

    start_time = datetime(2020, 1, 1, 12, 0, 0)
    expected_fire_at = start_time + timedelta(seconds=1)

    old_events = [
        helpers.new_orchestrator_started_event(start_time),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_timer_created_event(1, expected_fire_at)]
    new_events = [
        helpers.new_timer_fired_event(1, expected_fire_at)]

    executor = OrchestrationExecutor(registry, TEST_LOGGER)
    actions = executor.execute(TEST_INSTANCE_ID, old_events, new_events)

    complete_action = get_and_validate_single_complete_orchestration_action(actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result is not None
    assert complete_action.result.value == '"done"'  # results are JSON-encoded


def test_schedule_activity_actions():
    """Test the actions output for the call_activity orchestrator method"""
    def dummy_activity(ctx, _):
        pass

    def orchestrator(ctx: OrchestrationContext, orchestrator_input):
        yield ctx.call_activity(dummy_activity, input=orchestrator_input)

    registry = Registry()
    name = registry.add_orchestrator(orchestrator)

    # TODO: Test several different input types (int, bool, str, dict, etc.)
    encoded_input = json.dumps(42)
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input)]
    executor = OrchestrationExecutor(registry, TEST_LOGGER)
    actions = executor.execute(TEST_INSTANCE_ID, [], new_events)

    assert len(actions) == 1
    assert type(actions[0]) is pb.OrchestratorAction
    assert actions[0].id == 1
    assert actions[0].HasField("scheduleTask")
    assert actions[0].scheduleTask.name == get_name(dummy_activity)
    assert actions[0].scheduleTask.input.value == encoded_input


def test_activity_task_completion():
    """Tests the successful completion of an activity task"""

    def dummy_activity(ctx, _):
        pass

    def orchestrator(ctx: OrchestrationContext, orchestrator_input):
        result = yield ctx.call_activity(dummy_activity, input=orchestrator_input)
        return result

    registry = Registry()
    name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, get_name(dummy_activity))]

    encoded_output = json.dumps("done!")
    new_events = [helpers.new_task_completed_event(1, encoded_output)]

    executor = OrchestrationExecutor(registry, TEST_LOGGER)
    actions = executor.execute(TEST_INSTANCE_ID, old_events, new_events)

    complete_action = get_and_validate_single_complete_orchestration_action(actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert complete_action.result.value == encoded_output


def test_activity_task_failed():
    """Tests the failure of an activity task"""
    def dummy_activity(ctx, _):
        pass

    def orchestrator(ctx: OrchestrationContext, orchestrator_input):
        result = yield ctx.call_activity(dummy_activity, input=orchestrator_input)
        return result

    registry = Registry()
    name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, get_name(dummy_activity))]

    ex = Exception("Kah-BOOOOM!!!")
    new_events = [helpers.new_task_failed_event(1, ex)]

    executor = OrchestrationExecutor(registry, TEST_LOGGER)
    actions = executor.execute(TEST_INSTANCE_ID, old_events, new_events)

    complete_action = get_and_validate_single_complete_orchestration_action(actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == 'TaskFailedError'
    assert complete_action.failureDetails.errorMessage == str(ex)

    # Make sure the line of code where the exception was raised is included in the stack trace
    user_code_statement = "ctx.call_activity(dummy_activity, input=orchestrator_input)"
    assert user_code_statement in complete_action.failureDetails.stackTrace.value


def test_nondeterminism_expected_timer():
    """Tests the non-determinism detection logic when call_timer is expected but some other method (call_activity) is called instead"""
    def dummy_activity(ctx, _):
        pass

    def orchestrator(ctx: OrchestrationContext, _):
        result = yield ctx.call_activity(dummy_activity)
        return result

    registry = Registry()
    name = registry.add_orchestrator(orchestrator)

    fire_at = datetime.now()
    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_timer_created_event(1, fire_at)]
    new_events = [helpers.new_timer_fired_event(timer_id=1, fire_at=fire_at)]

    executor = OrchestrationExecutor(registry, TEST_LOGGER)
    actions = executor.execute(TEST_INSTANCE_ID, old_events, new_events)

    complete_action = get_and_validate_single_complete_orchestration_action(actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == 'NonDeterminismError'
    assert "1" in complete_action.failureDetails.errorMessage  # task ID
    assert "create_timer" in complete_action.failureDetails.errorMessage  # expected method name
    assert "call_activity" in complete_action.failureDetails.errorMessage  # actual method name


def test_nondeterminism_expected_activity_call_no_task_id():
    """Tests the non-determinism detection logic when invoking activity functions"""
    def orchestrator(ctx: OrchestrationContext, _):
        result = yield CompletableTask()  # dummy task
        return result

    registry = Registry()
    name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, "bogus_activity")]

    new_events = [helpers.new_task_completed_event(1)]

    executor = OrchestrationExecutor(registry, TEST_LOGGER)
    actions = executor.execute(TEST_INSTANCE_ID, old_events, new_events)

    complete_action = get_and_validate_single_complete_orchestration_action(actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == 'NonDeterminismError'
    assert "1" in complete_action.failureDetails.errorMessage  # task ID
    assert "call_activity" in complete_action.failureDetails.errorMessage  # expected method name


def test_nondeterminism_expected_activity_call_wrong_task_type():
    """Tests the non-determinism detection when an activity exists in the history but a non-activity is in the code"""
    def dummy_activity(ctx, _):
        pass

    def orchestrator(ctx: OrchestrationContext, _):
        # create a timer when the history expects an activity call
        yield ctx.create_timer(datetime.now())

    registry = Registry()
    name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, get_name(dummy_activity))]

    new_events = [helpers.new_task_completed_event(1)]

    executor = OrchestrationExecutor(registry, TEST_LOGGER)
    actions = executor.execute(TEST_INSTANCE_ID, old_events, new_events)

    complete_action = get_and_validate_single_complete_orchestration_action(actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == 'NonDeterminismError'
    assert "1" in complete_action.failureDetails.errorMessage  # task ID
    assert "call_activity" in complete_action.failureDetails.errorMessage  # expected method name
    assert "create_timer" in complete_action.failureDetails.errorMessage  # unexpected method name


def test_nondeterminism_wrong_activity_name():
    """Tests the non-determinism detection when calling an activity with a name that differs from the name in the history"""
    def dummy_activity(ctx, _):
        pass

    def orchestrator(ctx: OrchestrationContext, _):
        # create a timer when the history expects an activity call
        yield ctx.call_activity(dummy_activity)

    registry = Registry()
    name = registry.add_orchestrator(orchestrator)

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_task_scheduled_event(1, "original_activity")]

    new_events = [helpers.new_task_completed_event(1)]

    executor = OrchestrationExecutor(registry, TEST_LOGGER)
    actions = executor.execute(TEST_INSTANCE_ID, old_events, new_events)

    complete_action = get_and_validate_single_complete_orchestration_action(actions)
    assert complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert complete_action.failureDetails.errorType == 'NonDeterminismError'
    assert "1" in complete_action.failureDetails.errorMessage  # task ID
    assert "call_activity" in complete_action.failureDetails.errorMessage  # expected method name
    assert "original_activity" in complete_action.failureDetails.errorMessage  # expected activity name
    assert "dummy_activity" in complete_action.failureDetails.errorMessage  # unexpected activity name


def get_and_validate_single_complete_orchestration_action(actions: List[pb.OrchestratorAction]) -> pb.CompleteOrchestrationAction:
    assert len(actions) == 1
    assert type(actions[0]) is pb.OrchestratorAction
    assert actions[0].HasField("completeOrchestration")
    return actions[0].completeOrchestration
