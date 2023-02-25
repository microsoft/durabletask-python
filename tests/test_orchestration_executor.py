import durabletask.protos.helpers as helpers
import durabletask.protos.orchestrator_service_pb2 as protos

from datetime import datetime, timedelta

from durabletask.task.orchestration_executor import OrchestrationExecutor
from durabletask.task.orchestrator import OrchestrationContext
from durabletask.task.registry import Registry


def test_complete_orchestration_actions():
    """Tests the actions output for a completed orchestration"""

    def empty_orchestrator(ctx: OrchestrationContext):
        return "done"

    registry = Registry()
    name = registry.add_orchestrator(empty_orchestrator)

    newEvents = [helpers.new_execution_started_event(name, instance_id="abc123", input=None)]
    executor = OrchestrationExecutor(registry)
    actions = executor.execute("abc123", [], newEvents)

    assert actions is not None
    assert len(actions) == 1
    assert type(actions[0]) is protos.OrchestratorAction
    assert actions[0].HasField("completeOrchestration")
    assert actions[0].completeOrchestration.orchestrationStatus == protos.ORCHESTRATION_STATUS_COMPLETED
    assert actions[0].completeOrchestration.result is not None
    assert actions[0].completeOrchestration.result.value == '"done"'  # results are JSON-encoded


def test_unknown_orchestrator():
    """Tests the effect of scheduling an unknown orchestrator"""

    registry = Registry()
    name = "Bogus"
    newEvents = [helpers.new_execution_started_event(name, instance_id="abc123", input=None)]
    executor = OrchestrationExecutor(registry)
    actions = executor.execute("abc123", [], newEvents)

    assert actions is not None
    assert len(actions) == 1
    assert type(actions[0]) is protos.OrchestratorAction
    assert actions[0].HasField("completeOrchestration")
    assert actions[0].completeOrchestration.orchestrationStatus == protos.ORCHESTRATION_STATUS_FAILED
    assert actions[0].completeOrchestration.failureDetails is not None
    assert actions[0].completeOrchestration.failureDetails.errorType == "OrchestratorNotFound"
    assert name in actions[0].completeOrchestration.failureDetails.errorMessage


def test_create_timer_actions():
    """Tests the actions output for the create_timer orchestrator method"""

    def delay_orchestrator(ctx: OrchestrationContext):
        due_time = ctx.current_utc_datetime + timedelta(seconds=1)
        yield ctx.create_timer(due_time)
        return "done"

    registry = Registry()
    name = registry.add_orchestrator(delay_orchestrator)

    start_time = datetime(2020, 1, 1, 12, 0, 0)
    expected_fire_at = start_time + timedelta(seconds=1)

    newEvents = [
        helpers.new_orchestrator_started_event(start_time),
        helpers.new_execution_started_event(name, instance_id="abc123", input=None)]
    executor = OrchestrationExecutor(registry)
    actions = executor.execute("abc123", [], newEvents)

    assert actions is not None
    assert len(actions) == 1
    assert type(actions[0]) is protos.OrchestratorAction
    assert actions[0].id == 1
    assert actions[0].HasField("createTimer")
    assert actions[0].createTimer.fireAt.ToDatetime() == expected_fire_at


def test_timer_fired_completion():
    """Tests the resumption of task using a timer_fired event"""

    def delay_orchestrator(ctx: OrchestrationContext):
        due_time = ctx.current_utc_datetime + timedelta(seconds=1)
        yield ctx.create_timer(due_time)
        return "done"

    registry = Registry()
    name = registry.add_orchestrator(delay_orchestrator)

    start_time = datetime(2020, 1, 1, 12, 0, 0)
    expected_fire_at = start_time + timedelta(seconds=1)

    oldEvents = [
        helpers.new_orchestrator_started_event(start_time),
        helpers.new_execution_started_event(name, instance_id="abc123", input=None),
        helpers.new_timer_created_event(1, expected_fire_at)]
    newEvents = [
        helpers.new_timer_fired_event(1, expected_fire_at)]

    executor = OrchestrationExecutor(registry)
    actions = executor.execute("abc123", oldEvents, newEvents)

    assert actions is not None
    assert len(actions) == 1
    assert type(actions[0]) is protos.OrchestratorAction
    assert actions[0].HasField("completeOrchestration")
    assert actions[0].completeOrchestration.orchestrationStatus == protos.ORCHESTRATION_STATUS_COMPLETED
    assert actions[0].completeOrchestration.result is not None
    assert actions[0].completeOrchestration.result.value == '"done"'  # results are JSON-encoded
