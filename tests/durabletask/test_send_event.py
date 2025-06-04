# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import logging

import durabletask.internal.helpers as helpers
import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask import task, worker

logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.DEBUG)
TEST_LOGGER = logging.getLogger("tests")

TEST_INSTANCE_ID = "abc123"


def test_send_event_action():
    """Test that send_event creates the correct action"""

    def orchestrator(ctx: task.OrchestrationContext, _):
        ctx.send_event("target_instance", "my_event", data="test_data")
        return "completed"

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, [], new_events)
    actions = result.actions

    # Should have two actions: send_event and completion
    assert len(actions) == 2
    assert type(actions[0]) is pb.OrchestratorAction
    assert type(actions[1]) is pb.OrchestratorAction

    # First action should be send_event
    action = actions[0]
    assert action.WhichOneof("orchestratorActionType") == "sendEvent"
    assert action.id == 1

    send_action = action.sendEvent
    assert send_action.instance.instanceId == "target_instance"
    assert send_action.name == "my_event"
    assert send_action.data.value == json.dumps("test_data")

    # Second action should be completion
    completion_action = actions[1]
    assert completion_action.WhichOneof("orchestratorActionType") == "completeOrchestration"
    assert completion_action.completeOrchestration.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED


def test_send_event_with_no_data():
    """Test send_event with no data parameter"""

    def orchestrator(ctx: task.OrchestrationContext, _):
        ctx.send_event("target_instance", "my_event")
        return "completed"

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, [], new_events)
    actions = result.actions

    # Should have two actions: send_event and completion
    assert len(actions) == 2
    action = actions[0]
    assert action.WhichOneof("orchestratorActionType") == "sendEvent"
    send_action = action.sendEvent
    assert send_action.instance.instanceId == "target_instance"
    assert send_action.name == "my_event"
    # data should be None/empty when no data is provided
    assert not send_action.HasField("data") or send_action.data.value == ""

    # Second action should be completion
    completion_action = actions[1]
    assert completion_action.WhichOneof("orchestratorActionType") == "completeOrchestration"


def test_send_event_multiple():
    """Test sending multiple events in sequence"""

    def orchestrator(ctx: task.OrchestrationContext, _):
        ctx.send_event("target1", "event1", data="data1")
        ctx.send_event("target2", "event2", data="data2")
        return "completed"

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, [], new_events)
    actions = result.actions

    # Should have two actions for both send_event calls and one completion action
    assert len(actions) == 3

    # First action: send_event to target1
    action1 = actions[0]
    assert action1.WhichOneof("orchestratorActionType") == "sendEvent"
    assert action1.sendEvent.instance.instanceId == "target1"
    assert action1.sendEvent.name == "event1"
    assert action1.sendEvent.data.value == json.dumps("data1")

    # Second action: send_event to target2
    action2 = actions[1]
    assert action2.WhichOneof("orchestratorActionType") == "sendEvent"
    assert action2.sendEvent.instance.instanceId == "target2"
    assert action2.sendEvent.name == "event2"
    assert action2.sendEvent.data.value == json.dumps("data2")

    # Third action: completion
    action3 = actions[2]
    assert action3.WhichOneof("orchestratorActionType") == "completeOrchestration"
    assert action3.completeOrchestration.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED


def test_send_event_with_various_data_types():
    """Test send_event with different data types"""

    def orchestrator(ctx: task.OrchestrationContext, _):
        # Test with dict
        ctx.send_event("target1", "event1", data={"key": "value", "number": 42})
        # Test with list
        ctx.send_event("target2", "event2", data=[1, 2, 3])
        # Test with number
        ctx.send_event("target3", "event3", data=123)
        # Test with boolean
        ctx.send_event("target4", "event4", data=True)
        return "completed"

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, [], new_events)
    actions = result.actions

    # Should have four send_event actions and one completion action
    assert len(actions) == 5

    # First action: dict data
    action1 = actions[0]
    assert action1.WhichOneof("orchestratorActionType") == "sendEvent"
    assert action1.sendEvent.instance.instanceId == "target1"
    assert action1.sendEvent.name == "event1"
    expected_data = json.dumps({"key": "value", "number": 42})
    assert action1.sendEvent.data.value == expected_data

    # Second action: list data
    action2 = actions[1]
    assert action2.WhichOneof("orchestratorActionType") == "sendEvent"
    assert action2.sendEvent.instance.instanceId == "target2"
    assert action2.sendEvent.name == "event2"
    assert action2.sendEvent.data.value == json.dumps([1, 2, 3])

    # Third action: number data
    action3 = actions[2]
    assert action3.WhichOneof("orchestratorActionType") == "sendEvent"
    assert action3.sendEvent.instance.instanceId == "target3"
    assert action3.sendEvent.name == "event3"
    assert action3.sendEvent.data.value == json.dumps(123)

    # Fourth action: boolean data
    action4 = actions[3]
    assert action4.WhichOneof("orchestratorActionType") == "sendEvent"
    assert action4.sendEvent.instance.instanceId == "target4"
    assert action4.sendEvent.name == "event4"
    assert action4.sendEvent.data.value == json.dumps(True)

    # Fifth action: completion
    action5 = actions[4]
    assert action5.WhichOneof("orchestratorActionType") == "completeOrchestration"
    assert action5.completeOrchestration.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED


def test_send_event_validation():
    """Test send_event input validation"""

    def orchestrator_empty_instance(ctx: task.OrchestrationContext, _):
        ctx.send_event("", "event1", data="test")
        return "completed"

    registry = worker._Registry()

    # Test empty instance_id
    name1 = registry.add_orchestrator(orchestrator_empty_instance)
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name1, TEST_INSTANCE_ID, encoded_input=None),
    ]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)

    result = executor.execute(TEST_INSTANCE_ID, [], new_events)

    # Check if the orchestration failed due to validation error
    actions = result.actions
    if len(actions) > 0:
        action = actions[0]
        if action.WhichOneof("orchestratorActionType") == "completeOrchestration":
            complete_action = action.completeOrchestration
            if complete_action.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED:
                # The orchestration should have failed with the validation error
                failure_details = complete_action.failureDetails
                assert "instance_id cannot be None or empty" in failure_details.errorMessage
            else:
                assert False, "Expected orchestration to fail with validation error"
        else:
            assert False, "Expected failure completion action, got different action type"
    else:
        assert False, "Expected at least one action (failure completion)"


def test_orchestration_to_orchestration_communication():
    """Test advanced scenario: orchestration sends event to another waiting orchestration"""

    # Define the waiting orchestration that waits for an approval event
    def waiting_orchestration(ctx: task.OrchestrationContext, _):
        approval_data = yield ctx.wait_for_external_event("approval")
        return f"Received approval: {approval_data}"

    # Define the sender orchestration that sends an event to another orchestration
    def sender_orchestration(ctx: task.OrchestrationContext, target_instance_id: str):
        approval_payload = {"approved": True, "approver": "manager", "timestamp": "2024-01-01T10:00:00Z"}
        ctx.send_event(target_instance_id, "approval", data=approval_payload)
        return "Event sent successfully"

    registry = worker._Registry()
    waiting_name = registry.add_orchestrator(waiting_orchestration)
    sender_name = registry.add_orchestrator(sender_orchestration)
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)

    # Instance IDs for our orchestrations
    waiting_instance_id = "waiting-instance-123"
    sender_instance_id = "sender-instance-456"

    # Step 1: Start the waiting orchestration
    waiting_new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(waiting_name, waiting_instance_id, encoded_input=None),
    ]
    waiting_result = executor.execute(waiting_instance_id, [], waiting_new_events)

    # The waiting orchestration should produce no actions when waiting for an external event
    assert len(waiting_result.actions) == 0

    # Step 2: Start the sender orchestration with the waiting instance ID as input
    sender_new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(sender_name, sender_instance_id,
                                            encoded_input=json.dumps(waiting_instance_id)),
    ]
    sender_result = executor.execute(sender_instance_id, [], sender_new_events)

    # The sender orchestration should produce a send_event action and complete immediately
    assert len(sender_result.actions) == 2
    send_action = sender_result.actions[0]
    assert send_action.WhichOneof("orchestratorActionType") == "sendEvent"
    assert send_action.sendEvent.instance.instanceId == waiting_instance_id
    assert send_action.sendEvent.name == "approval"

    # Verify the data payload is correct
    expected_payload = {"approved": True, "approver": "manager", "timestamp": "2024-01-01T10:00:00Z"}
    assert send_action.sendEvent.data.value == json.dumps(expected_payload)

    # The sender should also complete successfully in the same execution
    sender_complete_action = sender_result.actions[1]
    assert sender_complete_action.WhichOneof("orchestratorActionType") == "completeOrchestration"
    assert sender_complete_action.completeOrchestration.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert sender_complete_action.completeOrchestration.result.value == json.dumps("Event sent successfully")

    # Step 3: Simulate the event being raised to the waiting orchestration
    event_raised = helpers.new_event_raised_event("approval", json.dumps(expected_payload))

    waiting_old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(waiting_name, waiting_instance_id, encoded_input=None)
    ]
    waiting_completion_result = executor.execute(waiting_instance_id, waiting_old_events, [event_raised])

    # The waiting orchestration should complete with the received data
    assert len(waiting_completion_result.actions) == 1
    waiting_complete_action = waiting_completion_result.actions[0]
    assert waiting_complete_action.WhichOneof("orchestratorActionType") == "completeOrchestration"
    assert waiting_complete_action.completeOrchestration.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED

    # Verify the data was passed correctly through the event
    expected_result = f"Received approval: {expected_payload}"
    assert waiting_complete_action.completeOrchestration.result.value == json.dumps(expected_result)
