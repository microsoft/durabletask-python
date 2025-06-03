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
        yield ctx.send_event("target_instance", "my_event", data="test_data")
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

    # Should have one action for send_event
    assert len(actions) == 1
    assert type(actions[0]) is pb.OrchestratorAction

    action = actions[0]
    assert action.WhichOneof("orchestratorActionType") == "sendEvent"
    assert action.id == 1

    send_action = action.sendEvent
    assert send_action.instance.instanceId == "target_instance"
    assert send_action.name == "my_event"
    assert send_action.data.value == json.dumps("test_data")


def test_send_event_completion():
    """Test that send_event can complete successfully"""

    def orchestrator(ctx: task.OrchestrationContext, _):
        result = yield ctx.send_event("target_instance", "my_event", data="test_data")
        return result

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)

    # First execution - should schedule the send_event
    old_events = []
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None)
    ]
    executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    assert len(actions) == 1
    action = actions[0]
    assert action.WhichOneof("orchestratorActionType") == "sendEvent"

    # Second execution - simulate event sent completion
    # The eventSent needs to have the same eventId as the action
    event_sent = helpers.new_event_sent_event("target_instance", "my_event", json.dumps("test_data"))
    event_sent.eventId = action.id  # This is the key - the event ID must match the action ID

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None)
    ]
    new_events = [event_sent]

    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    # Should have completion action
    assert len(actions) == 1
    complete_action = actions[0]
    assert complete_action.WhichOneof("orchestratorActionType") == "completeOrchestration"
    assert complete_action.completeOrchestration.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED


def test_send_event_with_no_data():
    """Test send_event with no data parameter"""

    def orchestrator(ctx: task.OrchestrationContext, _):
        yield ctx.send_event("target_instance", "my_event")
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

    # Should have one action for send_event
    assert len(actions) == 1
    action = actions[0]
    send_action = action.sendEvent
    assert send_action.instance.instanceId == "target_instance"
    assert send_action.name == "my_event"
    # data should be None/empty when no data is provided
    assert not send_action.HasField("data") or send_action.data.value == ""


def test_send_event_multiple():
    """Test sending multiple events in sequence"""

    def orchestrator(ctx: task.OrchestrationContext, _):
        yield ctx.send_event("target1", "event1", data="data1")
        yield ctx.send_event("target2", "event2", data="data2")
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

    # Should have one action for the first send_event
    assert len(actions) == 1
    action = actions[0]
    assert action.WhichOneof("orchestratorActionType") == "sendEvent"
    assert action.sendEvent.instance.instanceId == "target1"
    assert action.sendEvent.name == "event1"
    assert action.sendEvent.data.value == json.dumps("data1")

    # Complete the first send_event and continue
    event_sent = helpers.new_event_sent_event("target1", "event1", json.dumps("data1"))
    event_sent.eventId = action.id

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None)
    ]
    new_events = [event_sent]

    result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)
    actions = result.actions

    # Should have one action for the second send_event
    assert len(actions) == 1
    action = actions[0]
    assert action.WhichOneof("orchestratorActionType") == "sendEvent"
    assert action.sendEvent.instance.instanceId == "target2"
    assert action.sendEvent.name == "event2"
    assert action.sendEvent.data.value == json.dumps("data2")


def test_send_event_with_various_data_types():
    """Test send_event with different data types"""

    def orchestrator(ctx: task.OrchestrationContext, _):
        # Test with dict
        yield ctx.send_event("target1", "event1", data={"key": "value", "number": 42})
        # Test with list
        yield ctx.send_event("target2", "event2", data=[1, 2, 3])
        # Test with number
        yield ctx.send_event("target3", "event3", data=123)
        # Test with boolean
        yield ctx.send_event("target4", "event4", data=True)
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

    # Should have one action for the first send_event
    assert len(actions) == 1
    action = actions[0]
    assert action.WhichOneof("orchestratorActionType") == "sendEvent"
    assert action.sendEvent.instance.instanceId == "target1"
    assert action.sendEvent.name == "event1"
    expected_data = json.dumps({"key": "value", "number": 42})
    assert action.sendEvent.data.value == expected_data


def test_send_event_validation():
    """Test send_event input validation"""

    def orchestrator_empty_instance(ctx: task.OrchestrationContext, _):
        yield ctx.send_event("", "event1", data="test")
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
