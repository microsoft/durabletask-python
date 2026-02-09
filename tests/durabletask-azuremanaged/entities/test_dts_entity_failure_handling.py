
import json
import os
from durabletask import client, entities, task

from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker

# Read the environment variables
taskhub_name = os.getenv("TASKHUB", "default")
endpoint = os.getenv("ENDPOINT", "http://localhost:8080")


def test_class_entity_unhandled_failure_fails():
    class FailingEntity(entities.DurableEntity):
        def fail(self, _):
            raise ValueError("Something went wrong!")

    def test_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("FailingEntity", "testEntity")
        yield ctx.call_entity(entity_id, "fail")

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(test_orchestrator)
        w.add_entity(FailingEntity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        id = c.schedule_new_orchestration(test_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(test_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is not None
    assert state.failure_details.error_type == "TaskFailedError"
    # NOTE: Because FailureDetails does not support inner_failure, we can't verify that the inner failure type is
    # EntityOperationFailedException. In the future, we should consider adding support for inner failures in
    # FailureDetails to make this more robust. This applies to all tests in this file. For now, the error message's
    # structure is sufficient to verify that the failure was due to the EntityOperationFailedException.
    assert state.failure_details.message == "Operation 'fail' on entity '@failingentity@testEntity' failed with " \
                                            "error: Something went wrong!"
    assert state.runtime_status == client.OrchestrationStatus.FAILED


def test_function_entity_unhandled_failure_fails():
    def failing_entity(ctx: entities.EntityContext, _):
        raise ValueError("Something went wrong!")

    def test_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("failing_entity", "testEntity")
        yield ctx.call_entity(entity_id, "fail")

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(test_orchestrator)
        w.add_entity(failing_entity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        id = c.schedule_new_orchestration(test_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(test_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is not None
    assert state.failure_details.error_type == "TaskFailedError"
    assert state.failure_details.message == "Operation 'fail' on entity '@failing_entity@testEntity' failed with " \
                                            "error: Something went wrong!"
    assert state.runtime_status == client.OrchestrationStatus.FAILED


def test_class_entity_handled_failure_succeeds():
    class FailingEntity(entities.DurableEntity):
        def fail(self, _):
            raise ValueError("Something went wrong!")

    def test_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("FailingEntity", "testEntity")
        try:
            yield ctx.call_entity(entity_id, "fail")
        except task.TaskFailedError as e:
            # Need to return the exception wrapped in a list to avoid
            # https://github.com/microsoft/durabletask-python/issues/108
            return e.details.message  # returning just the message to avoid issues with JSON serialization of FailureDetails

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(test_orchestrator)
        w.add_entity(FailingEntity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        id = c.schedule_new_orchestration(test_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(test_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None

    assert state.serialized_output is not None
    output = json.loads(state.serialized_output)
    assert output == "Operation 'fail' on entity '@failingentity@testEntity' failed with error: Something went wrong!"
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED


def test_function_entity_handled_failure_succeeds():
    def failing_entity(ctx: entities.EntityContext, _):
        raise ValueError("Something went wrong!")

    def test_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("failing_entity", "testEntity")
        try:
            yield ctx.call_entity(entity_id, "fail")
        except task.TaskFailedError as e:
            return e.details.message  # returning just the message to avoid issues with JSON serialization of FailureDetails

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(test_orchestrator)
        w.add_entity(failing_entity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        id = c.schedule_new_orchestration(test_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(test_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None

    assert state.serialized_output is not None
    output = json.loads(state.serialized_output)
    assert output == "Operation 'fail' on entity '@failing_entity@testEntity' failed with error: Something went wrong!"
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED


def test_class_entity_failure_unlocks_entity():
    def failing_entity(ctx: entities.EntityContext, _):
        raise ValueError("Something went wrong!")

    def test_orchestrator(ctx: task.OrchestrationContext, _):
        exception_count = 0
        entity_id = entities.EntityInstanceId("failing_entity", "testEntity")
        with (yield ctx.lock_entities([entity_id])):
            try:
                yield ctx.call_entity(entity_id, "fail")
            except task.TaskFailedError as e:
                exception_count += 1
        try:
            yield ctx.call_entity(entity_id, "fail")
        except task.TaskFailedError as e:
            exception_count += 1
        return exception_count

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(test_orchestrator)
        w.add_entity(failing_entity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        id = c.schedule_new_orchestration(test_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(test_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None

    assert state.serialized_output is not None
    output = json.loads(state.serialized_output)
    assert output == 2
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
