from datetime import datetime, timezone
import os
import time

import pytest

from durabletask import client, entities, task
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker

# NOTE: These tests assume a sidecar process is running. Example command:
#       docker run -i -p 8080:8080 -p 8082:8082 -d mcr.microsoft.com/dts/dts-emulator:latest
pytestmark = pytest.mark.dts

# Read the environment variables
taskhub_name = os.getenv("TASKHUB", "default")
endpoint = os.getenv("ENDPOINT", "http://localhost:8080")


def test_client_signal_entity_and_custom_name():
    invoked = False

    def empty_entity(ctx: entities.EntityContext, _):
        nonlocal invoked  # don't do this in a real app!
        if ctx.operation == "do_nothing":
            invoked = True

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_entity(empty_entity, name="EntityNameCustom")
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        entity_id = entities.EntityInstanceId("EntityNameCustom", "testEntity")
        c.signal_entity(entity_id, "do_nothing")
        time.sleep(2)  # wait for the signal to be processed

    assert invoked


def test_client_get_entity():
    invoked = False

    def empty_entity(ctx: entities.EntityContext, _):
        nonlocal invoked  # don't do this in a real app!
        if ctx.operation == "do_nothing":
            invoked = True
            ctx.set_state(1)

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_entity(empty_entity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        entity_id = entities.EntityInstanceId("empty_entity", "testEntity")
        c.signal_entity(entity_id, "do_nothing")
        time.sleep(2)  # wait for the signal to be processed
        state = c.get_entity(entity_id)
        assert state is not None
        assert state.id == entity_id
        assert state.get_locked_by() is None
        assert state.last_modified < datetime.now(timezone.utc)
        assert state.get_state(int) == 1

    assert invoked


def test_orchestration_signal_entity_and_custom_name():
    invoked = False

    def empty_entity(ctx: entities.EntityContext, _):
        if ctx.operation == "do_nothing":
            nonlocal invoked  # don't do this in a real app!
            invoked = True

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("EntityNameCustom", f"{ctx.instance_id}_testEntity")
        ctx.signal_entity(entity_id, "do_nothing")

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(empty_entity, name="EntityNameCustom")
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        id = c.schedule_new_orchestration(empty_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)
        time.sleep(2)  # wait for the signal to be processed - signals cannot be awaited from inside the orchestrator

    assert invoked
    assert state is not None
    assert state.name == task.get_name(empty_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_input is None
    assert state.serialized_output is None
    assert state.serialized_custom_status is None


def test_orchestration_call_entity():
    invoked = False

    def empty_entity(ctx: entities.EntityContext, _):
        if ctx.operation == "do_nothing":
            nonlocal invoked  # don't do this in a real app!
            invoked = True

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("empty_entity", f"{ctx.instance_id}_testEntity")
        yield ctx.call_entity(entity_id, "do_nothing")

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(empty_entity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        id = c.schedule_new_orchestration(empty_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert invoked
    assert state is not None
    assert state.name == task.get_name(empty_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_input is None
    assert state.serialized_output is None
    assert state.serialized_custom_status is None


def test_orchestration_call_entity_with_lock():
    invoked = False

    def empty_entity(ctx: entities.EntityContext, _):
        if ctx.operation == "do_nothing":
            nonlocal invoked  # don't do this in a real app!
            invoked = True

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("empty_entity", f"{ctx.instance_id}_testEntity")
        with (yield ctx.lock_entities([entity_id])):
            yield ctx.call_entity(entity_id, "do_nothing")

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(empty_entity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        id = c.schedule_new_orchestration(empty_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

        # Call this a second time to ensure the entity is still responsive after being locked and unlocked
        id_2 = c.schedule_new_orchestration(empty_orchestrator)
        state_2 = c.wait_for_orchestration_completion(id_2, timeout=30)

    assert invoked
    assert state is not None
    assert state.name == task.get_name(empty_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_input is None
    assert state.serialized_output is None
    assert state.serialized_custom_status is None

    assert state_2 is not None
    assert state_2.name == task.get_name(empty_orchestrator)
    assert state_2.instance_id == id_2
    assert state_2.failure_details is None
    assert state_2.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state_2.serialized_input is None
    assert state_2.serialized_output is None
    assert state_2.serialized_custom_status is None


def test_orchestration_entity_signals_entity():
    invoked = False

    def empty_entity(ctx: entities.EntityContext, _):
        if ctx.operation == "do_nothing":
            nonlocal invoked  # don't do this in a real app!
            invoked = True
        elif ctx.operation == "signal_other":
            entity_id = entities.EntityInstanceId("empty_entity",
                                                  ctx.entity_id.key.replace("_testEntity", "_otherEntity"))
            ctx.signal_entity(entity_id, "do_nothing")

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("empty_entity", f"{ctx.instance_id}_testEntity")
        yield ctx.call_entity(entity_id, "signal_other")

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(empty_entity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        id = c.schedule_new_orchestration(empty_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert invoked
    assert state is not None
    assert state.name == task.get_name(empty_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_input is None
    assert state.serialized_output is None
    assert state.serialized_custom_status is None


def test_entity_starts_orchestration():
    invoked = False

    def empty_entity(ctx: entities.EntityContext, _):
        if ctx.operation == "start_orchestration":
            ctx.schedule_new_orchestration("empty_orchestrator")

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        nonlocal invoked  # don't do this in a real app!
        invoked = True

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(empty_entity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        c.signal_entity(entities.EntityInstanceId("empty_entity", "testEntity"), "start_orchestration")
        time.sleep(2)  # wait for the signal and orchestration to be processed

    assert invoked


def test_entity_locking_behavior():
    def empty_entity(ctx: entities.EntityContext, _):
        pass

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("empty_entity", f"{ctx.instance_id}_testEntity")
        with (yield ctx.lock_entities([entity_id])):
            # Cannot signal entities that have been locked
            assert pytest.raises(Exception, ctx.signal_entity, entity_id, "do_nothing")
            entity_call_task = ctx.call_entity(entity_id, "do_nothing")
            # Cannot call entities that have been locked and already called, but not yet returned a result
            assert pytest.raises(Exception, ctx.call_entity, entity_id, "do_nothing")
            yield entity_call_task

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(empty_entity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        id = c.schedule_new_orchestration(empty_orchestrator)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(empty_orchestrator)
    assert state.instance_id == id
    assert state.failure_details is None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_input is None
    assert state.serialized_output is None
    assert state.serialized_custom_status is None


def test_entity_unlocks_when_user_code_throws():
    invoke_count = 0

    def empty_entity(ctx: entities.EntityContext, _):
        nonlocal invoke_count  # don't do this in a real app!
        invoke_count += 1

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("empty_entity", f"{ctx.instance_id}_testEntity")
        with (yield ctx.lock_entities([entity_id])):
            yield ctx.call_entity(entity_id, "do_nothing")
            raise Exception("Simulated exception")

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(empty_entity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        time.sleep(2)  # wait for the signal and orchestration to be processed
        id = c.schedule_new_orchestration(empty_orchestrator)
        c.wait_for_orchestration_completion(id, timeout=30)
        id = c.schedule_new_orchestration(empty_orchestrator)
        c.wait_for_orchestration_completion(id, timeout=30)

    assert invoke_count == 2


def test_entity_unlocks_when_user_mishandles_lock():
    invoke_count = 0

    def empty_entity(ctx: entities.EntityContext, _):
        nonlocal invoke_count  # don't do this in a real app!
        invoke_count += 1

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("empty_entity", f"{ctx.instance_id}_testEntity")
        yield ctx.lock_entities([entity_id])
        yield ctx.call_entity(entity_id, "do_nothing")

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(empty_entity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        time.sleep(2)  # wait for the signal and orchestration to be processed
        id = c.schedule_new_orchestration(empty_orchestrator)
        c.wait_for_orchestration_completion(id, timeout=30)
        id = c.schedule_new_orchestration(empty_orchestrator)
        c.wait_for_orchestration_completion(id, timeout=30)

    assert invoke_count == 2


# TODO: Uncomment this test
# Will not pass until https://msazure.visualstudio.com/One/_git/AAPT-DTMB/pullrequest/13610881 is merged and
# deployed to the docker image
# def test_entity_unlocks_when_user_calls_continue_as_new():
#     invoke_count = 0

#     def empty_entity(ctx: entities.EntityContext, _):
#         nonlocal invoke_count  # don't do this in a real app!
#         invoke_count += 1

#     def empty_orchestrator(ctx: task.OrchestrationContext, entity_call_count: int):
#         entity_id = entities.EntityInstanceId("empty_entity", "testEntity")
#         nonlocal invoke_count
#         if not ctx.is_replaying:
#             invoke_count += 1
#         with (yield ctx.lock_entities([entity_id])):
#             yield ctx.call_entity(entity_id, "do_nothing")
#             if entity_call_count > 0:
#                 ctx.continue_as_new(entity_call_count - 1, save_events=True)

#     # Start a worker, which will connect to the sidecar in a background thread
#     with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
#                                     taskhub=taskhub_name, token_credential=None) as w:
#         w.add_orchestrator(empty_orchestrator)
#         w.add_entity(empty_entity)
#         w.start()

#         c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
#                                        taskhub=taskhub_name, token_credential=None)
#         time.sleep(2)  # wait for the signal and orchestration to be processed
#         id = c.schedule_new_orchestration(empty_orchestrator, input=2)
#         c.wait_for_orchestration_completion(id, timeout=30)

#     assert invoke_count == 6
