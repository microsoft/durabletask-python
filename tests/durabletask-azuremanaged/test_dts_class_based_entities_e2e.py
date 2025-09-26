import os
import time
from typing import Optional

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


def test_client_signal_class_entity():
    invoked = False

    class EmptyEntity(entities.DurableEntity):
        def do_nothing(self, _):
            nonlocal invoked  # don't do this in a real app!
            invoked = True

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_entity(EmptyEntity)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        entity_id = entities.EntityInstanceId("EmptyEntity", "testEntity")
        c.signal_entity(entity_id, "do_nothing")
        time.sleep(2)  # wait for the signal to be processed

    assert invoked


def test_orchestration_signal_class_entity():
    invoked = False

    class EmptyEntity(entities.DurableEntity):
        def do_nothing(self, _):
            nonlocal invoked  # don't do this in a real app!
            invoked = True

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("EmptyEntity", "testEntity")
        ctx.signal_entity(entity_id, "do_nothing")

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(EmptyEntity)
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


def test_orchestration_call_class_entity():
    invoked = False

    class EmptyEntity(entities.DurableEntity):
        def do_nothing(self, _):
            nonlocal invoked  # don't do this in a real app!
            invoked = True

    def empty_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("EmptyEntity", "testEntity")
        yield ctx.call_entity(entity_id, "do_nothing")

    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_entity(EmptyEntity)
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
