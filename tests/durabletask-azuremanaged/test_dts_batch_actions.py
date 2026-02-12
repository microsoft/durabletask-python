
import os

import pytest
from durabletask import client, task
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from durabletask.azuremanaged.client import DurableTaskSchedulerClient

# Read the environment variables
taskhub_name = os.getenv("TASKHUB", "default")
endpoint = os.getenv("ENDPOINT", "http://localhost:8080")

pytestmark = pytest.mark.dts


def empty_orchestrator(ctx: task.OrchestrationContext, _):
    return "Complete"


def test_get_all_orchestration_states():
    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(empty_orchestrator)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        id = c.schedule_new_orchestration(empty_orchestrator, input="Hello")
        c.wait_for_orchestration_completion(id, timeout=30)

        all_orchestrations = c.get_all_orchestration_states()
        all_orchestrations_with_state = c.get_all_orchestration_states(fetch_inputs_and_outputs=True)
        this_orch = c.get_orchestration_state(id)

    assert this_orch is not None
    assert this_orch.instance_id == id

    assert all_orchestrations is not None
    matching_orchestrations = [o for o in all_orchestrations if o.instance_id == id]
    assert len(matching_orchestrations) == 1
    orchestration_state = matching_orchestrations[0]
    assert orchestration_state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert orchestration_state.serialized_input is None
    assert orchestration_state.serialized_output is None
    assert orchestration_state.failure_details is None

    assert all_orchestrations_with_state is not None
    matching_orchestrations = [o for o in all_orchestrations_with_state if o.instance_id == id]
    assert len(matching_orchestrations) == 1
    orchestration_state = matching_orchestrations[0]
    assert orchestration_state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert orchestration_state.serialized_input == '"Hello"'
    assert orchestration_state.serialized_output == '"Complete"'
    assert orchestration_state.failure_details is None
