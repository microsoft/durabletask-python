
import pytest
from durabletask import client, task, worker

# NOTE: These tests assume a sidecar process is running. Example command:
#       go install github.com/microsoft/durabletask-go@main
#       durabletask-go --port 4001
pytestmark = pytest.mark.e2e


def empty_orchestrator(ctx: task.OrchestrationContext, _):
    return "Complete"


def test_get_all_orchestration_states():
    # Start a worker, which will connect to the sidecar in a background thread
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(empty_orchestrator)
        w.start()

        c = client.TaskHubGrpcClient()
        id = c.schedule_new_orchestration(empty_orchestrator, input="Hello")
        c.wait_for_orchestration_completion(id, timeout=30)

        all_orchestrations = c.get_all_orchestration_states()
        this_orch = c.get_orchestration_state(id)

    assert this_orch is not None
    assert this_orch.instance_id == id

    assert all_orchestrations is not None
    matching_orchestrations = [o for o in all_orchestrations if o.instance_id == id]
    assert len(matching_orchestrations) == 1
    orchestration_state = matching_orchestrations[0]
    assert orchestration_state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert orchestration_state.serialized_input == '"Hello"'
    assert orchestration_state.serialized_output == '"Complete"'
    assert orchestration_state.failure_details is None
