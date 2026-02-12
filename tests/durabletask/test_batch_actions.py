
from grpc._channel import _InactiveRpcError
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

        with pytest.raises(_InactiveRpcError) as exec_info:
            c.get_all_orchestration_states()
        assert "unimplemented" in str(exec_info.value)
