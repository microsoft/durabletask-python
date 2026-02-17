
from grpc._channel import _InactiveRpcError
import pytest
from durabletask import client, task, worker

# NOTE: These tests assume a sidecar process is running. Example command:
#       go install github.com/microsoft/durabletask-go@main
#       durabletask-go --port 4001
pytestmark = pytest.mark.e2e


def empty_orchestrator(ctx: task.OrchestrationContext, _):
    return "Complete"


def failing_orchestrator(ctx: task.OrchestrationContext, _):
    raise Exception("Orchestration failed")


# NOTE: The Go sidecar does not implement the batch query APIs, so these tests will need to be completed and
# enabled at such time when we have a way to test this outside of `durabletask-azuremanaged`.

# def test_get_all_orchestration_states():
#     # Start a worker, which will connect to the sidecar in a background thread
#     with worker.TaskHubGrpcWorker() as w:
#         w.add_orchestrator(empty_orchestrator)
#         w.start()

#         c = client.TaskHubGrpcClient()
#         id = c.schedule_new_orchestration(empty_orchestrator, input="Hello")
#         c.wait_for_orchestration_completion(id, timeout=30)

#         with pytest.raises(_InactiveRpcError) as exec_info:
#             c.get_all_orchestration_states()
#         assert "unimplemented" in str(exec_info.value)


# def test_get_all_entities():
#     # Start a worker, which will connect to the sidecar in a background thread
#     with worker.TaskHubGrpcWorker() as w:
#         w.add_orchestrator(empty_orchestrator)
#         w.start()

#         c = client.TaskHubGrpcClient()
#         with pytest.raises(_InactiveRpcError) as exec_info:
#             c.get_all_entities()
#         assert "method QueryEntities not implemented" in str(exec_info.value)


# def test_clean_entity_storage():
#     # Start a worker, which will connect to the sidecar in a background thread
#     with worker.TaskHubGrpcWorker() as w:
#         w.add_orchestrator(empty_orchestrator)
#         w.start()

#         c = client.TaskHubGrpcClient()
#         with pytest.raises(_InactiveRpcError) as exec_info:
#             c.clean_entity_storage()
#         assert "method CleanEntityStorage not implemented" in str(exec_info.value)


# def test_purge_orchestrations_by_status():
#     with worker.TaskHubGrpcWorker() as w:
#         w.add_orchestrator(failing_orchestrator)
#         w.start()

#         c = client.TaskHubGrpcClient()
#         with pytest.raises(_InactiveRpcError) as exec_info:
#             c.purge_orchestrations_by(
#                 runtime_status=[client.OrchestrationStatus.FAILED],
#                 recursive=True
#             )
#         # sic - error returned from sidecar
#         assert "multi-instance purge is not unimplemented" in str(exec_info.value)
