from durabletask.api.task_hub_client import TaskHubGrpcClient
from durabletask.task.orchestrator import OrchestrationContext

from durabletask.task.registry import Registry
import durabletask.worker.worker as worker
import durabletask.protos.orchestrator_service_pb2 as pb

# NOTE: These tests assume a sidecar process is running. Example command:
#       docker run --name durabletask-sidecar -p 4001:4001 --env 'DURABLETASK_SIDECAR_LOGLEVEL=Debug' --rm cgillum/durabletask-sidecar:latest start --backend Emulator


def test_empty_orchestration():

    def empty_orchestrator(ctx: OrchestrationContext):
        return "done"

    registry = Registry()
    name = registry.add_orchestrator(empty_orchestrator)

    # Start a worker, which will connect to the sidecar in a background thread
    with worker.TaskHubGrpcWorker() as task_hub_worker:
        task_hub_worker.start()

        task_hub_client = TaskHubGrpcClient()
        id = task_hub_client.schedule_new_orchestration(name)
        state = task_hub_client.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == name
    assert state.instance_id == id
    assert state.failure_details is None
    assert state.runtime_status == pb.ORCHESTRATION_STATUS_COMPLETED
