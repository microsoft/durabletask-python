import simplejson as json
from durabletask.api.task_hub_client import TaskHubGrpcClient
from durabletask.task.activities import ActivityContext
from durabletask.task.orchestration import OrchestrationContext

from durabletask.task.registry import Registry
import durabletask.worker.worker as worker
import durabletask.protos.orchestrator_service_pb2 as pb

# NOTE: These tests assume a sidecar process is running. Example command:
#       docker run --name durabletask-sidecar -p 4001:4001 --env 'DURABLETASK_SIDECAR_LOGLEVEL=Debug' --rm cgillum/durabletask-sidecar:latest start --backend Emulator


def test_empty_orchestration():

    invoked = False

    def empty_orchestrator(ctx: OrchestrationContext, _):
        nonlocal invoked  # don't do this in a real app!
        invoked = True

    registry = Registry()
    name = registry.add_orchestrator(empty_orchestrator)

    # Start a worker, which will connect to the sidecar in a background thread
    with worker.TaskHubGrpcWorker(registry) as task_hub_worker:
        task_hub_worker.start()

        task_hub_client = TaskHubGrpcClient()
        id = task_hub_client.schedule_new_orchestration(empty_orchestrator)
        state = task_hub_client.wait_for_orchestration_completion(id, timeout=30)

    assert invoked == True
    assert state is not None
    assert state.name == name
    assert state.instance_id == id
    assert state.failure_details is None
    assert state.runtime_status == pb.ORCHESTRATION_STATUS_COMPLETED
    assert state.serialized_input is None
    assert state.serialized_output is None
    assert state.serialized_custom_status is None


def test_activity_sequence():

    def plus_one(_: ActivityContext, input: int) -> int:
        return input + 1

    def sequence(ctx: OrchestrationContext, start_val: int):
        numbers = [start_val]
        current = start_val
        for _ in range(10):
            current = yield ctx.call_activity(plus_one, input=current)
            numbers.append(current)
        return numbers

    registry = Registry()
    registry.add_activity(plus_one)
    name = registry.add_orchestrator(sequence)

    # Start a worker, which will connect to the sidecar in a background thread
    with worker.TaskHubGrpcWorker(registry) as task_hub_worker:
        task_hub_worker.start()

        task_hub_client = TaskHubGrpcClient()
        id = task_hub_client.schedule_new_orchestration(sequence, input=1)
        state = task_hub_client.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.name == name
    assert state.instance_id == id
    assert state.runtime_status == pb.ORCHESTRATION_STATUS_COMPLETED
    assert state.failure_details is None
    assert state.serialized_input == json.dumps(1)
    assert state.serialized_output == json.dumps([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
    assert state.serialized_custom_status is None
