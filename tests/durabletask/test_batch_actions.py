
from datetime import datetime, timedelta, timezone
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


def test_get_all_entities():
    # Start a worker, which will connect to the sidecar in a background thread
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(empty_orchestrator)
        w.start()

        c = client.TaskHubGrpcClient()
        with pytest.raises(_InactiveRpcError) as exec_info:
            c.get_all_entities()
        assert "unimplemented" in str(exec_info.value)


def test_clean_entity_storage():
    # Start a worker, which will connect to the sidecar in a background thread
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(empty_orchestrator)
        w.start()

        c = client.TaskHubGrpcClient()
        with pytest.raises(_InactiveRpcError) as exec_info:
            c.clean_entity_storage()
        assert "unimplemented" in str(exec_info.value)


def test_purge_orchestrations_by_status():
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(failing_orchestrator)
        w.start()

        c = client.TaskHubGrpcClient()

        # Schedule and let it fail
        failed_id = c.schedule_new_orchestration(failing_orchestrator)
        try:
            c.wait_for_orchestration_completion(failed_id, timeout=30)
        except client.OrchestrationFailedError:
            pass  # Expected failure

        # Verify it exists and is failed
        state_before = c.get_orchestration_state(failed_id)
        assert state_before is not None
        assert state_before.runtime_status == client.OrchestrationStatus.FAILED

        # Purge failed orchestrations
        result = c.purge_orchestrations_by(
            runtime_status=[client.OrchestrationStatus.FAILED],
            recursive=True
        )

        # Verify purge result
        assert result.deleted_instance_count >= 1

        # Verify the failed orchestration no longer exists
        state_after = c.get_orchestration_state(failed_id)
        assert state_after is None


def test_purge_orchestrations_by_time_range():
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(empty_orchestrator)
        w.start()

        c = client.TaskHubGrpcClient()

        # Get current time
        before_creation = datetime.now(timezone.utc) - timedelta(seconds=5)

        # Schedule orchestration
        id = c.schedule_new_orchestration(empty_orchestrator, input="ToPurgeByTime")
        c.wait_for_orchestration_completion(id, timeout=30)

        after_creation = datetime.now(timezone.utc) + timedelta(seconds=5)

        # Verify it exists
        state_before = c.get_orchestration_state(id)
        assert state_before is not None

        # Purge by time range
        result = c.purge_orchestrations_by(
            created_time_from=before_creation,
            created_time_to=after_creation,
            runtime_status=[client.OrchestrationStatus.COMPLETED],
            recursive=True
        )

        # Verify purge result
        assert result.deleted_instance_count >= 1

        # Verify it no longer exists
        state_after = c.get_orchestration_state(id)
        assert state_after is None
