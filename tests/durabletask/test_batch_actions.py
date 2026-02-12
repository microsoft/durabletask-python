
import time
from datetime import datetime, timedelta, timezone

from durabletask import client, entities, task, worker


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

        all_orchestrations = c.get_all_orchestration_states()
        all_orchestrations_with_state = c.get_all_orchestration_states(fetch_inputs_and_outputs=True)
        this_orch = c.get_orchestration_state(id)

    assert this_orch is not None
    assert this_orch.instance_id == id

    assert all_orchestrations is not None
    assert len(all_orchestrations) >= 1
    print(f"Received {len(all_orchestrations)} orchestrations")
    assert len([o for o in all_orchestrations if o.instance_id == id]) == 1
    orchestration_state = [o for o in all_orchestrations if o.instance_id == id][0]
    assert orchestration_state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert orchestration_state.serialized_input is None
    assert orchestration_state.serialized_output is None
    assert orchestration_state.failure_details is None

    assert all_orchestrations_with_state is not None
    assert len(all_orchestrations_with_state) >= 1
    print(f"Received {len(all_orchestrations_with_state)} orchestrations")
    assert len([o for o in all_orchestrations_with_state if o.instance_id == id]) == 1
    orchestration_state = [o for o in all_orchestrations_with_state if o.instance_id == id][0]
    assert orchestration_state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert orchestration_state.serialized_input == '"Hello"'
    assert orchestration_state.serialized_output == '"Complete"'
    assert orchestration_state.failure_details is None


def test_get_orchestration_state_by_status():
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(empty_orchestrator)
        w.add_orchestrator(failing_orchestrator)
        w.start()

        c = client.TaskHubGrpcClient()

        # Schedule completed orchestration
        completed_id = c.schedule_new_orchestration(empty_orchestrator, input="Hello")
        c.wait_for_orchestration_completion(completed_id, timeout=30)

        # Schedule failed orchestration
        failed_id = c.schedule_new_orchestration(failing_orchestrator)
        try:
            c.wait_for_orchestration_completion(failed_id, timeout=30)
        except client.OrchestrationFailedError:
            pass  # Expected failure

        # Query by completed status
        completed_orchestrations = c.get_orchestration_state_by(
            runtime_status=[client.OrchestrationStatus.COMPLETED],
            fetch_inputs_and_outputs=True
        )

        # Query by failed status
        failed_orchestrations = c.get_orchestration_state_by(
            runtime_status=[client.OrchestrationStatus.FAILED],
            fetch_inputs_and_outputs=True
        )

    assert len([o for o in completed_orchestrations if o.instance_id == completed_id]) == 1
    completed_orch = [o for o in completed_orchestrations if o.instance_id == completed_id][0]
    assert completed_orch.runtime_status == client.OrchestrationStatus.COMPLETED
    assert completed_orch.serialized_output == '"Complete"'

    assert len([o for o in failed_orchestrations if o.instance_id == failed_id]) == 1
    failed_orch = [o for o in failed_orchestrations if o.instance_id == failed_id][0]
    assert failed_orch.runtime_status == client.OrchestrationStatus.FAILED
    assert failed_orch.failure_details is not None


def test_get_orchestration_state_by_time_range():
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(empty_orchestrator)
        w.start()

        c = client.TaskHubGrpcClient()

        # Get current time
        before_creation = datetime.now(timezone.utc) - timedelta(seconds=5)

        # Schedule orchestration
        id = c.schedule_new_orchestration(empty_orchestrator, input="TimeTest")
        c.wait_for_orchestration_completion(id, timeout=30)

        after_creation = datetime.now(timezone.utc) + timedelta(seconds=5)

        # Query by time range
        orchestrations_in_range = c.get_orchestration_state_by(
            created_time_from=before_creation,
            created_time_to=after_creation,
            fetch_inputs_and_outputs=True
        )

        # Query outside time range
        orchestrations_outside_range = c.get_orchestration_state_by(
            created_time_from=after_creation,
            created_time_to=after_creation + timedelta(hours=1)
        )

    assert len([o for o in orchestrations_in_range if o.instance_id == id]) == 1
    assert len([o for o in orchestrations_outside_range if o.instance_id == id]) == 0


def test_get_orchestration_state_by_max_instance_count():
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(empty_orchestrator)
        w.start()

        c = client.TaskHubGrpcClient()

        # Query with max_instance_count
        orchestrations = c.get_orchestration_state_by(max_instance_count=2)

    # Should return at most 2 instances
    assert len(orchestrations) <= 2


def test_purge_orchestration():
    with worker.TaskHubGrpcWorker() as w:
        w.add_orchestrator(empty_orchestrator)
        w.start()

        c = client.TaskHubGrpcClient()

        # Schedule and complete orchestration
        id = c.schedule_new_orchestration(empty_orchestrator, input="ToPurge")
        c.wait_for_orchestration_completion(id, timeout=30)

        # Verify it exists
        state_before = c.get_orchestration_state(id)
        assert state_before is not None

        # Purge the orchestration
        result = c.purge_orchestration(id, recursive=True)

        # Verify purge result
        assert result.deleted_instance_count >= 1

        # Verify it no longer exists
        state_after = c.get_orchestration_state(id)
        assert state_after is None


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


def test_get_all_entities():
    counter_value = 0

    def counter_entity(ctx: entities.EntityContext, _):
        nonlocal counter_value
        if ctx.operation == "add":
            counter_value += ctx.get_input(int)
            ctx.set_state(counter_value)
        elif ctx.operation == "get":
            ctx.set_result(counter_value)

    with worker.TaskHubGrpcWorker() as w:
        w.add_entity(counter_entity)
        w.start()

        c = client.TaskHubGrpcClient()

        # Create entity
        entity_id = entities.EntityInstanceId("counter_entity", "testCounter1")
        c.signal_entity(entity_id, "add", 5)
        time.sleep(2)  # Wait for signal to be processed

        # Get all entities without state
        all_entities = c.get_all_entities(include_state=False)
        assert len([e for e in all_entities if e.id == entity_id]) == 1
        entity_without_state = [e for e in all_entities if e.id == entity_id][0]
        assert entity_without_state.serialized_state is None

        # Get all entities with state
        all_entities_with_state = c.get_all_entities(include_state=True)
        assert len([e for e in all_entities_with_state if e.id == entity_id]) == 1
        entity_with_state = [e for e in all_entities_with_state if e.id == entity_id][0]
        assert entity_with_state.get_state(int) == 5


def test_get_entities_by_instance_id_prefix():
    def counter_entity(ctx: entities.EntityContext, _):
        if ctx.operation == "set":
            ctx.set_state(ctx.get_input(int))

    with worker.TaskHubGrpcWorker() as w:
        w.add_entity(counter_entity)
        w.start()

        c = client.TaskHubGrpcClient()

        # Create entities with different prefixes
        entity_id_1 = entities.EntityInstanceId("counter_entity", "prefix1_counter")
        entity_id_2 = entities.EntityInstanceId("counter_entity", "prefix2_counter")

        c.signal_entity(entity_id_1, "set", 10)
        c.signal_entity(entity_id_2, "set", 20)
        time.sleep(2)  # Wait for signals to be processed

        # Query by prefix
        entities_prefix1 = c.get_entities_by(
            instance_id_starts_with="@counter_entity@prefix1",
            include_state=True
        )

        entities_prefix2 = c.get_entities_by(
            instance_id_starts_with="@counter_entity@prefix2",
            include_state=True
        )

    assert len([e for e in entities_prefix1 if e.id == entity_id_1]) == 1
    assert len([e for e in entities_prefix1 if e.id == entity_id_2]) == 0

    assert len([e for e in entities_prefix2 if e.id == entity_id_2]) == 1
    assert len([e for e in entities_prefix2 if e.id == entity_id_1]) == 0


def test_get_entities_by_time_range():
    def simple_entity(ctx: entities.EntityContext, _):
        if ctx.operation == "set":
            ctx.set_state(ctx.get_input(str))

    with worker.TaskHubGrpcWorker() as w:
        w.add_entity(simple_entity)
        w.start()

        c = client.TaskHubGrpcClient()

        # Get current time
        before_creation = datetime.now(timezone.utc) - timedelta(seconds=5)

        # Create entity
        entity_id = entities.EntityInstanceId("simple_entity", "timeTestEntity")
        c.signal_entity(entity_id, "set", "test_value")
        time.sleep(2)  # Wait for signal to be processed

        after_creation = datetime.now(timezone.utc) + timedelta(seconds=5)

        # Query by time range
        entities_in_range = c.get_entities_by(
            last_modified_from=before_creation,
            last_modified_to=after_creation,
            include_state=True
        )

        # Query outside time range
        entities_outside_range = c.get_entities_by(
            last_modified_from=after_creation,
            last_modified_to=after_creation + timedelta(hours=1)
        )

    assert len([e for e in entities_in_range if e.id == entity_id]) == 1
    assert len([e for e in entities_outside_range if e.id == entity_id]) == 0


def test_clean_entity_storage():
    def empty_entity(ctx: entities.EntityContext, _):
        if ctx.operation == "delete":
            ctx.delete_state()

    with worker.TaskHubGrpcWorker() as w:
        w.add_entity(empty_entity)
        w.start()

        c = client.TaskHubGrpcClient()

        # Create an entity and then delete its state to make it empty
        entity_id = entities.EntityInstanceId("empty_entity", "toClean")
        c.signal_entity(entity_id, "delete")
        time.sleep(2)  # Wait for signal to be processed

        # Clean entity storage
        result = c.clean_entity_storage(
            remove_empty_entities=True,
            release_orphaned_locks=True
        )

    # Verify clean result
    assert result.empty_entities_removed >= 0
    assert result.orphaned_locks_released >= 0
