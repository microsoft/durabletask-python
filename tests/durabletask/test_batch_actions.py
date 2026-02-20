# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for batch query and purge APIs using the InMemoryOrchestrationBackend.
"""

import logging
import time
from datetime import datetime, timedelta, timezone

import pytest
from durabletask import client, entities, task
from durabletask.client import TaskHubGrpcClient
from durabletask.testing import create_test_backend
from durabletask.worker import TaskHubGrpcWorker

BATCH_TEST_PORT = 50058
HOST = f"localhost:{BATCH_TEST_PORT}"


@pytest.fixture
def backend():
    """Create an in-memory backend for batch action testing."""
    backend = create_test_backend(port=BATCH_TEST_PORT)
    yield backend
    backend.stop()
    backend.reset()


def empty_orchestrator(ctx: task.OrchestrationContext, _):
    return "Complete"


def failing_orchestrator(ctx: task.OrchestrationContext, _):
    raise Exception("Orchestration failed")


def test_get_all_orchestration_states(backend):
    worker = TaskHubGrpcWorker(host_address=HOST)
    c = TaskHubGrpcClient(host_address=HOST)

    worker.add_orchestrator(empty_orchestrator)
    worker.start()

    try:
        id = c.schedule_new_orchestration(empty_orchestrator, input="Hello")
        c.wait_for_orchestration_completion(id, timeout=30)

        all_orchestrations = c.get_all_orchestration_states()
        query = client.OrchestrationQuery()
        query.fetch_inputs_and_outputs = True
        all_orchestrations_with_state = c.get_all_orchestration_states(query)
        this_orch = c.get_orchestration_state(id)
    finally:
        worker.stop()

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


def test_get_orchestration_state_by_status(backend):
    worker = TaskHubGrpcWorker(host_address=HOST)
    c = TaskHubGrpcClient(host_address=HOST)

    worker.add_orchestrator(empty_orchestrator)
    worker.add_orchestrator(failing_orchestrator)
    worker.start()

    try:
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
        query = client.OrchestrationQuery()
        query.runtime_status = [client.OrchestrationStatus.COMPLETED]
        query.fetch_inputs_and_outputs = True
        completed_orchestrations = c.get_all_orchestration_states(query)

        # Query by failed status
        query = client.OrchestrationQuery()
        query.runtime_status = [client.OrchestrationStatus.FAILED]
        query.fetch_inputs_and_outputs = True
        failed_orchestrations = c.get_all_orchestration_states(query)
    finally:
        worker.stop()

    assert len([o for o in completed_orchestrations if o.instance_id == completed_id]) == 1
    completed_orch = [o for o in completed_orchestrations if o.instance_id == completed_id][0]
    assert completed_orch.runtime_status == client.OrchestrationStatus.COMPLETED
    assert completed_orch.serialized_output == '"Complete"'

    assert len([o for o in failed_orchestrations if o.instance_id == failed_id]) == 1
    failed_orch = [o for o in failed_orchestrations if o.instance_id == failed_id][0]
    assert failed_orch.runtime_status == client.OrchestrationStatus.FAILED
    assert failed_orch.failure_details is not None


def test_get_orchestration_state_by_time_range(backend):
    worker = TaskHubGrpcWorker(host_address=HOST)
    c = TaskHubGrpcClient(host_address=HOST)

    worker.add_orchestrator(empty_orchestrator)
    worker.start()

    try:
        # Get current time
        before_creation = datetime.now(timezone.utc) - timedelta(seconds=5)

        # Schedule orchestration
        id = c.schedule_new_orchestration(empty_orchestrator, input="TimeTest")
        c.wait_for_orchestration_completion(id, timeout=30)

        after_creation = datetime.now(timezone.utc) + timedelta(seconds=5)

        # Query by time range
        query = client.OrchestrationQuery(
            created_time_from=before_creation,
            created_time_to=after_creation,
            fetch_inputs_and_outputs=True
        )
        orchestrations_in_range = c.get_all_orchestration_states(query)

        # Query outside time range
        query = client.OrchestrationQuery(
            created_time_from=after_creation,
            created_time_to=after_creation + timedelta(hours=1),
            fetch_inputs_and_outputs=True
        )
        orchestrations_outside_range = c.get_all_orchestration_states(query)
    finally:
        worker.stop()

    assert len([o for o in orchestrations_in_range if o.instance_id == id]) == 1
    assert len([o for o in orchestrations_outside_range if o.instance_id == id]) == 0


def test_get_orchestration_state_pagination_succeeds(backend):
    # Create a custom handler to capture log messages
    log_records = []

    class ListHandler(logging.Handler):
        def emit(self, record):
            log_records.append(record)

    handler = ListHandler()

    worker = TaskHubGrpcWorker(host_address=HOST)
    c = TaskHubGrpcClient(host_address=HOST, log_handler=handler)

    worker.add_orchestrator(empty_orchestrator)
    worker.start()

    try:
        # Create at least 3 orchestrations to test the limit
        ids = []
        for i in range(3):
            id = c.schedule_new_orchestration(empty_orchestrator, input=f"Test{i}")
            ids.append(id)

        # Wait for all to complete
        for id in ids:
            c.wait_for_orchestration_completion(id, timeout=30)

        # Query with max_instance_count=2
        query = client.OrchestrationQuery(max_instance_count=2)
        orchestrations = c.get_all_orchestration_states(query)
    finally:
        worker.stop()

    # Should return more than 2 instances since we created at least 3
    assert len(orchestrations) > 2
    # Verify the pagination loop ran by checking for the continuation token log message
    assert any("Received continuation token" in record.getMessage() for record in log_records), \
        "Expected pagination loop to execute with continuation token"


def test_purge_orchestration(backend):
    worker = TaskHubGrpcWorker(host_address=HOST)
    c = TaskHubGrpcClient(host_address=HOST)

    worker.add_orchestrator(empty_orchestrator)
    worker.start()

    try:
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
    finally:
        worker.stop()


def test_purge_orchestrations_by_status(backend):
    worker = TaskHubGrpcWorker(host_address=HOST)
    c = TaskHubGrpcClient(host_address=HOST)

    worker.add_orchestrator(failing_orchestrator)
    worker.start()

    try:
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
    finally:
        worker.stop()


def test_purge_orchestrations_by_time_range(backend):
    worker = TaskHubGrpcWorker(host_address=HOST)
    c = TaskHubGrpcClient(host_address=HOST)

    worker.add_orchestrator(empty_orchestrator)
    worker.start()

    try:
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
    finally:
        worker.stop()


def test_get_all_entities(backend):
    counter_value = 0

    def counter_entity(ctx: entities.EntityContext, input):
        nonlocal counter_value
        if ctx.operation == "add":
            counter_value += input
            ctx.set_state(counter_value)
        elif ctx.operation == "get":
            return ctx.get_state(int, 0)

    worker = TaskHubGrpcWorker(host_address=HOST)
    c = TaskHubGrpcClient(host_address=HOST)

    worker.add_entity(counter_entity)
    worker.start()

    try:
        # Create entity
        entity_id = entities.EntityInstanceId("counter_entity", "testCounter1")
        c.signal_entity(entity_id, "add", input=5)
        time.sleep(3)  # Wait for signal to be processed

        # Get all entities without state
        query = client.EntityQuery(include_state=False)
        all_entities = c.get_all_entities(query)
        assert len([e for e in all_entities if e.id == entity_id]) == 1
        entity_without_state = [e for e in all_entities if e.id == entity_id][0]
        assert entity_without_state.get_state(int) is None

        # Get all entities with state
        query = client.EntityQuery(include_state=True)
        all_entities_with_state = c.get_all_entities(query)
        assert len([e for e in all_entities_with_state if e.id == entity_id]) == 1
        entity_with_state = [e for e in all_entities_with_state if e.id == entity_id][0]
        assert entity_with_state.get_state(int) == 5
    finally:
        worker.stop()


def test_get_entities_by_instance_id_prefix(backend):
    def counter_entity(ctx: entities.EntityContext, input):
        if ctx.operation == "set":
            ctx.set_state(input)

    worker = TaskHubGrpcWorker(host_address=HOST)
    c = TaskHubGrpcClient(host_address=HOST)

    worker.add_entity(counter_entity)
    worker.start()

    try:
        # Create entities with different prefixes
        entity_id_1 = entities.EntityInstanceId("counter_entity", "prefix1_counter")
        entity_id_2 = entities.EntityInstanceId("counter_entity", "prefix2_counter")

        c.signal_entity(entity_id_1, "set", input=10)
        c.signal_entity(entity_id_2, "set", input=20)
        time.sleep(3)  # Wait for signals to be processed

        # Query by prefix
        query = client.EntityQuery(
            instance_id_starts_with="@counter_entity@prefix1",
            include_state=True
        )
        entities_prefix1 = c.get_all_entities(query)

        query = client.EntityQuery(
            instance_id_starts_with="@counter_entity@prefix2",
            include_state=True
        )
        entities_prefix2 = c.get_all_entities(query)
    finally:
        worker.stop()

    assert len([e for e in entities_prefix1 if e.id == entity_id_1]) == 1
    assert len([e for e in entities_prefix1 if e.id == entity_id_2]) == 0

    assert len([e for e in entities_prefix2 if e.id == entity_id_2]) == 1
    assert len([e for e in entities_prefix2 if e.id == entity_id_1]) == 0


def test_get_entities_by_time_range(backend):
    def simple_entity(ctx: entities.EntityContext, input):
        if ctx.operation == "set":
            ctx.set_state(input)

    worker = TaskHubGrpcWorker(host_address=HOST)
    c = TaskHubGrpcClient(host_address=HOST)

    worker.add_entity(simple_entity)
    worker.start()

    try:
        # Get current time
        before_creation = datetime.now(timezone.utc) - timedelta(seconds=5)

        # Create entity
        entity_id = entities.EntityInstanceId("simple_entity", "timeTestEntity")
        c.signal_entity(entity_id, "set", input="test_value")
        time.sleep(3)  # Wait for signal to be processed

        after_creation = datetime.now(timezone.utc) + timedelta(seconds=5)

        # Query by time range
        query = client.EntityQuery(
            last_modified_from=before_creation,
            last_modified_to=after_creation,
            include_state=True
        )
        entities_in_range = c.get_all_entities(query)

        # Query outside time range
        query = client.EntityQuery(
            last_modified_from=after_creation,
            last_modified_to=after_creation + timedelta(hours=1)
        )
        entities_outside_range = c.get_all_entities(query)
    finally:
        worker.stop()

    assert len([e for e in entities_in_range if e.id == entity_id]) == 1
    assert len([e for e in entities_outside_range if e.id == entity_id]) == 0


def test_clean_entity_storage(backend):
    class EmptyEntity(entities.DurableEntity):
        pass

    worker = TaskHubGrpcWorker(host_address=HOST)
    c = TaskHubGrpcClient(host_address=HOST)

    worker.add_entity(EmptyEntity)
    worker.start()

    try:
        # Create an entity and then delete its state to make it empty
        entity_id = entities.EntityInstanceId("EmptyEntity", "toClean")
        c.signal_entity(entity_id, "delete")
        time.sleep(3)  # Wait for signal to be processed

        # Clean entity storage
        result = c.clean_entity_storage(
            remove_empty_entities=True,
            release_orphaned_locks=True
        )
    finally:
        worker.stop()

    # Verify clean result
    assert result.empty_entities_removed >= 0
    assert result.orphaned_locks_released >= 0
