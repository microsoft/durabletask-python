# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""End-to-end tests for work item filtering using the in-memory backend."""

import time

import pytest

from durabletask import client, entities, task, worker
from durabletask.worker import (
    ActivityWorkItemFilter,
    EntityWorkItemFilter,
    OrchestrationWorkItemFilter,
    VersioningOptions,
    VersionMatchStrategy,
    WorkItemFilters,
)
from durabletask.testing import create_test_backend

HOST = "localhost:50060"


@pytest.fixture(autouse=True)
def backend():
    """Create an in-memory backend for testing."""
    b = create_test_backend(port=50060)
    yield b
    b.stop()
    b.reset()


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _plus_one(_: task.ActivityContext, input: int) -> int:
    return input + 1


def _multiply(_: task.ActivityContext, input: int) -> int:
    return input * 2


def _orchestrator_with_activity(ctx: task.OrchestrationContext, start_val: int):
    result = yield ctx.call_activity(_plus_one, input=start_val)
    return result


def _other_orchestrator(ctx: task.OrchestrationContext, _):
    return "other"


# ------------------------------------------------------------------
# Tests: auto-generated filters
# ------------------------------------------------------------------

def test_auto_filters_processes_matching_work_items():
    """Worker with auto-generated filters processes matching orchestrations."""
    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(_orchestrator_with_activity)
        w.add_activity(_plus_one)
        w.use_work_item_filters()
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        id = c.schedule_new_orchestration(_orchestrator_with_activity, input=5)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == "6"


# ------------------------------------------------------------------
# Tests: explicit custom filters
# ------------------------------------------------------------------

def test_explicit_filters_matching():
    """Worker with explicit filters matching registered tasks processes work items."""
    custom_filters = WorkItemFilters(
        orchestrations=[
            OrchestrationWorkItemFilter(
                name=task.get_name(_orchestrator_with_activity)
            )
        ],
        activities=[
            ActivityWorkItemFilter(name=task.get_name(_plus_one))
        ],
    )

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(_orchestrator_with_activity)
        w.add_activity(_plus_one)
        w.use_work_item_filters(custom_filters)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        id = c.schedule_new_orchestration(_orchestrator_with_activity, input=10)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == "11"


# ------------------------------------------------------------------
# Tests: no filters (default behavior)
# ------------------------------------------------------------------

def test_no_filters_processes_all():
    """Without filters, worker processes all work items (default behavior)."""
    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(_orchestrator_with_activity)
        w.add_activity(_plus_one)
        # Intentionally do NOT call use_work_item_filters()
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        id = c.schedule_new_orchestration(_orchestrator_with_activity, input=7)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == "8"


# ------------------------------------------------------------------
# Tests: cleared filters (None)
# ------------------------------------------------------------------

def test_cleared_filters_processes_all():
    """Clearing filters with None restores process-all behavior."""
    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(_orchestrator_with_activity)
        w.add_activity(_plus_one)
        w.use_work_item_filters()      # auto-generate
        w.use_work_item_filters(None)   # then clear
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        id = c.schedule_new_orchestration(_orchestrator_with_activity, input=3)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == "4"


# ------------------------------------------------------------------
# Tests: entity work item filters
# ------------------------------------------------------------------

def test_entity_filters_process_matching_entity():
    """Worker with entity filters processes matching entity signals."""
    invoked = False

    class Counter(entities.DurableEntity):
        def add(self, amount: int):
            self.set_state(self.get_state(int, 0) + amount)
            nonlocal invoked
            invoked = True

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_entity(Counter)
        w.use_work_item_filters(WorkItemFilters(
            entities=[EntityWorkItemFilter(name="counter")],
        ))
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        entity_id = entities.EntityInstanceId("counter", "myKey")
        c.signal_entity(entity_id, "add", input=10)
        time.sleep(2)  # wait for the signal to be processed

        state = c.get_entity(entity_id, include_state=True)

    assert invoked
    assert state is not None
    assert state.get_state(int) == 10


# ------------------------------------------------------------------
# Tests: non-matching filters prevent processing
# ------------------------------------------------------------------

def test_non_matching_orchestrator_not_processed():
    """Work items for unmatched orchestrations are not processed."""
    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        # Register both orchestrators but only filter for one
        w.add_orchestrator(_orchestrator_with_activity)
        w.add_orchestrator(_other_orchestrator)
        w.add_activity(_plus_one)
        w.use_work_item_filters(WorkItemFilters(
            orchestrations=[
                OrchestrationWorkItemFilter(
                    name=task.get_name(_other_orchestrator)
                ),
            ],
            activities=[
                ActivityWorkItemFilter(name=task.get_name(_plus_one)),
            ],
        ))
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)

        # Schedule the non-matching orchestration — should NOT be processed
        non_match_id = c.schedule_new_orchestration(
            _orchestrator_with_activity, input=1)

        # Schedule the matching orchestration — should complete
        match_id = c.schedule_new_orchestration(_other_orchestrator)
        match_state = c.wait_for_orchestration_completion(
            match_id, timeout=30)

        # The matching orchestration completes normally
        assert match_state is not None
        assert match_state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert match_state.serialized_output == '"other"'

        # The non-matching orchestration should still be pending
        non_match_state = c.get_orchestration_state(non_match_id)
        assert non_match_state is not None
        assert non_match_state.runtime_status == client.OrchestrationStatus.PENDING


def test_non_matching_entity_not_processed():
    """Work items for unmatched entities are not processed."""
    matched_invoked = False
    unmatched_invoked = False

    class AllowedEntity(entities.DurableEntity):
        def ping(self, _):
            nonlocal matched_invoked
            matched_invoked = True

    class BlockedEntity(entities.DurableEntity):
        def ping(self, _):
            nonlocal unmatched_invoked
            unmatched_invoked = True

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_entity(AllowedEntity)
        w.add_entity(BlockedEntity)
        # Only filter for AllowedEntity
        w.use_work_item_filters(WorkItemFilters(
            entities=[EntityWorkItemFilter(name="allowedentity")],
        ))
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        c.signal_entity(
            entities.EntityInstanceId("allowedentity", "k1"), "ping")
        c.signal_entity(
            entities.EntityInstanceId("blockedentity", "k1"), "ping")
        time.sleep(3)  # wait for processing

    assert matched_invoked
    assert not unmatched_invoked


# ------------------------------------------------------------------
# Tests: version-aware filtering with strict versioning
# ------------------------------------------------------------------

def _simple_v2_orchestrator(ctx: task.OrchestrationContext, input: int):
    """Orchestrator that returns immediately (no activities) for version tests."""
    return input + 1


def test_strict_version_matching_orchestration_completes():
    """Orchestration scheduled with the matching version is processed."""
    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(_simple_v2_orchestrator)
        w.use_versioning(VersioningOptions(
            version="2.0",
            match_strategy=VersionMatchStrategy.STRICT,
        ))
        w.use_work_item_filters()  # auto-generate with version
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)
        id = c.schedule_new_orchestration(
            _simple_v2_orchestrator, input=10, version="2.0")
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == "11"


def test_strict_version_incompatible_orchestration_stays_pending():
    """Orchestration with an incompatible version is not dispatched and stays pending."""
    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(_simple_v2_orchestrator)
        w.use_versioning(VersioningOptions(
            version="2.0",
            match_strategy=VersionMatchStrategy.STRICT,
        ))
        w.use_work_item_filters()
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)

        # Schedule with version "1.0" — incompatible with the worker's "2.0"
        bad_id = c.schedule_new_orchestration(
            _simple_v2_orchestrator, input=5, version="1.0")

        # Schedule a compatible one so we can confirm the worker is active
        good_id = c.schedule_new_orchestration(
            _simple_v2_orchestrator, input=5, version="2.0")
        good_state = c.wait_for_orchestration_completion(good_id, timeout=30)

        assert good_state is not None
        assert good_state.runtime_status == client.OrchestrationStatus.COMPLETED

        # The incompatible orchestration must remain pending (not failed)
        bad_state = c.get_orchestration_state(bad_id)
        assert bad_state is not None
        assert bad_state.runtime_status == client.OrchestrationStatus.PENDING


def test_strict_version_no_version_orchestration_stays_pending():
    """Orchestration scheduled without a version is not dispatched by a strict worker."""
    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(_simple_v2_orchestrator)
        w.use_versioning(VersioningOptions(
            version="2.0",
            match_strategy=VersionMatchStrategy.STRICT,
        ))
        w.use_work_item_filters()
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)

        # Schedule without any version
        no_ver_id = c.schedule_new_orchestration(
            _simple_v2_orchestrator, input=1)

        # Schedule a compatible one to prove the worker is running
        good_id = c.schedule_new_orchestration(
            _simple_v2_orchestrator, input=1, version="2.0")
        good_state = c.wait_for_orchestration_completion(good_id, timeout=30)
        assert good_state is not None
        assert good_state.runtime_status == client.OrchestrationStatus.COMPLETED

        # The unversioned orchestration must remain pending
        no_ver_state = c.get_orchestration_state(no_ver_id)
        assert no_ver_state is not None
        assert no_ver_state.runtime_status == client.OrchestrationStatus.PENDING


def test_strict_version_explicit_filters_with_versions():
    """Explicit filters with version constraints enforce strict matching."""
    custom_filters = WorkItemFilters(
        orchestrations=[
            OrchestrationWorkItemFilter(
                name=task.get_name(_simple_v2_orchestrator),
                versions=["3.0"],
            ),
        ],
    )

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(_simple_v2_orchestrator)
        w.use_work_item_filters(custom_filters)
        w.start()

        c = client.TaskHubGrpcClient(host_address=HOST)

        # Version "2.0" does not match the filter's "3.0"
        bad_id = c.schedule_new_orchestration(
            _simple_v2_orchestrator, input=1, version="2.0")

        # Version "3.0" should match
        good_id = c.schedule_new_orchestration(
            _simple_v2_orchestrator, input=1, version="3.0")
        good_state = c.wait_for_orchestration_completion(good_id, timeout=30)

        assert good_state is not None
        assert good_state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert good_state.serialized_output == "2"

        # Mismatched version must remain pending
        bad_state = c.get_orchestration_state(bad_id)
        assert bad_state is not None
        assert bad_state.runtime_status == client.OrchestrationStatus.PENDING
