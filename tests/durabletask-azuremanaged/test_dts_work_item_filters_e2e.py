# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for work item filtering against DTS (emulator or deployed)."""

import os
import time

import pytest

from durabletask import client, entities, task
from durabletask.worker import (
    ActivityWorkItemFilter,
    EntityWorkItemFilter,
    OrchestrationWorkItemFilter,
    WorkItemFilters,
)
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker

# NOTE: These tests assume a sidecar process is running. Example command:
#       docker run -i -p 8080:8080 -p 8082:8082 -d mcr.microsoft.com/dts/dts-emulator:latest
pytestmark = pytest.mark.dts

# Read the environment variables
taskhub_name = os.getenv("TASKHUB", "default")
endpoint = os.getenv("ENDPOINT", "http://localhost:8080")


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _plus_one(_: task.ActivityContext, input: int) -> int:
    return input + 1


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
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(_orchestrator_with_activity)
        w.add_activity(_plus_one)
        w.use_work_item_filters()
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        id = c.schedule_new_orchestration(_orchestrator_with_activity, input=5)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == "6"


# ------------------------------------------------------------------
# Tests: explicit custom filters
# ------------------------------------------------------------------

def test_explicit_filters_matching():
    """Worker with explicit filters processes matching work items."""
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

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(_orchestrator_with_activity)
        w.add_activity(_plus_one)
        w.use_work_item_filters(custom_filters)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        id = c.schedule_new_orchestration(_orchestrator_with_activity, input=10)
        state = c.wait_for_orchestration_completion(id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.serialized_output == "11"


# ------------------------------------------------------------------
# Tests: no filters (default behavior)
# ------------------------------------------------------------------

def test_no_filters_processes_all():
    """Without filters the worker processes all work items."""
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(_orchestrator_with_activity)
        w.add_activity(_plus_one)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
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
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(_orchestrator_with_activity)
        w.add_activity(_plus_one)
        w.use_work_item_filters()
        w.use_work_item_filters(None)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
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

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_entity(Counter)
        w.use_work_item_filters(WorkItemFilters(
            entities=[EntityWorkItemFilter(name="counter")],
        ))
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        entity_id = entities.EntityInstanceId("counter", "myKey")
        c.signal_entity(entity_id, "add", input=10)
        time.sleep(5)  # wait for the signal to be processed

        state = c.get_entity(entity_id)

    assert invoked
    assert state is not None
    assert state.get_state(int) == 10


# ------------------------------------------------------------------
# Tests: non-matching filters prevent processing
# ------------------------------------------------------------------

def test_non_matching_orchestrator_not_processed():
    """Work items for unmatched orchestrations are not dispatched."""
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
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

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)

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
    """Work items for unmatched entities are not dispatched."""
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

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_entity(AllowedEntity)
        w.add_entity(BlockedEntity)
        w.use_work_item_filters(WorkItemFilters(
            entities=[EntityWorkItemFilter(name="allowedentity")],
        ))
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                       taskhub=taskhub_name, token_credential=None)
        c.signal_entity(
            entities.EntityInstanceId("allowedentity", "k1"), "ping")
        c.signal_entity(
            entities.EntityInstanceId("blockedentity", "k1"), "ping")
        time.sleep(5)  # wait for processing

    assert matched_invoked
    assert not unmatched_invoked
