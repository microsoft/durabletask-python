# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json

import pytest

from durabletask import client, task, worker
from durabletask.testing import create_test_backend

HOST = "localhost:50055"


@pytest.fixture(autouse=True)
def backend():
    """Create an in-memory backend for testing."""
    b = create_test_backend(port=50055)
    yield b
    b.stop()
    b.reset()


def test_version_flows_through_to_orchestration_context():
    """Verifies the version set by the client is visible on ctx.version."""

    def return_version(ctx: task.OrchestrationContext, _: None):
        return ctx.version

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(return_version)
        w.use_versioning(worker.VersioningOptions(
            version="2.5.0",
            default_version="2.5.0",
            match_strategy=worker.VersionMatchStrategy.CURRENT_OR_OLDER,
            failure_strategy=worker.VersionFailureStrategy.FAIL
        ))
        w.start()

        task_hub_client = client.TaskHubGrpcClient(host_address=HOST)
        id = task_hub_client.schedule_new_orchestration(
            return_version, version="2.5.0")
        state = task_hub_client.wait_for_orchestration_completion(
            id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.failure_details is None
    assert state.serialized_output == json.dumps("2.5.0")


def test_strict_version_mismatch_fails():
    """Worker with STRICT matching fails an orchestration whose version differs."""

    def simple(ctx: task.OrchestrationContext, _: None):
        return "done"

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(simple)
        w.use_versioning(worker.VersioningOptions(
            version="1.0.0",
            default_version="1.0.0",
            match_strategy=worker.VersionMatchStrategy.STRICT,
            failure_strategy=worker.VersionFailureStrategy.FAIL
        ))
        w.start()

        task_hub_client = client.TaskHubGrpcClient(host_address=HOST)
        id = task_hub_client.schedule_new_orchestration(
            simple, version="2.0.0")
        state = task_hub_client.wait_for_orchestration_completion(
            id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.FAILED
    assert state.failure_details is not None
    assert "does not match the worker version" in state.failure_details.message


def test_newer_orchestration_version_fails_current_or_older():
    """Worker rejects orchestrations with a version newer than its own."""

    def simple(ctx: task.OrchestrationContext, _: None):
        return "done"

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(simple)
        w.use_versioning(worker.VersioningOptions(
            version="1.0.0",
            default_version="1.0.0",
            match_strategy=worker.VersionMatchStrategy.CURRENT_OR_OLDER,
            failure_strategy=worker.VersionFailureStrategy.FAIL
        ))
        w.start()

        task_hub_client = client.TaskHubGrpcClient(host_address=HOST)
        id = task_hub_client.schedule_new_orchestration(
            simple, version="1.1.0")
        state = task_hub_client.wait_for_orchestration_completion(
            id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.FAILED
    assert state.failure_details is not None
    assert "is greater than the worker version" in state.failure_details.message
