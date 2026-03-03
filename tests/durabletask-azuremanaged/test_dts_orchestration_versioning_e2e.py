# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import os

import pytest

from durabletask import client, task, worker
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker

# NOTE: These tests assume a sidecar process is running. Example command:
#       docker run -i -p 8080:8080 -p 8082:8082 -d mcr.microsoft.com/dts/dts-emulator:latest
pytestmark = pytest.mark.dts

# Read the environment variables
taskhub_name = os.getenv("TASKHUB", "default")
endpoint = os.getenv("ENDPOINT", "http://localhost:8080")


def plus_one(_: task.ActivityContext, input: int) -> int:
    return input + 1


def single_activity(ctx: task.OrchestrationContext, start_val: int):
    yield ctx.call_activity(plus_one, input=start_val)
    return "Success"


def test_version_flows_through_to_orchestration_context():
    """Verifies the version set by the client is visible on ctx.version."""

    def return_version(ctx: task.OrchestrationContext, _: None):
        return ctx.version

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(return_version)
        w.use_versioning(worker.VersioningOptions(
            version="2.5.0",
            default_version="2.5.0",
            match_strategy=worker.VersionMatchStrategy.CURRENT_OR_OLDER,
            failure_strategy=worker.VersionFailureStrategy.FAIL
        ))
        w.start()

        task_hub_client = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                     taskhub=taskhub_name, token_credential=None)
        id = task_hub_client.schedule_new_orchestration(
            return_version, version="2.5.0")
        state = task_hub_client.wait_for_orchestration_completion(
            id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.failure_details is None
    assert state.serialized_output == json.dumps("2.5.0")


def test_strict_version_mismatch_fails():
    """Worker with STRICT matching fails when orchestration version differs."""

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(single_activity)
        w.add_activity(plus_one)
        w.use_versioning(worker.VersioningOptions(
            version="1.0.0",
            default_version="1.0.0",
            match_strategy=worker.VersionMatchStrategy.STRICT,
            failure_strategy=worker.VersionFailureStrategy.FAIL
        ))
        w.start()

        task_hub_client = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                     taskhub=taskhub_name, token_credential=None,
                                                     default_version="1.0.0")
        id = task_hub_client.schedule_new_orchestration(
            single_activity, input=1, version="2.0.0")
        state = task_hub_client.wait_for_orchestration_completion(
            id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.FAILED
    assert state.failure_details is not None
    assert "does not match" in state.failure_details.message


def test_newer_orchestration_version_fails_current_or_older():
    """Worker with CURRENT_OR_OLDER fails when orchestration version is newer."""

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(single_activity)
        w.add_activity(plus_one)
        w.use_versioning(worker.VersioningOptions(
            version="1.0.0",
            default_version="1.0.0",
            match_strategy=worker.VersionMatchStrategy.CURRENT_OR_OLDER,
            failure_strategy=worker.VersionFailureStrategy.FAIL
        ))
        w.start()

        task_hub_client = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                     taskhub=taskhub_name, token_credential=None,
                                                     default_version="1.0.0")
        id = task_hub_client.schedule_new_orchestration(
            single_activity, input=1, version="1.1.0")
        state = task_hub_client.wait_for_orchestration_completion(
            id, timeout=30)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.FAILED
    assert state.failure_details is not None
    assert "is greater than" in state.failure_details.message
