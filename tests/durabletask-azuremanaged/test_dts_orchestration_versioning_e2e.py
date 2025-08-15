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


def sequence(ctx: task.OrchestrationContext, start_val: int):
    numbers = [start_val]
    current = start_val
    for _ in range(10):
        current = yield ctx.call_activity(plus_one, input=current)
        numbers.append(current)
    return numbers


def test_versioned_orchestration_succeeds():
    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(sequence)
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
        id = task_hub_client.schedule_new_orchestration(sequence, input=1, version="1.0.0")
        state = task_hub_client.wait_for_orchestration_completion(
            id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(sequence)
    assert state.instance_id == id
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.failure_details is None
    assert state.serialized_input == json.dumps(1)
    assert state.serialized_output == json.dumps([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
    assert state.serialized_custom_status is None


def test_lower_version_worker_fails():
    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(single_activity)
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
        id = task_hub_client.schedule_new_orchestration(single_activity, input=1, version="1.1.0")
        state = task_hub_client.wait_for_orchestration_completion(
            id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(single_activity)
    assert state.instance_id == id
    assert state.runtime_status == client.OrchestrationStatus.FAILED
    assert state.failure_details is not None
    assert state.failure_details.message.find("The orchestration version '1.1.0' is greater than the worker version '1.0.0'.") >= 0


def test_lower_version_worker_no_match_strategy_succeeds():
    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(single_activity)
        w.add_activity(plus_one)
        w.use_versioning(worker.VersioningOptions(
            version="1.0.0",
            default_version="1.0.0",
            match_strategy=worker.VersionMatchStrategy.NONE,
            failure_strategy=worker.VersionFailureStrategy.FAIL
        ))
        w.start()

        task_hub_client = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                     taskhub=taskhub_name, token_credential=None,
                                                     default_version="1.0.0")
        id = task_hub_client.schedule_new_orchestration(single_activity, input=1, version="1.1.0")
        state = task_hub_client.wait_for_orchestration_completion(
            id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(single_activity)
    assert state.instance_id == id
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.failure_details is None


def test_upper_version_worker_succeeds():
    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(single_activity)
        w.add_activity(plus_one)
        w.use_versioning(worker.VersioningOptions(
            version="1.1.0",
            default_version="1.1.0",
            match_strategy=worker.VersionMatchStrategy.CURRENT_OR_OLDER,
            failure_strategy=worker.VersionFailureStrategy.FAIL
        ))
        w.start()

        task_hub_client = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                     taskhub=taskhub_name, token_credential=None,
                                                     default_version="1.1.0")
        id = task_hub_client.schedule_new_orchestration(single_activity, input=1, version="1.0.0")
        state = task_hub_client.wait_for_orchestration_completion(
            id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(single_activity)
    assert state.instance_id == id
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.failure_details is None


def test_upper_version_worker_strict_fails():
    # Start a worker, which will connect to the sidecar in a background thread
    instance_id: str = ''
    thrown = False
    try:
        with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                        taskhub=taskhub_name, token_credential=None) as w:
            w.add_orchestrator(single_activity)
            w.add_activity(plus_one)
            w.use_versioning(worker.VersioningOptions(
                version="1.0.0",
                default_version="1.1.0",
                match_strategy=worker.VersionMatchStrategy.STRICT,
                failure_strategy=worker.VersionFailureStrategy.REJECT
            ))
            w.start()

            task_hub_client = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                         taskhub=taskhub_name, token_credential=None,
                                                         default_version="1.1.0")
            instance_id = task_hub_client.schedule_new_orchestration(single_activity, input=1)
            state = task_hub_client.wait_for_orchestration_completion(
                instance_id, timeout=5)
    except TimeoutError as e:
        thrown = True
        assert str(e).find("Timed-out waiting for the orchestration to complete") >= 0

    assert thrown is True

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(single_activity)
        w.add_activity(plus_one)
        w.use_versioning(worker.VersioningOptions(
            version="1.1.0",
            default_version="1.1.0",
            match_strategy=worker.VersionMatchStrategy.STRICT,
            failure_strategy=worker.VersionFailureStrategy.REJECT
        ))
        w.start()

        task_hub_client = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                     taskhub=taskhub_name, token_credential=None,
                                                     default_version="1.1.0")
        state = task_hub_client.wait_for_orchestration_completion(
            instance_id, timeout=5)

    assert state is not None
    assert state.name == task.get_name(single_activity)
    assert state.instance_id == instance_id
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.failure_details is None


def test_reject_abandons_and_reprocess():
    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(single_activity)
        w.add_activity(plus_one)
        w.use_versioning(worker.VersioningOptions(
            version="1.1.0",
            default_version="1.1.0",
            match_strategy=worker.VersionMatchStrategy.STRICT,
            failure_strategy=worker.VersionFailureStrategy.FAIL
        ))
        w.start()

        task_hub_client = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                                     taskhub=taskhub_name, token_credential=None,
                                                     default_version="1.1.0")
        id = task_hub_client.schedule_new_orchestration(single_activity, input=1, version="1.0.0")
        state = task_hub_client.wait_for_orchestration_completion(
            id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(single_activity)
    assert state.instance_id == id
    assert state.runtime_status == client.OrchestrationStatus.FAILED
    assert state.failure_details is not None
    assert state.failure_details.message.find("The orchestration version '1.0.0' does not match the worker version '1.1.0'.") >= 0


# Sub-orchestration tests


def sequence_suborchestator(ctx: task.OrchestrationContext, start_val: int):
    numbers = []
    for current in range(start_val, start_val + 5):
        current = yield ctx.call_activity(plus_one, input=current)
        numbers.append(current)
    return numbers


def sequence_parent(ctx: task.OrchestrationContext, sub_orchestration_version: str):
    tasks = []
    for current in range(2):
        tasks.append(ctx.call_sub_orchestrator(sequence_suborchestator, input=current * 5, version=sub_orchestration_version))
    results = yield task.when_all(tasks)
    numbers = []
    for result in results:
        numbers.extend(result)
    return numbers


def test_versioned_sub_orchestration_succeeds():
    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(sequence_parent)
        w.add_orchestrator(sequence_suborchestator)
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
        id = task_hub_client.schedule_new_orchestration(sequence_parent, input='1.0.0', version="1.0.0")
        state = task_hub_client.wait_for_orchestration_completion(
            id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(sequence_parent)
    assert state.instance_id == id
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED
    assert state.failure_details is None
    assert state.serialized_input == json.dumps("1.0.0")
    assert state.serialized_output == json.dumps([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    assert state.serialized_custom_status is None


def test_higher_versioned_sub_orchestration_fails():
    # Start a worker, which will connect to the sidecar in a background thread
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(sequence_parent)
        w.add_orchestrator(sequence_suborchestator)
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
        id = task_hub_client.schedule_new_orchestration(sequence_parent, input='1.1.0', version="1.0.0")
        state = task_hub_client.wait_for_orchestration_completion(
            id, timeout=30)

    assert state is not None
    assert state.name == task.get_name(sequence_parent)
    assert state.instance_id == id
    assert state.runtime_status == client.OrchestrationStatus.FAILED
    assert state.failure_details is not None
    assert state.failure_details.message.find("The orchestration version '1.1.0' is greater than the worker version '1.0.0'.") >= 0
