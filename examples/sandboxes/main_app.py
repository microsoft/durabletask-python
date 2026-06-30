# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Declarer app for the Durable Task Scheduler sandbox activities sample."""

import os
from collections.abc import Generator
from typing import Any

from azure.core.credentials import TokenCredential
from azure.identity import DefaultAzureCredential

from durabletask import client, task
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.preview.sandboxes import SandboxActivitiesClient
from durabletask.azuremanaged.preview.sandboxes import SandboxWorkerProfile
from durabletask.azuremanaged.preview.sandboxes import SandboxWorkerProfileOptions
from durabletask.azuremanaged.preview.sandboxes import sandbox_worker_profile
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker

from activities import REMOTE_HELLO

WORKER_PROFILE_ID = "remote-hello-profile"


def hello_orchestrator(ctx: task.OrchestrationContext, name: str) -> Generator[task.Task[Any], Any, Any]:
    """Orchestrator that calls an activity executed by the remote worker image."""
    return (yield ctx.call_activity(REMOTE_HELLO.name, input=name))


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value
    raise RuntimeError(f"Set {name} before running the sandbox sample.")


def _parse_scheduler_connection_string(connection_string: str) -> dict[str, str]:
    settings: dict[str, str] = {}
    for segment in connection_string.split(";"):
        if not segment.strip():
            continue
        key, separator, value = segment.partition("=")
        if not separator or not key.strip():
            raise RuntimeError(
                "DURABLE_TASK_SCHEDULER_CONNECTION_STRING must use key=value segments.")
        settings[key.strip().lower()] = value.strip()
    return settings


def _resolve_scheduler_connection() -> tuple[str, str, bool, TokenCredential | None]:
    settings = _parse_scheduler_connection_string(
        _get_required_env("DURABLE_TASK_SCHEDULER_CONNECTION_STRING"))
    endpoint = settings.get("endpoint")
    taskhub = settings.get("taskhub")
    if not endpoint:
        raise RuntimeError("DURABLE_TASK_SCHEDULER_CONNECTION_STRING must include Endpoint.")
    if not taskhub:
        raise RuntimeError("DURABLE_TASK_SCHEDULER_CONNECTION_STRING must include TaskHub.")

    authentication = settings.get("authentication", "").lower()
    if authentication in ("", "none"):
        credential = None
    elif authentication == "defaultazure":
        credential = DefaultAzureCredential()
    else:
        raise RuntimeError(
            "DURABLE_TASK_SCHEDULER_CONNECTION_STRING Authentication must be DefaultAzure or None.")

    endpoint = endpoint.strip()
    secure_channel = endpoint.lower().startswith(("https://", "grpcs://"))
    return endpoint, taskhub.strip(), secure_channel, credential


endpoint, taskhub_name, secure_channel, credential = _resolve_scheduler_connection()
container_image = _get_required_env("DTS_SANDBOX_CONTAINER_IMAGE")
image_pull_managed_identity_client_id = _get_required_env("DTS_SANDBOX_IMAGE_PULL_UMI_CLIENT_ID")
scheduler_managed_identity_client_id = _get_required_env("DTS_SANDBOX_SCHEDULER_UMI_CLIENT_ID")
sample_input = os.getenv("DTS_SAMPLE_HELLO_INPUT", "sandbox Python")
sample_timeout_seconds = int(os.getenv("DTS_SAMPLE_TIMEOUT_SECONDS", "300"))


@sandbox_worker_profile(WORKER_PROFILE_ID)
class RemoteWorkerProfile(SandboxWorkerProfile):
    """Sandbox worker profile used by the sample remote activity."""

    def configure(self, options: SandboxWorkerProfileOptions) -> None:
        options.image.image_ref = container_image
        options.image.managed_identity_client_id = image_pull_managed_identity_client_id
        options.scheduler_managed_identity_client_id = scheduler_managed_identity_client_id
        options.cpu = "1000m"
        options.memory = "2048Mi"
        options.max_concurrent_activities = 1
        options.environment_variables["SANDBOX_SAMPLE_MARKER"] = "sandboxes-python-sample-marker"
        options.add_activity(REMOTE_HELLO.name)


print(f"Using taskhub: {taskhub_name}")
print(f"Using endpoint: {endpoint}")
print(f"Declaring sandbox activity image: {container_image}")

with SandboxActivitiesClient(
        host_address=endpoint,
        secure_channel=secure_channel,
        taskhub=taskhub_name,
        token_credential=credential) as sandboxes_client:
    sandboxes_client.enable_sandbox_activities()

with DurableTaskSchedulerWorker(
        host_address=endpoint,
        secure_channel=secure_channel,
        taskhub=taskhub_name,
        token_credential=credential) as worker:
    worker.add_orchestrator(hello_orchestrator)
    worker.use_work_item_filters()
    worker.start()

    durable_client = DurableTaskSchedulerClient(
        host_address=endpoint,
        secure_channel=secure_channel,
        taskhub=taskhub_name,
        token_credential=credential)
    instance_id = durable_client.schedule_new_orchestration(
        hello_orchestrator,
        input=sample_input)
    state = durable_client.wait_for_orchestration_completion(instance_id, timeout=sample_timeout_seconds)
    if state and state.runtime_status == client.OrchestrationStatus.COMPLETED:
        print(f"Orchestration completed! Result: {state.serialized_output}")
    elif state:
        print(f"Orchestration failed: {state.failure_details}")
