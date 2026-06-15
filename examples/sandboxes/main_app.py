"""Declarer app for the Durable Task Scheduler sandbox activities sample."""

import os

from azure.identity import DefaultAzureCredential

from durabletask import client, task
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.preview.sandboxes import SandboxActivitiesClient
from durabletask.azuremanaged.preview.sandboxes import SandboxWorkerProfile
from durabletask.azuremanaged.preview.sandboxes import sandbox_worker_profile
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker

from activities import REMOTE_HELLO


def hello_orchestrator(ctx: task.OrchestrationContext, name: str):
    """Orchestrator that calls an activity executed by the remote worker image."""
    return (yield ctx.call_activity(REMOTE_HELLO.name, input=name))


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value
    raise RuntimeError(f"Set {name} before running the sandbox sample.")


taskhub_name = os.getenv("DTS_TASK_HUB") or "SandboxPocHub"
endpoint = os.getenv("DTS_ENDPOINT", "http://localhost:8080")
worker_profile_id = _get_required_env("DTS_WORKER_PROFILE_ID")
container_image = (
    os.getenv("DTS_SANDBOX_CONTAINER_IMAGE")
    or "sandboxes-remote-worker:local")
image_pull_managed_identity_client_id = _get_required_env("DTS_SANDBOX_IMAGE_PULL_UMI_CLIENT_ID")
scheduler_managed_identity_client_id = _get_required_env("DTS_SANDBOX_SCHEDULER_UMI_CLIENT_ID")
sample_input = os.getenv("DTS_SAMPLE_HELLO_INPUT", "sandbox Python")
sample_timeout_seconds = int(os.getenv("DTS_SAMPLE_TIMEOUT_SECONDS", "300"))


@sandbox_worker_profile(worker_profile_id)
class RemoteWorkerProfile(SandboxWorkerProfile):
    """Sandbox worker profile used by the sample remote activity."""

    def configure(self, options) -> None:
        options.container_image = container_image
        options.image_pull_managed_identity_client_id = image_pull_managed_identity_client_id
        options.scheduler_managed_identity_client_id = scheduler_managed_identity_client_id
        options.cpu = "1000m"
        options.memory = "2048Mi"
        options.max_concurrent_activities = 1
        options.environment_variables["SANDBOX_SAMPLE_MARKER"] = "sandboxes-python-sample-marker"
        options.add_activity(REMOTE_HELLO.name, version=REMOTE_HELLO.version)


print(f"Using taskhub: {taskhub_name}")
print(f"Using endpoint: {endpoint}")
print(f"Declaring sandbox activity image: {container_image}")

secure_channel = endpoint.startswith("https://") or endpoint.startswith("grpcs://")
credential = DefaultAzureCredential() if secure_channel else None

sandboxes_client = SandboxActivitiesClient(
    host_address=endpoint,
    secure_channel=secure_channel,
    taskhub=taskhub_name,
    token_credential=credential)
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
