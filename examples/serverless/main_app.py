"""Declarer app for the DTS on-demand sandbox activities sample."""

import os

from azure.identity import DefaultAzureCredential

from durabletask import client, task
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.preview.on_demand_sandbox import OnDemandSandboxActivitiesClient
from durabletask.azuremanaged.preview.on_demand_sandbox import OnDemandSandboxWorkerProfile
from durabletask.azuremanaged.preview.on_demand_sandbox import on_demand_sandbox_worker_profile
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker

from activity_names import REMOTE_HELLO


def hello_orchestrator(ctx: task.OrchestrationContext, name: str):
    """Orchestrator that calls an activity executed by the remote worker image."""
    return (yield ctx.call_activity(REMOTE_HELLO, input=name))


def _get_required_env(legacy_name: str, preferred_name: str) -> str:
    value = os.getenv(legacy_name)
    if value:
        return value
    raise RuntimeError(f"Set {preferred_name} before running the on-demand sandbox sample.")


taskhub_name = os.getenv("DTS_TASK_HUB") or "OnDemandSandboxPocHub"
endpoint = os.getenv("DTS_ENDPOINT", "http://localhost:8080")
worker_profile_id = os.getenv("DTS_WORKER_PROFILE_ID", "default")
container_image = (
    os.getenv("DTS_ON_DEMAND_SANDBOX_CONTAINER_IMAGE")
    or os.getenv("DTS_SERVERLESS_CONTAINER_IMAGE")
    or "on-demand-sandbox-remote-worker:local")
image_pull_managed_identity_client_id = (
    os.getenv("DTS_ON_DEMAND_SANDBOX_IMAGE_PULL_UMI_CLIENT_ID")
    or _get_required_env("DTS_SERVERLESS_IMAGE_PULL_UMI_CLIENT_ID", "DTS_ON_DEMAND_SANDBOX_IMAGE_PULL_UMI_CLIENT_ID"))
scheduler_managed_identity_client_id = (
    os.getenv("DTS_ON_DEMAND_SANDBOX_SCHEDULER_UMI_CLIENT_ID")
    or _get_required_env("DTS_SERVERLESS_SCHEDULER_UMI_CLIENT_ID", "DTS_ON_DEMAND_SANDBOX_SCHEDULER_UMI_CLIENT_ID"))
sample_input = os.getenv("DTS_SAMPLE_HELLO_INPUT", "on-demand sandbox Python")
sample_timeout_seconds = int(os.getenv("DTS_SAMPLE_TIMEOUT_SECONDS", "300"))


@on_demand_sandbox_worker_profile(worker_profile_id)
class RemoteWorkerProfile(OnDemandSandboxWorkerProfile):
    """On-demand sandbox worker profile used by the sample remote activity."""

    def configure(self, options) -> None:
        options.container_image = container_image
        options.image_pull_managed_identity_client_id = image_pull_managed_identity_client_id
        options.scheduler_managed_identity_client_id = scheduler_managed_identity_client_id
        options.cpu = "1000m"
        options.memory = "2048Mi"
        options.max_concurrent_activities = 1
        options.environment_variables["ON_DEMAND_SANDBOX_SAMPLE_MARKER"] = "on-demand-sandbox-python-sample-marker"
        options.add_activity(REMOTE_HELLO)


print(f"Using taskhub: {taskhub_name}")
print(f"Using endpoint: {endpoint}")
print(f"Declaring on-demand sandbox activity image: {container_image}")

secure_channel = endpoint.startswith("https://") or endpoint.startswith("grpcs://")
credential = DefaultAzureCredential() if secure_channel else None

on_demand_sandbox_client = OnDemandSandboxActivitiesClient(
    host_address=endpoint,
    secure_channel=secure_channel,
    taskhub=taskhub_name,
    token_credential=credential)
on_demand_sandbox_client.enable_on_demand_sandbox_activities()

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
