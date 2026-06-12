"""Declarer app for the DTS serverless activities sample."""

import os

from azure.identity import DefaultAzureCredential

from durabletask import client, task
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.extensions.serverless import ServerlessActivitiesClient
from durabletask.azuremanaged.extensions.serverless import ServerlessWorkerProfile
from durabletask.azuremanaged.extensions.serverless import serverless_worker_profile
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker

from activity_names import REMOTE_HELLO


def hello_orchestrator(ctx: task.OrchestrationContext, name: str):
    """Orchestrator that calls an activity executed by the remote worker image."""
    return (yield ctx.call_activity(REMOTE_HELLO, input=name))


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value
    raise RuntimeError(f"Set {name} before running the serverless sample.")


taskhub_name = os.getenv("DTS_TASK_HUB") or "ServerlessPocHub"
endpoint = os.getenv("DTS_ENDPOINT", "http://localhost:8080")
worker_profile_id = _get_required_env("DTS_WORKER_PROFILE_ID")
container_image = os.getenv("DTS_SERVERLESS_CONTAINER_IMAGE", "serverless-remote-worker:local")
sample_input = os.getenv("DTS_SAMPLE_HELLO_INPUT", "serverless Python")


@serverless_worker_profile(worker_profile_id)
class RemoteWorkerProfile(ServerlessWorkerProfile):
    """Serverless worker profile used by the sample remote activity."""

    def configure(self, options) -> None:
        options.container_image = container_image
        options.cpu = "1000m"
        options.memory = "2048Mi"
        options.max_concurrent_activities = 1
        options.environment_variables["SERVERLESS_SAMPLE_MARKER"] = "serverless-python-sample-marker"
        options.add_activity(REMOTE_HELLO)


print(f"Using taskhub: {taskhub_name}")
print(f"Using endpoint: {endpoint}")
print(f"Declaring serverless activity image: {container_image}")

secure_channel = endpoint.startswith("https://") or endpoint.startswith("grpcs://")
credential = DefaultAzureCredential() if secure_channel else None

serverless_client = ServerlessActivitiesClient(
    host_address=endpoint,
    secure_channel=secure_channel,
    taskhub=taskhub_name,
    token_credential=credential)
serverless_client.enable_serverless_activities()

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
    state = durable_client.wait_for_orchestration_completion(instance_id, timeout=120)
    if state and state.runtime_status == client.OrchestrationStatus.COMPLETED:
        print(f"Orchestration completed! Result: {state.serialized_output}")
    elif state:
        print(f"Orchestration failed: {state.failure_details}")
