"""Declarer app for the DTS serverless activities sample."""

import os

from azure.identity import DefaultAzureCredential

from durabletask import client, task
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.extensions.serverless import ServerlessActivitiesClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker


REMOTE_ACTIVITY_NAME = "remote_hello"


def hello_orchestrator(ctx: task.OrchestrationContext, name: str):
    """Orchestrator that calls an activity executed by the remote worker image."""
    return (yield ctx.call_activity(REMOTE_ACTIVITY_NAME, input=name))


taskhub_name = os.getenv("DTS_TASK_HUB") or "ServerlessPocHub"
endpoint = os.getenv("DTS_ENDPOINT", "http://localhost:8080")
worker_profile_id = os.getenv("DTS_WORKER_PROFILE_ID", "default")
serverless_image = os.getenv("DTS_SERVERLESS_ACTIVITY_IMAGE", "serverless-remote-worker:local")
serverless_cpu = os.getenv("DTS_SERVERLESS_CPU", "1000m")
serverless_memory = os.getenv("DTS_SERVERLESS_MEMORY", "2048Mi")
serverless_max_activities = int(os.getenv("DTS_SERVERLESS_MAX_ACTIVITIES", "1"))
sample_input = os.getenv("DTS_SAMPLE_HELLO_INPUT", "serverless Python")

print(f"Using taskhub: {taskhub_name}")
print(f"Using endpoint: {endpoint}")
print(f"Declaring serverless activity image: {serverless_image}")

secure_channel = endpoint.startswith("https://") or endpoint.startswith("grpcs://")
credential = DefaultAzureCredential() if secure_channel else None

serverless_client = ServerlessActivitiesClient(
    host_address=endpoint,
    secure_channel=secure_channel,
    taskhub=taskhub_name,
    token_credential=credential)
serverless_client.declare_serverless_activities(
    activity_names=[REMOTE_ACTIVITY_NAME],
    worker_profile_id=worker_profile_id,
    container_image=serverless_image,
    cpu=serverless_cpu,
    memory=serverless_memory,
    max_concurrent_activities=serverless_max_activities)

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
