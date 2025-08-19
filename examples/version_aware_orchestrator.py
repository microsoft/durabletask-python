"""End-to-end sample that demonstrates how to configure an orchestrator
that a dynamic number activity functions in parallel, waits for them all
to complete, and prints an aggregate summary of the outputs."""
import os

from azure.identity import DefaultAzureCredential

from durabletask import client, task, worker
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker


def activity_v1(ctx: task.ActivityContext, input: str) -> str:
    """Activity function that returns a result for a given work item"""
    print("processing input:", input)
    return "Success from activity v1"


def activity_v2(ctx: task.ActivityContext, input: str) -> str:
    """Activity function that returns a result for a given work item"""
    print("processing input:", input)
    return "Success from activity v2"


def orchestrator(ctx: task.OrchestrationContext, _):
    """Orchestrator function that checks the orchestration version and has version-aware behavior
    Use case: Updating an orchestrator with new logic while maintaining compatibility with previously
    started orchestrations"""
    if ctx.version == "1.0.0":
        # For version 1.0.0, we use the original logic
        result: int = yield ctx.call_activity(activity_v1, input="input for v1")
    elif ctx.version == "2.0.0":
        # For version 2.0.0, we use the updated logic
        result: int = yield ctx.call_activity(activity_v2, input="input for v2")
    else:
        raise ValueError(f"Unsupported version: {ctx.version}")
    return {
        'result': result,
    }


# Use environment variables if provided, otherwise use default emulator values
taskhub_name = os.getenv("TASKHUB", "default")
endpoint = os.getenv("ENDPOINT", "http://localhost:8080")

print(f"Using taskhub: {taskhub_name}")
print(f"Using endpoint: {endpoint}")

# Set credential to None for emulator, or DefaultAzureCredential for Azure
credential = None if endpoint == "http://localhost:8080" else DefaultAzureCredential()

# configure and start the worker - use secure_channel=False for emulator
secure_channel = endpoint != "http://localhost:8080"
with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=secure_channel,
                                taskhub=taskhub_name, token_credential=credential) as w:
    # This worker is versioned for v2, as the orchestrator code has already been updated
    # CURRENT_OR_OLDER allows this worker to process orchestrations versioned below 2.0.0 - e.g. 1.0.0
    w.use_versioning(worker.VersioningOptions(
        version="2.0.0",
        default_version="2.0.0",
        match_strategy=worker.VersionMatchStrategy.CURRENT_OR_OLDER,
        failure_strategy=worker.VersionFailureStrategy.FAIL
    ))
    w.add_orchestrator(orchestrator)
    w.add_activity(activity_v1)
    w.add_activity(activity_v2)
    w.start()

    # create a client, start an orchestration, and wait for it to finish
    # The client's version matches the worker's version
    c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=secure_channel,
                                   taskhub=taskhub_name, token_credential=credential,
                                   default_version="2.0.0")
    # Schedule a new orchestration manually versioned to 1.0.0
    # Normally, this would have been scheduled before the worker started from a worker also versioned v1.0.0,
    # Here we are doing it manually to avoid creating two workers
    instance_id_v1 = c.schedule_new_orchestration(orchestrator, version="1.0.0")
    state_v1 = c.wait_for_orchestration_completion(instance_id_v1, timeout=30)
    if state_v1 and state_v1.runtime_status == client.OrchestrationStatus.COMPLETED:
        print(f'Orchestration v1 completed! Result: {state_v1.serialized_output}')
    elif state_v1:
        print(f'Orchestration v1 failed: {state_v1.failure_details}')

    # Also check that the orchestrator can be run with the current version
    instance_id = c.schedule_new_orchestration(orchestrator)
    state = c.wait_for_orchestration_completion(instance_id, timeout=30)
    if state and state.runtime_status == client.OrchestrationStatus.COMPLETED:
        print(f'Orchestration completed! Result: {state.serialized_output}')
    elif state:
        print(f'Orchestration failed: {state.failure_details}')

    exit()
