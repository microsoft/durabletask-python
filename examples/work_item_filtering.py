"""End-to-end sample that demonstrates how to use work item filters
to control which orchestrations and activities a worker processes."""
import os

from azure.identity import DefaultAzureCredential

from durabletask import client, task, worker
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker


# --- Activity definitions ---

def greet(ctx: task.ActivityContext, name: str) -> str:
    """Activity that returns a greeting."""
    return f"Hello, {name}!"


def farewell(ctx: task.ActivityContext, name: str) -> str:
    """Activity that returns a farewell message."""
    return f"Goodbye, {name}!"


# --- Orchestrator definitions ---

def greeting_orchestrator(ctx: task.OrchestrationContext, name: str):
    """Orchestrator that calls the greet activity."""
    result = yield ctx.call_activity(greet, input=name)
    return result


def farewell_orchestrator(ctx: task.OrchestrationContext, name: str):
    """Orchestrator that calls the farewell activity."""
    result = yield ctx.call_activity(farewell, input=name)
    return result


# --- Main ---

# Use environment variables if provided, otherwise use default emulator values
taskhub_name = os.getenv("TASKHUB", "default")
endpoint = os.getenv("ENDPOINT", "http://localhost:8080")

print(f"Using taskhub: {taskhub_name}")
print(f"Using endpoint: {endpoint}")

# Set credential to None for emulator, or DefaultAzureCredential for Azure
secure_channel = endpoint.startswith("https://")
credential = DefaultAzureCredential() if secure_channel else None

# === Example 1: Auto-generated filters ===
# Calling use_work_item_filters() with no arguments tells the worker to
# automatically build filters from the registered orchestrators, activities,
# and entities. The backend will then only dispatch matching work items.
print("\n--- Example 1: Auto-generated filters ---")
with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=secure_channel,
                                taskhub=taskhub_name, token_credential=credential) as w:
    w.add_orchestrator(greeting_orchestrator)
    w.add_activity(greet)
    # Opt in to work item filtering — filters are derived from the registry
    w.use_work_item_filters()
    w.start()

    c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=secure_channel,
                                   taskhub=taskhub_name, token_credential=credential)
    instance_id = c.schedule_new_orchestration(greeting_orchestrator, input="World")
    state = c.wait_for_orchestration_completion(instance_id, timeout=30)
    if state and state.runtime_status == client.OrchestrationStatus.COMPLETED:
        print(f"  Completed: {state.serialized_output}")
    elif state:
        print(f"  Failed: {state.failure_details}")

# === Example 2: Explicit / custom filters ===
# You can supply your own WorkItemFilters to have fine-grained control
# over which work items the worker receives, including version constraints.
print("\n--- Example 2: Explicit filters ---")
with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=secure_channel,
                                taskhub=taskhub_name, token_credential=credential) as w:
    w.add_orchestrator(greeting_orchestrator)
    w.add_orchestrator(farewell_orchestrator)
    w.add_activity(greet)
    w.add_activity(farewell)

    # Only process greeting-related work items, ignoring farewell tasks
    w.use_work_item_filters(worker.WorkItemFilters(
        orchestrations=[
            worker.OrchestrationWorkItemFilter(name="greeting_orchestrator"),
        ],
        activities=[
            worker.ActivityWorkItemFilter(name="greet"),
        ],
    ))
    w.start()

    c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=secure_channel,
                                   taskhub=taskhub_name, token_credential=credential)
    instance_id = c.schedule_new_orchestration(greeting_orchestrator, input="World")
    state = c.wait_for_orchestration_completion(instance_id, timeout=30)
    if state and state.runtime_status == client.OrchestrationStatus.COMPLETED:
        print(f"  Completed: {state.serialized_output}")
    elif state:
        print(f"  Failed: {state.failure_details}")

    exit()
