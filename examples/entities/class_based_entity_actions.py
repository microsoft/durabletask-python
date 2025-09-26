"""End-to-end sample that demonstrates how to configure an orchestrator
that calls an activity function in a sequence and prints the outputs."""
import os

from azure.identity import DefaultAzureCredential

from durabletask import client, task, entities
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker


class Counter(entities.DurableEntity):
    def set(self, input: int):
        self.set_state(input)

    def add(self, input: int):
        current_state = self.get_state(int, 0)
        new_state = current_state + (input or 1)
        self.set_state(new_state)
        return new_state

    def update_parent(self):
        parent_entity_id = entities.EntityInstanceId("Counter", "parentCounter")
        if self.entity_context.entity_id == parent_entity_id:
            return  # Prevent self-update
        self.signal_entity(parent_entity_id, "set", self.get_state(int, 0))

    def start_hello(self):
        self.schedule_new_orchestration("hello_orchestrator")

    def get(self):
        return self.get_state(int, 0)


def counter_orchestrator(ctx: task.OrchestrationContext, _):
    """Orchestrator function that demonstrates the behavior of the counter entity"""

    entity_id = task.EntityInstanceId("Counter", "myCounter")
    parent_entity_id = task.EntityInstanceId("Counter", "parentCounter")

    # Use Counter to demonstrate starting an orchestration from an entity
    ctx.signal_entity(entity_id, "start_hello")

    # User Counter to demonstrate signaling an entity from another entity
    # Initialize myCounter with state 0, increment it by 1, and set the state of parentCounter using
    # update_parent on myCounter. Retrieve and return the state of parentCounter (should be 1).
    ctx.signal_entity(entity_id, "set", 0)
    yield ctx.call_entity(entity_id, "add", 1)
    yield ctx.call_entity(entity_id, "update_parent")

    return (yield ctx.call_entity(parent_entity_id, "get"))


def hello_orchestrator(ctx: task.OrchestrationContext, _):
    return f"Hello world!"


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
    w.add_orchestrator(counter_orchestrator)
    w.add_orchestrator(hello_orchestrator)
    w.add_entity(Counter)
    w.start()

    # Construct the client and run the orchestrations
    c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=secure_channel,
                                   taskhub=taskhub_name, token_credential=credential)
    instance_id = c.schedule_new_orchestration(counter_orchestrator)
    state = c.wait_for_orchestration_completion(instance_id, timeout=60)
    if state and state.runtime_status == client.OrchestrationStatus.COMPLETED:
        print(f'Orchestration completed! Result: {state.serialized_output}')
    elif state:
        print(f'Orchestration failed: {state.failure_details}')
