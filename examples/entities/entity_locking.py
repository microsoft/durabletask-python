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
        new_state = current_state + (1 if input is None else input)
        self.set_state(new_state)
        return new_state

    def get(self):
        return self.get_state(int, 0)


def counter_orchestrator(ctx: task.OrchestrationContext, _):
    """Orchestrator function that demonstrates the behavior of the counter entity"""

    entity_id = entities.EntityInstanceId("Counter", "myCounter")

    # Initialize the entity with state 0, increment the counter by 1, and get the entity state using
    # entity locking to ensure no other orchestrator can modify the entity state between the calls to call_entity
    with (yield ctx.lock_entities([entity_id])):
        yield ctx.call_entity(entity_id, "set", 0)
        yield ctx.call_entity(entity_id, "add", 1)
        result = yield ctx.call_entity(entity_id, "get")
    # Return the entity's current value (will be 1)
    return result


# Use environment variables if provided, otherwise use default emulator values
taskhub_name = os.getenv("TASKHUB", "default")
endpoint = os.getenv("ENDPOINT", "http://localhost:8080")

print(f"Using taskhub: {taskhub_name}")
print(f"Using endpoint: {endpoint}")

# Set credential to None for emulator, or DefaultAzureCredential for Azure
secure_channel = endpoint.startswith("https://")
credential = DefaultAzureCredential() if secure_channel else None
with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=secure_channel,
                                taskhub=taskhub_name, token_credential=credential) as w:
    w.add_orchestrator(counter_orchestrator)
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
