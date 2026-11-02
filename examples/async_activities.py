# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""End-to-end sample that demonstrates how to write activities as `async def`
functions and fan them out from an orchestrator."""
import asyncio
import os
import random
from collections.abc import Generator
from typing import Any

from azure.identity import DefaultAzureCredential

from durabletask import client, task
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker


async def fetch_temperature(ctx: task.ActivityContext, city: str) -> float:
    """Async activity function that simulates a call to a weather API.

    The worker awaits async activities on its event loop, so many of them can
    wait on I/O at the same time without each holding a worker thread."""
    await asyncio.sleep(random.uniform(0.5, 1.5))
    return round(random.uniform(-5, 30), 1)


def forecast(ctx: task.OrchestrationContext, cities: list[str]) -> Generator[task.Task[Any], Any, dict[str, float]]:
    """Orchestrator function that fetches the temperature of each city in parallel"""
    tasks: list[task.Task[float]] = [ctx.call_activity(fetch_temperature, input=city) for city in cities]
    temperatures: list[float] = yield task.when_all(tasks)
    return dict(zip(cities, temperatures))


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
    w.add_orchestrator(forecast)
    w.add_activity(fetch_temperature)
    w.start()

    # Construct the client and run the orchestration
    c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=secure_channel,
                                   taskhub=taskhub_name, token_credential=credential)
    instance_id = c.schedule_new_orchestration(forecast, input=["Tokyo", "Seattle", "London", "Dakar"])
    state = c.wait_for_orchestration_completion(instance_id, timeout=60)
    if state and state.runtime_status == client.OrchestrationStatus.COMPLETED:
        print(f'Orchestration completed! Result: {state.serialized_output}')
    elif state:
        print(f'Orchestration failed: {state.failure_details}')
