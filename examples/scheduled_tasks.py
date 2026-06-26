# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""End-to-end sample showing how to create and manage a recurring schedule.

The schedule periodically starts a target orchestration. This sample uses the
Durable Task Scheduler worker/client (compatible with the DTS emulator).
"""
import os
import time
from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from typing import Any

from azure.identity import DefaultAzureCredential

from durabletask import task
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from durabletask.scheduled import (ScheduledTaskClient, ScheduleCreationOptions,
                                   configure_scheduled_tasks)


def greet_orchestrator(ctx: task.OrchestrationContext, name: str) -> Generator[task.Task[Any], Any, Any]:
    """The target orchestration that the schedule will start on each run."""
    yield ctx.create_timer(timedelta(seconds=1))
    return f"Hello, {name}!"


# Use environment variables if provided, otherwise use default emulator values
taskhub_name = os.getenv("TASKHUB", "default")
endpoint = os.getenv("ENDPOINT", "http://localhost:8080")

print(f"Using taskhub: {taskhub_name}")
print(f"Using endpoint: {endpoint}")

secure_channel = endpoint.startswith("https://")
credential = DefaultAzureCredential() if secure_channel else None

with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=secure_channel,
                                taskhub=taskhub_name, token_credential=credential) as worker:
    worker.add_orchestrator(greet_orchestrator)
    # Register the schedule entity and operation orchestrator.
    configure_scheduled_tasks(worker)
    worker.start()

    client = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=secure_channel,
                                        taskhub=taskhub_name, token_credential=credential)
    scheduled_tasks = ScheduledTaskClient(client)

    # Create a schedule that runs the greet orchestration every 5 seconds.
    schedule = scheduled_tasks.create_schedule(ScheduleCreationOptions(
        schedule_id="greet-every-5s",
        orchestration_name=task.get_name(greet_orchestrator),
        interval=timedelta(seconds=5),
        orchestration_input="world",
        start_at=datetime.now(timezone.utc),
        start_immediately_if_late=True,
    ))

    print(f"Created schedule '{schedule.schedule_id}'.")
    print(f"Description: {scheduled_tasks.get_schedule(schedule.schedule_id)}")

    # Let it run for a bit.
    time.sleep(12)

    # Pause, then resume the schedule.
    schedule.pause()
    print("Schedule paused.")
    time.sleep(2)
    schedule.resume()
    print("Schedule resumed.")

    # List all schedules.
    print(f"All schedules: {[s.schedule_id for s in scheduled_tasks.list_schedules()]}")

    # Clean up.
    schedule.delete()
    print("Schedule deleted.")
