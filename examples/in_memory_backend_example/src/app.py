#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Run the order-processing workflow against a real Durable Task Scheduler backend.

Usage (emulator — no env vars needed):
    python -m src.app

Usage (Azure):
    export ENDPOINT=https://<scheduler>.durabletask.io
    export TASKHUB=<taskhub>
    python -m src.app
"""

import os

from azure.identity import DefaultAzureCredential

from durabletask import client
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker

from src.workflows import (
    Order,
    OrderItem,
    calculate_total,
    order_with_approval,
    process_order,
    process_payment,
    send_confirmation,
    ship_item,
    validate_order,
)


def main():
    # Use environment variables if provided, otherwise default to the emulator.
    taskhub_name = os.getenv("TASKHUB", "default")
    endpoint = os.getenv("ENDPOINT", "http://localhost:8080")

    print(f"Using taskhub: {taskhub_name}")
    print(f"Using endpoint: {endpoint}")

    secure_channel = endpoint.startswith("https://")
    credential = DefaultAzureCredential() if secure_channel else None

    with DurableTaskSchedulerWorker(
        host_address=endpoint,
        secure_channel=secure_channel,
        taskhub=taskhub_name,
        token_credential=credential,
    ) as w:
        # Register all orchestrators and activities
        w.add_orchestrator(process_order)
        w.add_orchestrator(order_with_approval)
        w.add_activity(validate_order)
        w.add_activity(calculate_total)
        w.add_activity(process_payment)
        w.add_activity(send_confirmation)
        w.add_activity(ship_item)
        w.start()

        c = DurableTaskSchedulerClient(
            host_address=endpoint,
            secure_channel=secure_channel,
            taskhub=taskhub_name,
            token_credential=credential,
        )

        # Build a sample order
        order = Order(
            customer="Contoso",
            items=[
                OrderItem(name="Widget", quantity=3, unit_price=25.00),
                OrderItem(name="Gadget", quantity=1, unit_price=99.99),
            ],
        )

        # Schedule and run the orchestration
        instance_id = c.schedule_new_orchestration(process_order, input=order)
        print(f"Orchestration scheduled with ID: {instance_id}")

        state = c.wait_for_orchestration_completion(instance_id, timeout=60)
        if state and state.runtime_status == client.OrchestrationStatus.COMPLETED:
            print(f"Orchestration completed! Result: {state.serialized_output}")
        elif state:
            print(f"Orchestration failed: {state.failure_details}")
        else:
            print("Orchestration timed out.")


if __name__ == "__main__":
    main()
