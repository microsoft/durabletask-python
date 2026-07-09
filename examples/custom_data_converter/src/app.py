#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Run the pydantic-backed workflow against a Durable Task Scheduler backend.

The only thing that differs from an ordinary app is the ``data_converter=``
argument passed to **both** the worker and the client. Pass the *same*
converter instance (or an equivalent one) to both sides so payloads serialized
by one are reconstructed by the other.

Usage (emulator -- no env vars needed):
    python -m src.app

Usage (Azure):
    export ENDPOINT=https://<scheduler>.durabletask.io
    export TASKHUB=<taskhub>
    python -m src.app

For a self-contained proof that needs no backend at all, run the tests instead
(see ``test/test_custom_converter.py``).
"""

import os

from azure.identity import DefaultAzureCredential

from durabletask import client
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker

from src.converter import PydanticDataConverter
from src.workflows import Receipt, calculate_total, charge_payment, process_order, sample_order


def main() -> None:
    taskhub_name = os.getenv("TASKHUB", "default")
    endpoint = os.getenv("ENDPOINT", "http://localhost:8080")

    print(f"Using taskhub: {taskhub_name}")
    print(f"Using endpoint: {endpoint}")

    secure_channel = endpoint.startswith("https://")
    credential = DefaultAzureCredential() if secure_channel else None

    # The single line that wires in the third-party converter. The same
    # converter is handed to the worker and the client below.
    converter = PydanticDataConverter()

    with DurableTaskSchedulerWorker(
        host_address=endpoint,
        secure_channel=secure_channel,
        taskhub=taskhub_name,
        token_credential=credential,
        data_converter=converter,
    ) as w:
        w.add_orchestrator(process_order)
        w.add_activity(calculate_total)
        w.add_activity(charge_payment)
        w.start()

        c = DurableTaskSchedulerClient(
            host_address=endpoint,
            secure_channel=secure_channel,
            taskhub=taskhub_name,
            token_credential=credential,
            data_converter=converter,
        )

        # ``input`` is a pydantic ``Order``; the converter serializes it.
        instance_id = c.schedule_new_orchestration(process_order, input=sample_order())
        print(f"Scheduled orchestration: {instance_id}")

        state = c.wait_for_orchestration_completion(instance_id, timeout=60)
        if state and state.runtime_status == client.OrchestrationStatus.COMPLETED:
            # ``get_output(Receipt)`` reconstructs the typed, validated result.
            receipt = state.get_output(Receipt)
            assert receipt is not None
            print("Orchestration completed. Typed receipt:")
            print(f"  customer        = {receipt.customer}")
            print(f"  total           = {receipt.total}")
            print(f"  item_count      = {receipt.item_count}")
            print(f"  confirmation_id = {receipt.confirmation_id}")
        elif state:
            print(f"Orchestration failed: {state.failure_details}")
        else:
            print("Orchestration timed out.")


if __name__ == "__main__":
    main()
