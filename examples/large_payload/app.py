#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""End-to-end sample demonstrating large payload externalization.

This example shows how to configure a BlobPayloadStore so that large
orchestration inputs, activity outputs, and event data are automatically
offloaded to Azure Blob Storage and replaced with compact reference
tokens in gRPC messages.

Prerequisites:
    pip install durabletask[azure-blob-payloads] durabletask-azuremanaged azure-identity

Usage (emulator + Azurite — no Azure resources needed):
    # Start the DTS emulator (port 8080) and Azurite (port 10000)
    python app.py

Usage (Azure):
    export ENDPOINT=https://<scheduler>.durabletask.io
    export TASKHUB=<taskhub>
    export STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;..."
    python app.py
"""

import os

from azure.identity import DefaultAzureCredential

from durabletask import client, task
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from durabletask.extensions.azure_blob_payloads import BlobPayloadStore, BlobPayloadStoreOptions


# --------------- Activities ---------------

def generate_report(ctx: task.ActivityContext, num_records: int) -> str:
    """Activity that returns a large payload (simulating a report)."""
    return "RECORD|" * num_records


def summarize(ctx: task.ActivityContext, report: str) -> str:
    """Activity that summarizes a report."""
    record_count = report.count("RECORD|")
    return f"Report contains {record_count} records ({len(report)} bytes)"


# --------------- Orchestrator ---------------

def large_payload_orchestrator(ctx: task.OrchestrationContext, num_records: int):
    """Orchestrator that generates a large report and then summarizes it.

    Both the report (activity output) and the orchestration input are
    transparently externalized to blob storage when they exceed the
    configured threshold.
    """
    report = yield ctx.call_activity(generate_report, input=num_records)
    summary = yield ctx.call_activity(summarize, input=report)
    return summary


# --------------- Main ---------------

def main():
    # DTS endpoint and taskhub (defaults to the emulator)
    taskhub_name = os.getenv("TASKHUB", "default")
    endpoint = os.getenv("ENDPOINT", "http://localhost:8080")

    # Azure Storage connection string (defaults to Azurite)
    storage_conn_str = os.getenv(
        "STORAGE_CONNECTION_STRING",
        "UseDevelopmentStorage=true",
    )

    print(f"Using taskhub: {taskhub_name}")
    print(f"Using endpoint: {endpoint}")

    # Configure the blob payload store
    store = BlobPayloadStore(BlobPayloadStoreOptions(
        connection_string=storage_conn_str,
        # Use a low threshold so that we can see externalization in action
        threshold_bytes=1_024,
    ))

    secure_channel = endpoint.startswith("https://")
    credential = DefaultAzureCredential() if secure_channel else None

    with DurableTaskSchedulerWorker(
        host_address=endpoint,
        secure_channel=secure_channel,
        taskhub=taskhub_name,
        token_credential=credential,
        payload_store=store,
    ) as w:
        w.add_orchestrator(large_payload_orchestrator)
        w.add_activity(generate_report)
        w.add_activity(summarize)
        w.start()

        c = DurableTaskSchedulerClient(
            host_address=endpoint,
            secure_channel=secure_channel,
            taskhub=taskhub_name,
            token_credential=credential,
            payload_store=store,
        )

        # Schedule an orchestration with a small input (stays inline)
        print("\n--- Small payload (stays inline) ---")
        instance_id = c.schedule_new_orchestration(
            large_payload_orchestrator, input=10)
        state = c.wait_for_orchestration_completion(instance_id, timeout=60)
        if state and state.runtime_status == client.OrchestrationStatus.COMPLETED:
            print(f"Result: {state.serialized_output}")

        # Schedule an orchestration that produces a large activity output
        # (the report will be externalized to blob storage automatically)
        print("\n--- Large payload (externalized to blob storage) ---")
        instance_id = c.schedule_new_orchestration(
            large_payload_orchestrator, input=10_000)
        state = c.wait_for_orchestration_completion(instance_id, timeout=60)
        if state and state.runtime_status == client.OrchestrationStatus.COMPLETED:
            print(f"Result: {state.serialized_output}")
        elif state:
            print(f"Orchestration failed: {state.failure_details}")


if __name__ == "__main__":
    main()
