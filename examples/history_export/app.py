#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""End-to-end sample for exporting orchestration history to Azure Blob Storage.

This sample uses the in-memory backend, so it can run with no external
services other than Azurite (the local Azure Storage emulator).

Prerequisites:
    pip install durabletask[history-export-azure]
    azurite --silent --blobPort 10000

Usage:
    python app.py
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone

from durabletask import client, task, worker
from durabletask.extensions.history_export import (
    ExportDestination,
    ExportFormat,
    ExportFormatKind,
    ExportHistoryClient,
    ExportJobCreationOptions,
    ExportMode,
)
from durabletask.extensions.history_export.azure_blob import (
    AzureBlobHistoryExportWriter,
    AzureBlobHistoryExportWriterOptions,
)
from durabletask.testing import create_test_backend


HOST = "localhost:50300"
CONTAINER_NAME = os.getenv("EXPORT_CONTAINER", "history-export-sample")
AZURITE_CONN_STR = os.getenv(
    "STORAGE_CONNECTION_STRING", "UseDevelopmentStorage=true"
)


# --------------- Activities & orchestrator (synthetic workload) ---------------

def square(_: task.ActivityContext, n: int) -> int:
    return n * n


def sample_orchestrator(ctx: task.OrchestrationContext, n: int):
    result = yield ctx.call_activity(square, input=n)
    return result


# --------------- Main ---------------

def main() -> None:
    print(f"Using container: {CONTAINER_NAME}")
    print(f"Using storage connection: {AZURITE_CONN_STR}")

    backend = create_test_backend(port=50300)
    try:
        writer = AzureBlobHistoryExportWriter(
            AzureBlobHistoryExportWriterOptions(
                container_name=CONTAINER_NAME,
                connection_string=AZURITE_CONN_STR,
                api_version="2024-08-04",
            )
        )

        dt_client = client.TaskHubGrpcClient(host_address=HOST)
        export_client = ExportHistoryClient(dt_client, writer)

        with worker.TaskHubGrpcWorker(host_address=HOST) as w:
            # Register the workload orchestrator and activity.
            w.add_orchestrator(sample_orchestrator)
            w.add_activity(square)

            # Register the export-job entity, activities, and orchestrator.
            export_client.register_worker(w)
            w.start()

            # Seed some terminal instances to export.
            print("\nSeeding sample orchestrations...")
            for n in range(1, 6):
                sid = dt_client.schedule_new_orchestration(sample_orchestrator, input=n)
                state = dt_client.wait_for_orchestration_completion(sid, timeout=30)
                assert state and state.runtime_status == client.OrchestrationStatus.COMPLETED
            time.sleep(0.5)

            # Create an export job for the seeded window.
            now = datetime.now(timezone.utc)
            print("\nCreating export job...")
            desc = export_client.create_job(
                ExportJobCreationOptions(
                    mode=ExportMode.BATCH,
                    completed_time_from=now - timedelta(hours=1),
                    completed_time_to=now + timedelta(hours=1),
                    destination=ExportDestination(container=CONTAINER_NAME, prefix="sample-run"),
                    format=ExportFormat(kind=ExportFormatKind.JSONL_GZIP),
                    max_instances_per_batch=10,
                )
            )
            print(f"  job_id: {desc.job_id}")
            print(f"  orchestrator_instance_id: {desc.orchestrator_instance_id}")

            final = export_client.wait_for_job(desc.job_id, timeout=120, poll_interval=0.5)
            print("\nFinal job status:")
            print(f"  status:              {final.status.value}")
            print(f"  scanned_instances:   {final.scanned_instances}")
            print(f"  exported_instances:  {final.exported_instances}")
            print(f"  failed_instances:    {final.failed_instances}")
            if final.last_error:
                print(f"  last_error:          {final.last_error}")

            writer.close()
    finally:
        backend.stop()
        backend.reset()


if __name__ == "__main__":
    main()
