# Orchestration history export

This sample shows how to use `durabletask.extensions.history_export` to
export the event history of terminal orchestrations to Azure Blob
Storage. It uses the in-memory backend, so it only needs Azurite (the
local Azure Storage emulator) — no other services or accounts.

## Prerequisites

```bash
pip install durabletask[history-export-azure]
```

Start Azurite locally:

```bash
azurite --silent --blobPort 10000
```

## Run the sample

```bash
python app.py
```

The script:

1. Spins up an in-memory durabletask backend.
2. Schedules five small orchestrations to populate terminal history.
3. Creates an export job that scans the recent time window and writes
   each instance's history to the `history-export-sample` container as
   gzipped JSONL.
4. Polls the job through `ExportHistoryClient.wait_for_job` and prints
   the final status.

> [!TIP]
> Set `STORAGE_CONNECTION_STRING` to point at a real Azure Storage
> account instead of Azurite.
