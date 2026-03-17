# Large Payload Externalization Example

This example demonstrates how to use the large payload externalization
feature to automatically offload oversized orchestration payloads to
Azure Blob Storage.

## Overview

When orchestration inputs, activity outputs, or event data exceed a
configurable size threshold, the SDK can automatically:

1. Compress the payload with GZip
1. Upload it to Azure Blob Storage
1. Replace the payload in the gRPC message with a compact reference
   token

On the receiving side, the SDK detects these tokens and transparently
downloads and decompresses the original data. No changes are needed in
your orchestrator or activity code.

## Prerequisites

- Python 3.10+
- [Docker](https://www.docker.com/) (for the DTS emulator)
- [Azurite](https://learn.microsoft.com/azure/storage/common/storage-use-azurite)
  or an Azure Storage account

## Getting Started

1. Start the DTS emulator:

   ```bash
   docker run --name dtsemulator -d -p 8080:8080 mcr.microsoft.com/dts/dts-emulator:latest
   ```

1. Start Azurite (blob service only):

   ```bash
   azurite-blob --location /tmp/azurite --blobPort 10000
   ```

   Or use the Azurite Docker image:

   ```bash
   docker run -d -p 10000:10000 \
     mcr.microsoft.com/azure-storage/azurite \
     azurite-blob --blobHost 0.0.0.0
   ```

1. Create and activate a virtual environment:

   Bash:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

   PowerShell:

   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

1. Install dependencies (from the repository root):

   ```bash
   pip install -e ".[azure-blob-payloads]" -e ./durabletask-azuremanaged
   ```

## Running the Example

```bash
python app.py
```

The example schedules two orchestrations:

- **Small payload** — The input and output stay inline in the gRPC
  messages (below the 1 KB threshold configured in the example).
- **Large payload** — The activity output (~70 KB) exceeds the
  threshold and is automatically externalized to blob storage and
  retrieved transparently.

## Using Azure Storage Instead of Azurite

Set the `STORAGE_CONNECTION_STRING` environment variable to your Azure
Storage connection string:

Bash:

```bash
export STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;..."
```

PowerShell:

```powershell
$env:STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;..."
```

## Configuration Options

The `BlobPayloadStore` constructor supports the following settings:

| Option | Default | Description |
|---|---|---|
| `threshold_bytes` | 900,000 (900 KB) | Payloads larger than this are externalized |
| `max_stored_payload_bytes` | 10,485,760 (10 MB) | Maximum externalized payload size |
| `enable_compression` | `True` | GZip-compress before uploading |
| `container_name` | `"durabletask-payloads"` | Blob container name |
| `connection_string` | `None` | Storage connection string |
| `account_url` | `None` | Storage account URL (with `credential`) |
| `credential` | `None` | `TokenCredential` for token-based auth |

For more details, see the
[feature documentation](../../docs/features.md#large-payload-externalization).
