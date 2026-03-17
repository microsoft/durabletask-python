# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Azure Blob Storage payload externalization for Durable Task.

This optional extension package provides a :class:`BlobPayloadStore`
that stores large orchestration / activity payloads in Azure Blob
Storage, keeping gRPC message sizes within safe limits.

Install the required dependency with::

    pip install durabletask[azure-blob-payloads]

Usage::

    from durabletask.extensions.azure_blob_payloads import BlobPayloadStore

    store = BlobPayloadStore(
        connection_string="DefaultEndpointsProtocol=https;...",
    )
    worker = TaskHubGrpcWorker(payload_store=store)
"""

try:
    from azure.storage.blob import BlobServiceClient  # noqa: F401
except ImportError as exc:
    raise ImportError(
        "The 'azure-storage-blob' package is required for blob payload "
        "support. Install it with:  pip install durabletask[azure-blob-payloads]"
    ) from exc

from durabletask.extensions.azure_blob_payloads.blob_payload_store import BlobPayloadStore

__all__ = ["BlobPayloadStore"]
