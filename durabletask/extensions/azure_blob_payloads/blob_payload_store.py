# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Azure Blob Storage implementation of :class:`PayloadStore`."""

from __future__ import annotations

import gzip
import logging
import uuid
from typing import Optional

from azure.storage.blob import BlobServiceClient
from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient

from durabletask.extensions.azure_blob_payloads.options import BlobPayloadStoreOptions
from durabletask.payload.store import PayloadStore

logger = logging.getLogger("durabletask-blobpayloads")

# Token format matching the .NET SDK: blob:v1:<container>:<blobName>
_TOKEN_PREFIX = "blob:v1:"


class BlobPayloadStore(PayloadStore):
    """Stores and retrieves large payloads in Azure Blob Storage.

    This implementation is compatible with the .NET SDK's
    ``AzureBlobPayloadsSideCarInterceptor`` – both SDKs use the same
    token format (``blob:v1:<container>:<blobName>``) and the same
    storage layout, allowing cross-language interoperability.

    Example::

        store = BlobPayloadStore(BlobPayloadStoreOptions(
            connection_string="...",
        ))

    Args:
        options: A :class:`BlobPayloadStoreOptions` with all settings.
    """

    def __init__(self, options: BlobPayloadStoreOptions):
        if not options.connection_string and not options.account_url:
            raise ValueError(
                "Either 'connection_string' or 'account_url' (with 'credential') must be provided."
            )

        self._options = options
        self._container_name = options.container_name

        # Optional kwargs shared by both sync and async clients.
        extra_kwargs: dict = {}
        if options.api_version:
            extra_kwargs["api_version"] = options.api_version

        # Build sync client
        if options.connection_string:
            self._blob_service_client = BlobServiceClient.from_connection_string(
                options.connection_string, **extra_kwargs,
            )
        else:
            assert options.account_url is not None  # guaranteed by validation above
            self._blob_service_client = BlobServiceClient(
                account_url=options.account_url,
                credential=options.credential,
                **extra_kwargs,
            )

        # Build async client
        if options.connection_string:
            self._async_blob_service_client = AsyncBlobServiceClient.from_connection_string(
                options.connection_string, **extra_kwargs,
            )
        else:
            assert options.account_url is not None  # guaranteed by validation above
            self._async_blob_service_client = AsyncBlobServiceClient(
                account_url=options.account_url,
                credential=options.credential,
                **extra_kwargs,
            )

        self._ensure_container_created = False

    @property
    def options(self) -> BlobPayloadStoreOptions:
        return self._options

    # ------------------------------------------------------------------
    # Sync operations
    # ------------------------------------------------------------------

    def upload(self, data: bytes, *, instance_id: Optional[str] = None) -> str:
        self._ensure_container_sync()

        if self._options.enable_compression:
            data = gzip.compress(data)

        blob_name = self._make_blob_name(instance_id)
        container_client = self._blob_service_client.get_container_client(self._container_name)
        container_client.upload_blob(name=blob_name, data=data, overwrite=True)

        token = f"{_TOKEN_PREFIX}{self._container_name}:{blob_name}"
        logger.debug("Uploaded %d bytes -> %s", len(data), token)
        return token

    def download(self, token: str) -> bytes:
        container, blob_name = self._parse_token(token)
        container_client = self._blob_service_client.get_container_client(container)
        blob_data = container_client.download_blob(blob_name).readall()

        if self._options.enable_compression:
            blob_data = gzip.decompress(blob_data)

        logger.debug("Downloaded %d bytes <- %s", len(blob_data), token)
        return blob_data

    # ------------------------------------------------------------------
    # Async operations
    # ------------------------------------------------------------------

    async def upload_async(self, data: bytes, *, instance_id: Optional[str] = None) -> str:
        await self._ensure_container_async()

        if self._options.enable_compression:
            data = gzip.compress(data)

        blob_name = self._make_blob_name(instance_id)
        container_client = self._async_blob_service_client.get_container_client(self._container_name)
        await container_client.upload_blob(name=blob_name, data=data, overwrite=True)

        token = f"{_TOKEN_PREFIX}{self._container_name}:{blob_name}"
        logger.debug("Uploaded %d bytes -> %s", len(data), token)
        return token

    async def download_async(self, token: str) -> bytes:
        container, blob_name = self._parse_token(token)
        container_client = self._async_blob_service_client.get_container_client(container)
        stream = await container_client.download_blob(blob_name)
        blob_data = await stream.readall()

        if self._options.enable_compression:
            blob_data = gzip.decompress(blob_data)

        logger.debug("Downloaded %d bytes <- %s", len(blob_data), token)
        return blob_data

    # ------------------------------------------------------------------
    # Token helpers
    # ------------------------------------------------------------------

    def is_known_token(self, value: str) -> bool:
        try:
            self._parse_token(value)
            return True
        except ValueError:
            return False

    @staticmethod
    def _parse_token(token: str) -> tuple[str, str]:
        """Parse ``blob:v1:<container>:<blobName>`` into (container, blobName)."""
        if not token.startswith(_TOKEN_PREFIX):
            raise ValueError(f"Invalid blob payload token: {token!r}")
        rest = token[len(_TOKEN_PREFIX):]
        parts = rest.split(":", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(f"Invalid blob payload token: {token!r}")
        return parts[0], parts[1]

    @staticmethod
    def _make_blob_name(instance_id: Optional[str] = None) -> str:
        """Generate a blob name, optionally scoped under an instance ID folder."""
        unique = uuid.uuid4().hex
        if instance_id:
            return f"{instance_id}/{unique}"
        return unique

    # ------------------------------------------------------------------
    # Container lifecycle
    # ------------------------------------------------------------------

    def _ensure_container_sync(self) -> None:
        if self._ensure_container_created:
            return
        container_client = self._blob_service_client.get_container_client(self._container_name)
        try:
            container_client.create_container()
        except Exception:
            # Container may already exist — that is fine.
            pass
        self._ensure_container_created = True

    async def _ensure_container_async(self) -> None:
        if self._ensure_container_created:
            return
        container_client = self._async_blob_service_client.get_container_client(self._container_name)
        try:
            await container_client.create_container()
        except Exception:
            # Container may already exist — that is fine.
            pass
        self._ensure_container_created = True
