# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Azure Blob Storage implementation of :class:`PayloadStore`."""

from __future__ import annotations

import asyncio
import gzip
import logging
import re
import uuid
from typing import Self
from urllib.parse import SplitResult, unquote, urlsplit

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.storage.blob.aio import (
    BlobServiceClient as AsyncBlobServiceClient,
    ContainerClient as AsyncContainerClient,
)

from durabletask.extensions.azure_blob_payloads.options import BlobPayloadStoreOptions
from durabletask.payload.store import PayloadStore

logger = logging.getLogger("durabletask-blobpayloads")

_TOKEN_PREFIX_V1 = "blob:v1:"
_TOKEN_PREFIX_V2 = "blob:v2:"


class BlobPayloadStore(PayloadStore):
    """Stores and retrieves large payloads in Azure Blob Storage.

    Uploads return self-describing ``blob:v2:<fullBlobUrl>`` references
    compatible with the .NET SDK. Downloads also accept legacy
    ``blob:v1:<container>:<blobName>`` references. Both formats are
    restricted to the configured Storage endpoint and container.

    Example::

        store = BlobPayloadStore(BlobPayloadStoreOptions(
            connection_string="...",
        ))

    Args:
        options: A :class:`BlobPayloadStoreOptions` with all settings.
    """

    def __init__(self, options: BlobPayloadStoreOptions) -> None:
        if not options.connection_string and not options.account_url:
            raise ValueError(
                "Either 'connection_string' or 'account_url' (with 'credential') must be provided."
            )

        self._options: BlobPayloadStoreOptions = options
        self._container_name: str = options.container_name
        self._blob_service_client: BlobServiceClient
        self._async_blob_service_client: AsyncBlobServiceClient

        # Build sync client
        if options.connection_string:
            if options.api_version:
                self._blob_service_client = BlobServiceClient.from_connection_string(
                    options.connection_string,
                    api_version=options.api_version,
                )
            else:
                self._blob_service_client = BlobServiceClient.from_connection_string(
                    options.connection_string,
                )
        else:
            assert options.account_url is not None  # guaranteed by validation above
            if options.api_version:
                self._blob_service_client = BlobServiceClient(
                    account_url=options.account_url,
                    credential=options.credential,
                    api_version=options.api_version,
                )
            else:
                self._blob_service_client = BlobServiceClient(
                    account_url=options.account_url,
                    credential=options.credential,
                )

        # Build async client
        if options.connection_string:
            if options.api_version:
                self._async_blob_service_client = AsyncBlobServiceClient.from_connection_string(
                    options.connection_string,
                    api_version=options.api_version,
                )
            else:
                self._async_blob_service_client = AsyncBlobServiceClient.from_connection_string(
                    options.connection_string,
                )
        else:
            assert options.account_url is not None  # guaranteed by validation above
            if options.api_version:
                self._async_blob_service_client = AsyncBlobServiceClient(
                    account_url=options.account_url,
                    credential=options.credential,
                    api_version=options.api_version,
                )
            else:
                self._async_blob_service_client = AsyncBlobServiceClient(
                    account_url=options.account_url,
                    credential=options.credential,
                )

        self._ensure_container_created: bool = False

    # ------------------------------------------------------------------
    # Lifecycle / resource management
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying sync blob service client."""
        self._blob_service_client.close()

    async def close_async(self) -> None:
        """Close the underlying async blob service client."""
        await self._async_blob_service_client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close_async()

    @property
    def options(self) -> BlobPayloadStoreOptions:
        return self._options

    # ------------------------------------------------------------------
    # Sync operations
    # ------------------------------------------------------------------

    def upload(self, data: bytes, *, instance_id: str | None = None) -> str:
        blob_name = self._make_blob_name(instance_id)
        self._ensure_container_sync()

        if self._options.enable_compression:
            data = gzip.compress(data)

        container_client: ContainerClient = self._blob_service_client.get_container_client(
            self._container_name
        )
        container_client.upload_blob(name=blob_name, data=data, overwrite=True)

        token = self._encode_token(container_client.get_blob_client(blob_name).url)
        logger.debug("Uploaded %d bytes -> %s", len(data), token)
        return token

    def download(self, token: str) -> bytes:
        blob_name = self._get_download_blob_name(token)
        container_client: ContainerClient = self._blob_service_client.get_container_client(
            self._container_name
        )
        blob_data = container_client.download_blob(blob_name).readall()

        if self._options.enable_compression:
            blob_data = gzip.decompress(blob_data)

        logger.debug("Downloaded %d bytes <- %s", len(blob_data), token)
        return blob_data

    # ------------------------------------------------------------------
    # Async operations
    # ------------------------------------------------------------------

    async def upload_async(self, data: bytes, *, instance_id: str | None = None) -> str:
        blob_name = self._make_blob_name(instance_id)
        await self._ensure_container_async()

        if self._options.enable_compression:
            data = await asyncio.to_thread(gzip.compress, data)

        container_client: AsyncContainerClient = self._async_blob_service_client.get_container_client(
            self._container_name
        )
        await container_client.upload_blob(name=blob_name, data=data, overwrite=True)

        token = self._encode_token(container_client.get_blob_client(blob_name).url)
        logger.debug("Uploaded %d bytes -> %s", len(data), token)
        return token

    async def download_async(self, token: str) -> bytes:
        blob_name = self._get_download_blob_name(token)
        container_client: AsyncContainerClient = self._async_blob_service_client.get_container_client(
            self._container_name
        )
        stream = await container_client.download_blob(blob_name)
        blob_data = await stream.readall()

        if self._options.enable_compression:
            blob_data = await asyncio.to_thread(gzip.decompress, blob_data)

        logger.debug("Downloaded %d bytes <- %s", len(blob_data), token)
        return blob_data

    # ------------------------------------------------------------------
    # Token helpers
    # ------------------------------------------------------------------

    def is_known_token(self, value: str) -> bool:
        try:
            if value.startswith(_TOKEN_PREFIX_V2):
                self._parse_v2_url(value)
            else:
                self._parse_token(value)
            return True
        except ValueError:
            return False

    @staticmethod
    def _encode_token(blob_url: str) -> str:
        # References identify the blob, not the credentials used to access it.
        url = urlsplit(blob_url)
        url = url._replace(netloc=url.netloc.rsplit("@", 1)[-1], query="", fragment="")
        return f"{_TOKEN_PREFIX_V2}{url.geturl()}"

    @staticmethod
    def _parse_token(token: str) -> tuple[str, str]:
        """Parse ``blob:v1:<container>:<blobName>`` into (container, blobName)."""
        if not token.startswith(_TOKEN_PREFIX_V1):
            raise ValueError(f"Invalid blob payload token: {token!r}")
        rest = token[len(_TOKEN_PREFIX_V1):]
        parts = rest.split(":", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(f"Invalid blob payload token: {token!r}")
        return parts[0], parts[1]

    @staticmethod
    def _parse_v2_url(token: str) -> SplitResult:
        """Parse a v2 URL, decoding its path exactly once."""
        rest = token[len(_TOKEN_PREFIX_V2):]
        if any(ord(char) <= 32 or ord(char) == 127 for char in rest) or "?" in rest or "#" in rest:
            raise ValueError("Invalid blob payload URL.")
        url = urlsplit(rest)
        if (url.scheme not in ("https", "http") or not url.hostname
                or url.username is not None or url.password is not None
                or url.port == 0 or re.search(r"%(?![0-9a-fA-F]{2})", url.path)):
            raise ValueError("Invalid blob payload URL.")
        path = unquote(url.path, errors="strict")
        parts = path.split("/", 2)
        if len(parts) != 3 or not parts[1] or not parts[2]:
            raise ValueError("Invalid blob payload URL.")
        return url._replace(path=path)

    def _get_download_blob_name(self, token: str) -> str:
        if token.startswith(_TOKEN_PREFIX_V2):
            url = self._parse_v2_url(token)
            configured = urlsplit(self._blob_service_client.url)
            origin = (url.scheme, url.hostname, url.port or (443 if url.scheme == "https" else 80))
            configured_origin = (
                configured.scheme, configured.hostname,
                configured.port or (443 if configured.scheme == "https" else 80),
            )
            if origin != configured_origin:
                raise ValueError("Blob payload URL must reference the configured Storage endpoint.")
            prefix = f"{unquote(configured.path, errors='strict').rstrip('/')}/{self._container_name}/"
            if not url.path.startswith(prefix) or len(url.path) == len(prefix):
                raise ValueError("Blob payload URL must reference the configured container.")
            blob_name = url.path[len(prefix):]
        else:
            container, blob_name = self._parse_token(token)
            if container != self._container_name:
                raise ValueError("Blob payload token must reference the configured container.")
        self._validate_blob_name(blob_name)
        return blob_name

    @staticmethod
    def _validate_blob_name(blob_name: str) -> None:
        # HTTP transports can normalize dot segments out of the configured container.
        if any(part in (".", "..") for part in blob_name.split("/")):
            raise ValueError("Blob payload names must not contain '.' or '..' path segments.")

    def _make_blob_name(self, instance_id: str | None = None) -> str:
        """Generate a blob name, optionally scoped under an instance ID folder."""
        unique = uuid.uuid4().hex
        blob_name = f"{instance_id}/{unique}" if instance_id else unique
        self._validate_blob_name(blob_name)
        return blob_name

    # ------------------------------------------------------------------
    # Container lifecycle
    # ------------------------------------------------------------------

    def _ensure_container_sync(self) -> None:
        if self._ensure_container_created:
            return
        container_client: ContainerClient = self._blob_service_client.get_container_client(
            self._container_name
        )
        try:
            container_client.create_container()
        except ResourceExistsError:
            pass
        self._ensure_container_created = True

    async def _ensure_container_async(self) -> None:
        if self._ensure_container_created:
            return
        container_client: AsyncContainerClient = self._async_blob_service_client.get_container_client(
            self._container_name
        )
        try:
            await container_client.create_container()
        except ResourceExistsError:
            pass
        self._ensure_container_created = True
