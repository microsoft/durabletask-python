# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Azure Blob Storage implementation of :class:`PayloadStore`."""

from __future__ import annotations

import asyncio
import gzip
import logging
import re
import uuid
from ipaddress import ip_address
from typing import Self
from urllib.parse import SplitResult, quote, unquote, urlsplit

from azure.core.credentials import TokenCredential
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient, ContentSettings
from azure.storage.blob.aio import (
    BlobClient as AsyncBlobClient,
    BlobServiceClient as AsyncBlobServiceClient,
    ContainerClient as AsyncContainerClient,
)

from durabletask.extensions.azure_blob_payloads.options import BlobPayloadStoreOptions
from durabletask.internal.exceptions import PayloadValidationError
from durabletask.payload.store import PayloadStore

logger = logging.getLogger("durabletask-blobpayloads")

_TOKEN_PREFIX_V1 = "blob:v1:"
_TOKEN_PREFIX_V2 = "blob:v2:"
_AccountEndpoint = tuple[str, str, int, str]


class BlobPayloadStore(PayloadStore):
    """Stores and retrieves large payloads in Azure Blob Storage.

    Uploads return self-describing ``blob:v2:<fullBlobUrl>`` references
    compatible with the .NET SDK. Downloads also accept legacy
    ``blob:v1:<container>:<blobName>`` references. Both formats are
    restricted to the configured Storage endpoint and container by default.
    Explicit options can allow account-wide reads and additional account
    endpoints. Recognizing a reference does not authorize its destination.

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

        if type(options.allow_all_containers_in_configured_account) is not bool:
            raise ValueError("'allow_all_containers_in_configured_account' must be a bool.")
        if type(options.additional_account_urls) is not list:
            raise ValueError("'additional_account_urls' must be a list of account URLs.")
        additional_urls = [self._parse_url(url) for url in options.additional_account_urls]
        for url in additional_urls:
            self._validate_blob_name(url.path)
            if url.scheme != "https":
                raise ValueError("Additional account endpoints require HTTPS with a TokenCredential.")
        if additional_urls and (options.connection_string or not self._is_token_credential(options.credential)):
            raise ValueError(
                "Additional account endpoints require 'account_url' and a TokenCredential; "
                "connection strings, account keys, SAS, and anonymous access are not supported."
            )
        self._validate_container_name(options.container_name)
        self._options: BlobPayloadStoreOptions = options
        self._container_name: str = options.container_name
        self._allow_all_containers_in_configured_account = options.allow_all_containers_in_configured_account
        self._blob_service_client: BlobServiceClient
        self._async_blob_service_client: AsyncBlobServiceClient

        # Build sync client
        if options.connection_string:
            if options.api_version:
                self._blob_service_client = BlobServiceClient.from_connection_string(
                    options.connection_string,
                    api_version=options.api_version,
                    permit_redirects=False,
                )
            else:
                self._blob_service_client = BlobServiceClient.from_connection_string(
                    options.connection_string,
                    permit_redirects=False,
                )
        else:
            assert options.account_url is not None  # guaranteed by validation above
            if options.api_version:
                self._blob_service_client = BlobServiceClient(
                    account_url=options.account_url,
                    credential=options.credential,
                    api_version=options.api_version,
                    permit_redirects=False,
                )
            else:
                self._blob_service_client = BlobServiceClient(
                    account_url=options.account_url,
                    credential=options.credential,
                    permit_redirects=False,
                )

        self._configured_endpoint = self._client_endpoint(self._blob_service_client.url)
        self._validate_account_service(self._blob_service_client, self._configured_endpoint)
        configured_url = options.account_url
        if options.connection_string:
            configured_url = None
            for setting in options.connection_string.split(";"):
                name, _, value = setting.partition("=")
                if name.lower() == "blobendpoint":
                    configured_url = value
        if configured_url and self._client_endpoint(configured_url) != self._configured_endpoint:
            self._blob_service_client.close()
            raise ValueError("The configured account endpoint is not preserved by the Azure Blob client.")

        # Build async client
        if options.connection_string:
            if options.api_version:
                self._async_blob_service_client = AsyncBlobServiceClient.from_connection_string(
                    options.connection_string,
                    api_version=options.api_version,
                    permit_redirects=False,
                )
            else:
                self._async_blob_service_client = AsyncBlobServiceClient.from_connection_string(
                    options.connection_string,
                    permit_redirects=False,
                )
        else:
            assert options.account_url is not None  # guaranteed by validation above
            if options.api_version:
                self._async_blob_service_client = AsyncBlobServiceClient(
                    account_url=options.account_url,
                    credential=options.credential,
                    api_version=options.api_version,
                    permit_redirects=False,
                )
            else:
                self._async_blob_service_client = AsyncBlobServiceClient(
                    account_url=options.account_url,
                    credential=options.credential,
                    permit_redirects=False,
                )

        self._validate_account_service(self._async_blob_service_client, self._configured_endpoint)
        self._blob_service_clients = {self._configured_endpoint: self._blob_service_client}
        self._async_blob_service_clients = {self._configured_endpoint: self._async_blob_service_client}
        for url in additional_urls:
            endpoint = self._account_endpoint(url)
            if endpoint == self._configured_endpoint:
                raise ValueError(
                    "The configured account must not appear in 'additional_account_urls'; "
                    "use 'allow_all_containers_in_configured_account' instead."
                )
            if endpoint in self._blob_service_clients:
                continue
            account_url = url._replace(path=quote(url.path.rstrip("/"), safe="/")).geturl()
            if options.api_version:
                service = BlobServiceClient(
                    account_url=account_url, credential=options.credential,
                    api_version=options.api_version, permit_redirects=False)
            else:
                service = BlobServiceClient(
                    account_url=account_url, credential=options.credential, permit_redirects=False)
            if (self._client_endpoint(service.url) != endpoint
                    or endpoint[3] and service.account_name != endpoint[3][1:]):
                service.close()
                raise ValueError("An additional account endpoint is not preserved by the Azure Blob client.")
            self._blob_service_clients[endpoint] = service
            if options.api_version:
                self._async_blob_service_clients[endpoint] = AsyncBlobServiceClient(
                    account_url=account_url, credential=options.credential,
                    api_version=options.api_version, permit_redirects=False)
            else:
                self._async_blob_service_clients[endpoint] = AsyncBlobServiceClient(
                    account_url=account_url, credential=options.credential, permit_redirects=False)
            self._validate_account_service(self._async_blob_service_clients[endpoint], endpoint)

        self._ensure_container_created: bool = False

    # ------------------------------------------------------------------
    # Lifecycle / resource management
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying sync blob service clients."""
        for client in self._blob_service_clients.values():
            client.close()

    async def close_async(self) -> None:
        """Close the underlying async blob service clients."""
        for client in self._async_blob_service_clients.values():
            await client.close()

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
        container_client: ContainerClient = self._blob_service_client.get_container_client(
            self._container_name
        )
        blob = container_client.get_blob_client(blob_name)
        self._validate_blob_client(blob, self._configured_endpoint, self._container_name, blob_name)
        self._ensure_container_sync()

        content_settings = None
        if self._options.enable_compression:
            data = gzip.compress(data)
            content_settings = ContentSettings(content_encoding="gzip")

        blob.upload_blob(
            data=data, overwrite=True, content_settings=content_settings,
        )

        token = self._encode_token(blob.url)
        logger.debug("Uploaded %d bytes -> %s", len(data), token)
        return token

    def download(self, token: str) -> bytes:
        endpoint, container, blob_name = self._get_download_destination(token)
        container_client: ContainerClient = self._blob_service_clients[endpoint].get_container_client(
            container
        )
        blob = container_client.get_blob_client(blob_name)
        self._validate_blob_client(blob, endpoint, container, blob_name)
        stream = blob.download_blob(decompress=False)
        blob_data = stream.readall()

        encoding = stream.properties.content_settings.content_encoding
        if self._options.enable_compression or encoding is not None and encoding.lower() == "gzip":
            blob_data = gzip.decompress(blob_data)

        logger.debug("Downloaded %d bytes <- %s", len(blob_data), self._encode_token(blob.url))
        return blob_data

    # ------------------------------------------------------------------
    # Async operations
    # ------------------------------------------------------------------

    async def upload_async(self, data: bytes, *, instance_id: str | None = None) -> str:
        blob_name = self._make_blob_name(instance_id)
        container_client: AsyncContainerClient = self._async_blob_service_client.get_container_client(
            self._container_name
        )
        blob = container_client.get_blob_client(blob_name)
        self._validate_blob_client(blob, self._configured_endpoint, self._container_name, blob_name)
        await self._ensure_container_async()

        content_settings = None
        if self._options.enable_compression:
            data = await asyncio.to_thread(gzip.compress, data)
            content_settings = ContentSettings(content_encoding="gzip")

        await blob.upload_blob(
            data=data, overwrite=True, content_settings=content_settings,
        )

        token = self._encode_token(blob.url)
        logger.debug("Uploaded %d bytes -> %s", len(data), token)
        return token

    async def download_async(self, token: str) -> bytes:
        endpoint, container, blob_name = self._get_download_destination(token)
        container_client: AsyncContainerClient = self._async_blob_service_clients[endpoint].get_container_client(
            container
        )
        blob = container_client.get_blob_client(blob_name)
        self._validate_blob_client(blob, endpoint, container, blob_name)
        stream = await blob.download_blob(decompress=False)
        blob_data = await stream.readall()

        encoding = stream.properties.content_settings.content_encoding
        if self._options.enable_compression or encoding is not None and encoding.lower() == "gzip":
            blob_data = await asyncio.to_thread(gzip.decompress, blob_data)

        logger.debug("Downloaded %d bytes <- %s", len(blob_data), self._encode_token(blob.url))
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
        url = BlobPayloadStore._parse_url(BlobPayloadStore._v2_location(token))
        parts = url.path.split("/", 2)
        if len(parts) != 3 or not parts[1] or not parts[2]:
            raise ValueError("Invalid blob payload URL.")
        return url

    @staticmethod
    def _v2_location(token: str) -> str:
        # Older references can contain SAS or selectors; only their location is used.
        return token[len(_TOKEN_PREFIX_V2):].partition("#")[0].partition("?")[0]

    @staticmethod
    def _parse_url(value: object) -> SplitResult:
        if (not isinstance(value, str) or any(ord(char) <= 32 or ord(char) == 127 for char in value)
                or "?" in value or "#" in value):
            raise ValueError("Invalid blob payload URL.")
        url = urlsplit(value)
        if (url.scheme not in ("https", "http") or not url.hostname
                or url.username is not None or url.password is not None
                or "\\" in url.netloc or "%" in url.netloc or "*" in url.netloc
                or url.port == 0 or re.search(r"%(?![0-9a-fA-F]{2})", url.path)):
            raise ValueError("Invalid blob payload URL.")
        return url._replace(path=unquote(url.path, errors="strict"))

    @staticmethod
    def _is_token_credential(credential: object) -> bool:
        return isinstance(credential, TokenCredential)

    @classmethod
    def _client_endpoint(cls, value: str) -> _AccountEndpoint:
        url = cls._parse_url(urlsplit(value)._replace(query="", fragment="").geturl())
        cls._validate_blob_name(url.path)
        path = url.path.rstrip("/")
        if path and (not cls._is_path_style_host(url.hostname or "")
                     or not re.fullmatch(r"/[A-Za-z0-9_-]+", path)):
            raise ValueError("Account endpoint paths must identify one SDK-recognized path-style account.")
        return cls._account_endpoint(url)

    @staticmethod
    def _is_path_style_host(hostname: str) -> bool:
        try:
            ip_address(hostname)
            return True
        except ValueError:
            return hostname == "localhost"

    @staticmethod
    def _account_endpoint(url: SplitResult) -> _AccountEndpoint:
        assert url.hostname is not None  # validated by _parse_url
        if not url.path.rstrip("/") and BlobPayloadStore._is_path_style_host(url.hostname):
            raise ValueError("Path-style account endpoints must include an account path.")
        return (url.scheme, url.hostname, url.port or (443 if url.scheme == "https" else 80),
                url.path.rstrip("/"))

    def _get_download_destination(self, token: str) -> tuple[_AccountEndpoint, str, str]:
        if token.startswith(_TOKEN_PREFIX_V2):
            url = self._parse_v2_url(token)
            self._validate_blob_name(url.path)
            origin = self._account_endpoint(url)[:3]
            # Match the entire account path before interpreting the container and blob.
            for endpoint in sorted(self._blob_service_clients, key=lambda item: len(item[3]), reverse=True):
                prefix = endpoint[3] + "/"
                if endpoint[:3] == origin and url.path.startswith(prefix):
                    container, sep, blob_name = url.path[len(prefix):].partition("/")
                    if not sep or not blob_name:
                        raise ValueError("Blob payload URL must name a container and blob.")
                    break
            else:
                raise ValueError(
                    "Blob payload URL must reference the configured Storage endpoint "
                    "or an explicitly allowed additional account endpoint."
                )
            structural_parts = urlsplit(self._v2_location(token)).path.split("/")[1:3 if endpoint[3] else 2]
            if any(re.search(r"%2f", part, re.IGNORECASE) for part in structural_parts):
                raise ValueError("Encoded separators must not change the payload account or container.")
        else:
            container, blob_name = self._parse_token(token)
            endpoint = self._configured_endpoint
        if (endpoint == self._configured_endpoint and not self._allow_all_containers_in_configured_account
                and container != self._container_name):
            raise ValueError("Blob payload token must reference the configured container.")
        self._validate_container_name(container)
        self._validate_blob_name(blob_name)
        return endpoint, container, blob_name

    @classmethod
    def _validate_account_service(
        cls, service: BlobServiceClient | AsyncBlobServiceClient, endpoint: _AccountEndpoint,
    ) -> None:
        if (cls._client_endpoint(service.url) != endpoint
                or endpoint[3] and service.account_name != endpoint[3][1:]):
            raise ValueError("The account endpoint is not preserved by the Azure Blob client.")

    @classmethod
    def _validate_destination_url(cls, value: str, endpoint: _AccountEndpoint, path: str) -> None:
        url = cls._parse_url(urlsplit(value)._replace(query="", fragment="").geturl())
        if cls._account_endpoint(url)[:3] != endpoint[:3] or url.path != path:
            raise ValueError("The Azure Blob client destination does not match the selected payload location.")

    @classmethod
    def _validate_blob_client(
        cls, blob: BlobClient | AsyncBlobClient, endpoint: _AccountEndpoint, container: str, blob_name: str,
    ) -> None:
        if blob.container_name != container or blob.blob_name != blob_name:
            raise ValueError("The Azure Blob client destination does not match the selected payload location.")
        cls._validate_destination_url(blob.url, endpoint, f"{endpoint[3]}/{container}/{blob_name}")

    def _validate_container_client(self, container: ContainerClient | AsyncContainerClient) -> None:
        if container.container_name != self._container_name:
            raise ValueError("The Azure Blob client container does not match the configured payload container.")
        self._validate_destination_url(
            container.url, self._configured_endpoint, f"{self._configured_endpoint[3]}/{self._container_name}")

    @staticmethod
    def _validate_container_name(container: str) -> None:
        if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\$(?:root|web|logs)", container):
            raise ValueError("Invalid blob payload container name.")

    @staticmethod
    def _validate_blob_name(blob_name: str) -> None:
        # HTTP transports can normalize dot segments out of the configured container.
        if any(part in (".", "..") for part in blob_name.split("/")):
            raise PayloadValidationError("Blob payload names must not contain '.' or '..' path segments.")

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
        self._validate_container_client(container_client)
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
        self._validate_container_client(container_client)
        try:
            await container_client.create_container()
        except ResourceExistsError:
            pass
        self._ensure_container_created = True
