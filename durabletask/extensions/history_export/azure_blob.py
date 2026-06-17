# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Azure Blob Storage destination for history exports.

This optional module implements the
:class:`~durabletask.extensions.history_export.writer.HistoryWriter`
protocol on top of ``azure-storage-blob``.

Install the dependency with::

    pip install durabletask[history-export-azure]

The writer is synchronous, matching the synchronous activity execution
model used by the rest of the extension.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

try:
    from azure.core.exceptions import ResourceExistsError
    from azure.storage.blob import BlobServiceClient, ContentSettings
except ImportError as exc:  # pragma: no cover - import-time guard
    raise ImportError(
        "The 'azure-storage-blob' package is required for the Azure Blob "
        "history-export writer. Install it with: "
        "pip install durabletask[history-export-azure]"
    ) from exc


@dataclass
class AzureBlobHistoryExportWriterOptions:
    """Configuration for :class:`AzureBlobHistoryExportWriter`.

    Provide either *connection_string*, or both *account_url* and
    *credential*.

    Attributes:
        container_name: Azure Blob container that exports are written
            to.  The container is created on first use if it does not
            already exist.
        connection_string: Azure Storage connection string.  Mutually
            exclusive with *account_url*.
        account_url: Azure Storage account URL
            (e.g. ``https://<account>.blob.core.windows.net``).  Use
            together with *credential* for token-based auth.
        credential: A ``TokenCredential`` instance (e.g.
            ``DefaultAzureCredential``).
        api_version: Optional Azure Storage API version override
            (useful for Azurite compatibility).
        create_container_if_not_exists: When ``True`` (the default),
            ensure the container exists on the first write.
        overwrite: When ``True`` (the default), each blob upload
            replaces any existing blob of the same name.  Set to
            ``False`` for compliance setups that require
            write-once / immutable exports; in that mode the writer
            raises if a blob already exists at the target path.
    """

    container_name: str
    connection_string: str | None = None
    account_url: str | None = None
    credential: Any = field(default=None, repr=False)
    api_version: str | None = None
    create_container_if_not_exists: bool = True
    overwrite: bool = True

    def __post_init__(self) -> None:
        if not self.container_name:
            raise ValueError("container_name is required")
        if self.connection_string and self.account_url:
            raise ValueError(
                "'connection_string' and 'account_url' are mutually exclusive"
            )
        if not self.connection_string and not self.account_url:
            raise ValueError(
                "Either 'connection_string' or 'account_url' (with 'credential') "
                "must be provided"
            )
        if self.account_url and self.credential is None:
            raise ValueError(
                "'credential' is required when 'account_url' is provided"
            )


class AzureBlobHistoryExportWriter:
    """Writes exported history blobs to Azure Blob Storage."""

    def __init__(self, options: AzureBlobHistoryExportWriterOptions) -> None:
        self._options = options
        extra: dict[str, Any] = {}
        if options.api_version:
            extra["api_version"] = options.api_version

        if options.connection_string:
            self._service = BlobServiceClient.from_connection_string(
                options.connection_string, **extra
            )
        else:
            assert options.account_url is not None
            self._service = BlobServiceClient(
                account_url=options.account_url,
                credential=options.credential,
                **extra,
            )

        self._container_ready = False

    # ------------------------------------------------------------------
    # Context-manager / cleanup helpers
    # ------------------------------------------------------------------

    def close(self) -> None:
        self._service.close()

    def __enter__(self) -> "AzureBlobHistoryExportWriter":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # HistoryWriter protocol
    # ------------------------------------------------------------------

    def write(
        self,
        *,
        instance_id: str,
        container: str,
        blob_name: str,
        payload: bytes,
        content_type: str,
        content_encoding: str | None,
        metadata: Mapping[str, str] | None = None,
    ) -> None:
        del instance_id  # included by the protocol but not needed here
        # This writer pins to the container configured at construction
        # time and ignores the per-call ``container`` argument; the
        # configured value is authoritative for any given writer
        # instance.  Run a separate writer per destination container
        # if you need per-job routing.
        del container
        self._ensure_container()
        container_client = self._service.get_container_client(
            self._options.container_name
        )
        # Only set Content-Encoding if the format actually compresses
        # the payload; an empty header value would be persisted on
        # the blob and confuse downstream clients.
        content_settings = (
            ContentSettings(
                content_type=content_type,
                content_encoding=content_encoding,
            )
            if content_encoding
            else ContentSettings(content_type=content_type)
        )
        # Azure Blob Storage requires the metadata dict to be a plain
        # ``dict[str, str]`` (the SDK does its own validation).  Copy
        # whatever the activity passed into the shape the underlying
        # SDK expects, and pass ``None`` through unchanged so blobs
        # written via :meth:`write` without metadata behave exactly
        # the same as they did before this kwarg existed.
        blob_metadata = dict(metadata) if metadata else None
        container_client.upload_blob(
            name=blob_name,
            data=payload,
            overwrite=self._options.overwrite,
            content_settings=content_settings,
            metadata=blob_metadata,
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _ensure_container(self) -> None:
        if self._container_ready or not self._options.create_container_if_not_exists:
            self._container_ready = True
            return
        try:
            # The azure-storage-blob stubs leave create_container's return
            # type partially unknown; we don't use it, so it's safe to
            # suppress the strict-mode warning.
            self._service.create_container(  # pyright: ignore[reportUnknownMemberType]
                self._options.container_name,
            )
        except ResourceExistsError:
            pass
        self._container_ready = True
