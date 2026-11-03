# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Configuration options for the Azure Blob payload store."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from durabletask.payload.store import LargePayloadStorageOptions


@dataclass
class BlobPayloadStoreOptions(LargePayloadStorageOptions):
    """Configuration specific to the Azure Blob payload store.

    Inherits general threshold / compression settings from
    :class:`~durabletask.payload.store.LargePayloadStorageOptions`
    and adds Azure Blob-specific fields.

    Attributes:
        container_name: Azure Blob container used to store externalized
            payloads. Downloads default to this container only.
            Defaults to ``"durabletask-payloads"``.
        connection_string: Azure Storage connection string.  Mutually
            exclusive with *account_url*.
        account_url: Azure Storage account URL (e.g.
            ``"https://<account>.blob.core.windows.net"``).  Use
            together with *credential* for token-based auth.
        credential: Storage authentication when using *account_url*:
            a ``TokenCredential`` (e.g. ``DefaultAzureCredential``),
            account key, or SAS credential. ``None`` uses a SAS embedded
            in *account_url*, or anonymous access if there is no SAS.
        api_version: Azure Storage API version override (useful for
            Azurite compatibility).
        allow_all_containers_in_configured_account: Allow downloads from all containers in
            the configured account endpoint. Defaults to ``False``.
            Does not allow other account endpoints or change uploads.
        additional_account_urls: Additional account endpoints whose
            containers may be read. Defaults to an empty list. Requires
            *account_url* and a ``TokenCredential`` in *credential*; connection
            strings, account keys, SAS, and anonymous access are not supported
            for additional accounts. Endpoints must use HTTPS and must not
            include credentials, query parameters, fragments, or container/blob
            paths. This list does not broaden primary-account container access.

    Access scopes are captured when the store is constructed. Create a new
    store to change them. Azure Storage permissions still apply to every read.
    """
    container_name: str = "durabletask-payloads"
    connection_string: str | None = None
    account_url: str | None = None
    credential: Any = field(default=None, repr=False)
    api_version: str | None = None
    allow_all_containers_in_configured_account: bool = False
    additional_account_urls: list[str] = field(default_factory=list[str])
