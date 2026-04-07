# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Configuration options for the Azure Blob payload store."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from durabletask.payload.store import LargePayloadStorageOptions


@dataclass
class BlobPayloadStoreOptions(LargePayloadStorageOptions):
    """Configuration specific to the Azure Blob payload store.

    Inherits general threshold / compression settings from
    :class:`~durabletask.payload.store.LargePayloadStorageOptions`
    and adds Azure Blob-specific fields.

    Attributes:
        container_name: Azure Blob container used to store externalized
            payloads.  Defaults to ``"durabletask-payloads"``.
        connection_string: Azure Storage connection string.  Mutually
            exclusive with *account_url*.
        account_url: Azure Storage account URL (e.g.
            ``"https://<account>.blob.core.windows.net"``).  Use
            together with *credential* for token-based auth.
        credential: A ``TokenCredential`` instance (e.g.
            ``DefaultAzureCredential``) for authenticating to the
            storage account when using *account_url*.
        api_version: Azure Storage API version override (useful for
            Azurite compatibility).
    """
    container_name: str = "durabletask-payloads"
    connection_string: Optional[str] = None
    account_url: Optional[str] = None
    credential: Any = field(default=None, repr=False)
    api_version: Optional[str] = None
