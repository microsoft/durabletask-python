# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Abstract base class for external payload storage providers.

This module defines the interface that payload storage backends must
implement to support externalizing large orchestration payloads. The
default (and currently only) implementation stores payloads in Azure
Blob Storage.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Optional


@dataclass
class LargePayloadStorageOptions:
    """Configuration options for large-payload externalization.

    Attributes:
        threshold_bytes: Payloads larger than this value (in bytes) will
            be externalized to the payload store.  Defaults to 900,000
            (900 KB), matching the .NET SDK default.
        max_stored_payload_bytes: Maximum payload size (in bytes) that
            can be stored externally.  Payloads exceeding this limit
            will cause an error.  Defaults to 10,485,760 (10 MB).
        enable_compression: When ``True`` (the default), payloads are
            GZip-compressed before uploading.
    """
    threshold_bytes: int = 900_000
    max_stored_payload_bytes: int = 10 * 1024 * 1024  # 10 MB
    enable_compression: bool = True


class PayloadStore(abc.ABC):
    """Abstract base class for external payload storage backends."""

    @property
    @abc.abstractmethod
    def options(self) -> LargePayloadStorageOptions:
        """Return the storage options for this payload store."""
        ...

    @abc.abstractmethod
    def upload(self, data: bytes, *, instance_id: Optional[str] = None) -> str:
        """Upload a payload and return a reference token string.

        The returned token is a compact string that can be embedded in
        gRPC messages in place of the original payload.  The format
        must be recognisable by :meth:`is_known_token`.

        Args:
            data: The raw payload bytes to store.
            instance_id: Optional orchestration instance ID for
                organizing stored blobs.

        Returns:
            A token string that can be used to retrieve the payload.
        """
        ...

    @abc.abstractmethod
    async def upload_async(self, data: bytes, *, instance_id: Optional[str] = None) -> str:
        """Async version of :meth:`upload`."""
        ...

    @abc.abstractmethod
    def download(self, token: str) -> bytes:
        """Download an externalized payload identified by *token*.

        Args:
            token: The reference token returned by a previous
                :meth:`upload` call.

        Returns:
            The original payload bytes.
        """
        ...

    @abc.abstractmethod
    async def download_async(self, token: str) -> bytes:
        """Async version of :meth:`download`."""
        ...

    @abc.abstractmethod
    def is_known_token(self, value: str) -> bool:
        """Return ``True`` if *value* looks like a token produced by this store."""
        ...
