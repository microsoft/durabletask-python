# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Destination-agnostic writer protocol for the history export extension.

A :class:`HistoryWriter` is the extension point that lets the history
export workflow target any blob-style backend (Azure Blob Storage,
S3, GCS, local filesystem, SFTP, etc.).  The export activities call
:meth:`HistoryWriter.write` once per exported instance with the
serialized payload and the content-type / content-encoding metadata
appropriate for the configured :class:`ExportFormat`.

The protocol is intentionally structural — implementations do **not**
need to inherit from a base class.  Any object with a compatible
``write(...)`` method is a valid writer.  Use ``@runtime_checkable``
support to write ``isinstance(obj, HistoryWriter)`` assertions if
desired.

Example custom writer::

    from typing import Optional


    class LocalFileSystemHistoryWriter:
        def __init__(self, root_dir: str) -> None:
            self._root = root_dir

        def write(
            self,
            *,
            instance_id: str,
            blob_name: str,
            payload: bytes,
            content_type: str,
            content_encoding: Optional[str],
        ) -> None:
            import os
            path = os.path.join(self._root, blob_name)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as fp:
                fp.write(payload)

    writer = LocalFileSystemHistoryWriter("/var/exports")
    export_client = ExportHistoryClient(dt_client, writer)

The reason the protocol exposes both ``blob_name`` and ``instance_id``
is so that destinations that key by something other than the blob
name (database row IDs, message metadata, etc.) still have the
orchestration identity available.
"""

from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class HistoryWriter(Protocol):
    """Destination-agnostic interface for writing one exported blob.

    Implementations are expected to be **synchronous** and thread-safe
    if a single instance is shared across activity workers.
    """

    def write(
        self,
        *,
        instance_id: str,
        blob_name: str,
        payload: bytes,
        content_type: str,
        content_encoding: Optional[str],
    ) -> None:
        """Persist one exported blob.

        Args:
            instance_id: The orchestration instance whose history this
                payload represents.  Provided so destinations may use
                it as a key, metadata, or sharding hint.
            blob_name: Destination-relative path / key, including any
                configured destination prefix and file extension.
            payload: The serialized history bytes.  Already compressed
                if the configured format calls for it.
            content_type: The HTTP-style content type appropriate for
                the configured format (e.g. ``application/json``).
            content_encoding: ``"gzip"`` for the JSONL_GZIP format,
                ``None`` for uncompressed formats.  Destinations that
                model HTTP-style headers (such as Azure Blob Storage)
                should persist this on the blob; destinations that
                cannot represent it may ignore it.
        """
        ...


__all__ = ["HistoryWriter"]
