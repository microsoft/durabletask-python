# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Helpers for externalizing and de-externalizing large payloads in protobuf messages.

These functions walk protobuf messages recursively, finding ``StringValue``
fields whose content exceeds a configured threshold (externalize) or
matches a known payload-store token (de-externalize).  The actual upload
/ download is delegated to a :class:`PayloadStore` instance.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from google.protobuf import message as proto_message
from google.protobuf import wrappers_pb2

from durabletask.internal.exceptions import PayloadSizeExceededError

if TYPE_CHECKING:
    from durabletask.payload.store import PayloadStore

logger = logging.getLogger("durabletask-payloads")


# ------------------------------------------------------------------
# Synchronous helpers
# ------------------------------------------------------------------


def externalize_payloads(
    msg: proto_message.Message,
    store: PayloadStore,
    *,
    instance_id: str | None = None,
) -> None:
    """Walk *msg* in-place, uploading large ``StringValue`` fields to *store*.

    Any ``StringValue`` whose UTF-8 byte length exceeds
    ``store.options.threshold_bytes`` is uploaded and its value replaced
    with the token returned by the store. Empty values and known tokens
    are skipped.

    All eligible payloads are snapshotted and checked against
    ``store.options.max_stored_payload_bytes`` before any upload or mutation.
    A size violation therefore raises ``ValueError`` before any hypothetical
    earlier upload failure. Upload failures can still leave earlier uploads
    and token replacements in place; this is not a storage transaction.
    """
    payloads = _collect_payloads_to_externalize(msg, store)
    for field_name, sv, payload_bytes in payloads:
        token = store.upload(payload_bytes, instance_id=instance_id)
        sv.value = token
        logger.debug(
            "Externalized %d-byte payload in field '%s' -> %s",
            len(payload_bytes), field_name, token,
        )


def deexternalize_payloads(
    msg: proto_message.Message,
    store: PayloadStore,
) -> None:
    """Walk *msg* in-place, downloading any ``StringValue`` fields that
    contain a known payload-store token and replacing them with the
    original content."""
    _walk_and_deexternalize(msg, store)


# ------------------------------------------------------------------
# Async helpers
# ------------------------------------------------------------------


async def externalize_payloads_async(
    msg: proto_message.Message,
    store: PayloadStore,
    *,
    instance_id: str | None = None,
) -> None:
    """Async version of :func:`externalize_payloads`, with the same preflight.

    Uploads remain sequential; upload errors and cancellation propagate without
    rolling back earlier uploads or token replacements.
    """
    payloads = _collect_payloads_to_externalize(msg, store)
    for field_name, sv, payload_bytes in payloads:
        token = await store.upload_async(payload_bytes, instance_id=instance_id)
        sv.value = token
        logger.debug(
            "Externalized %d-byte payload in field '%s' -> %s",
            len(payload_bytes), field_name, token,
        )


async def deexternalize_payloads_async(
    msg: proto_message.Message,
    store: PayloadStore,
) -> None:
    """Async version of :func:`deexternalize_payloads`."""
    await _walk_and_deexternalize_async(msg, store)


# ------------------------------------------------------------------
# Internal preflight and recursive walkers – sync
# ------------------------------------------------------------------

def _is_map_field(fd: Any) -> bool:
    """Return True if the field descriptor represents a protobuf map field."""
    mt = fd.message_type
    return mt is not None and fd.is_repeated and mt.GetOptions().map_entry


def _iter_payload_fields(
    msg: proto_message.Message,
) -> Iterator[tuple[str, wrappers_pb2.StringValue]]:
    for fd in msg.DESCRIPTOR.fields:
        if fd.message_type is None:
            continue

        if _is_map_field(fd):
            values = getattr(msg, fd.name).values()
        elif fd.is_repeated:
            values = getattr(msg, fd.name)
        else:
            if not msg.HasField(fd.name):
                continue
            values = (getattr(msg, fd.name),)

        for value in values:
            if isinstance(value, wrappers_pb2.StringValue):
                yield fd.name, value
            elif isinstance(value, proto_message.Message):
                yield from _iter_payload_fields(value)


def _collect_payloads_to_externalize(
    msg: proto_message.Message,
    store: PayloadStore,
) -> list[tuple[str, wrappers_pb2.StringValue, bytes]]:
    threshold = store.options.threshold_bytes
    max_bytes = store.options.max_stored_payload_bytes
    payloads: list[tuple[str, wrappers_pb2.StringValue, bytes]] = []
    for field_name, sv in _iter_payload_fields(msg):
        val = sv.value
        if not val or store.is_known_token(val):
            continue
        payload_bytes = val.encode("utf-8")
        if len(payload_bytes) <= threshold:
            continue
        if len(payload_bytes) > max_bytes:
            raise PayloadSizeExceededError(
                f"Payload size {len(payload_bytes)} bytes exceeds the maximum "
                f"allowed size of {max_bytes} bytes."
            )
        payloads.append((field_name, sv, payload_bytes))
    return payloads


def _walk_and_deexternalize(
    msg: proto_message.Message,
    store: PayloadStore,
) -> None:
    for fd in msg.DESCRIPTOR.fields:
        if fd.message_type is None:
            continue

        if _is_map_field(fd):
            for map_value in getattr(msg, fd.name).values():
                if isinstance(map_value, proto_message.Message):
                    if isinstance(map_value, wrappers_pb2.StringValue):
                        _try_deexternalize_field(map_value, store)
                    else:
                        _walk_and_deexternalize(map_value, store)
        elif fd.is_repeated:
            value = getattr(msg, fd.name)
            for item in value:
                if isinstance(item, proto_message.Message):
                    if isinstance(item, wrappers_pb2.StringValue):
                        _try_deexternalize_field(item, store)
                    else:
                        _walk_and_deexternalize(item, store)
        else:
            if not msg.HasField(fd.name):
                continue
            value = getattr(msg, fd.name)
            if isinstance(value, wrappers_pb2.StringValue):
                _try_deexternalize_field(value, store)
            else:
                _walk_and_deexternalize(value, store)


def _try_deexternalize_field(
    sv: wrappers_pb2.StringValue,
    store: PayloadStore,
) -> None:
    val = sv.value
    if not val or not store.is_known_token(val):
        return
    payload_bytes = store.download(val)
    sv.value = payload_bytes.decode("utf-8")
    logger.debug("De-externalized token %s -> %d bytes", val, len(payload_bytes))


# ------------------------------------------------------------------
# Internal recursive walkers – async
# ------------------------------------------------------------------

async def _walk_and_deexternalize_async(
    msg: proto_message.Message,
    store: PayloadStore,
) -> None:
    for fd in msg.DESCRIPTOR.fields:
        if fd.message_type is None:
            continue

        if _is_map_field(fd):
            for map_value in getattr(msg, fd.name).values():
                if isinstance(map_value, proto_message.Message):
                    if isinstance(map_value, wrappers_pb2.StringValue):
                        await _try_deexternalize_field_async(map_value, store)
                    else:
                        await _walk_and_deexternalize_async(map_value, store)
        elif fd.is_repeated:
            value = getattr(msg, fd.name)
            for item in value:
                if isinstance(item, proto_message.Message):
                    if isinstance(item, wrappers_pb2.StringValue):
                        await _try_deexternalize_field_async(item, store)
                    else:
                        await _walk_and_deexternalize_async(item, store)
        else:
            if not msg.HasField(fd.name):
                continue
            value = getattr(msg, fd.name)
            if isinstance(value, wrappers_pb2.StringValue):
                await _try_deexternalize_field_async(value, store)
            else:
                await _walk_and_deexternalize_async(value, store)


async def _try_deexternalize_field_async(
    sv: wrappers_pb2.StringValue,
    store: PayloadStore,
) -> None:
    val = sv.value
    if not val or not store.is_known_token(val):
        return
    payload_bytes = await store.download_async(val)
    sv.value = payload_bytes.decode("utf-8")
    logger.debug("De-externalized token %s -> %d bytes", val, len(payload_bytes))
