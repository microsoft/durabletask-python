# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from __future__ import annotations

from typing import AsyncIterable, Iterable, Optional

import durabletask.history as history
import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask.payload import helpers as payload_helpers
from durabletask.payload.store import PayloadStore


def collect_history_events(
    chunks: Iterable[pb.HistoryChunk],
    payload_store: Optional[PayloadStore] = None,
) -> list[history.HistoryEvent]:
    events: list[history.HistoryEvent] = []
    for chunk in chunks:
        events.extend(_clone_and_convert_events(chunk.events, payload_store))
    return events


async def collect_history_events_async(
    chunks: AsyncIterable[pb.HistoryChunk],
    payload_store: Optional[PayloadStore] = None,
) -> list[history.HistoryEvent]:
    events: list[history.HistoryEvent] = []
    async for chunk in chunks:
        events.extend(await _clone_and_convert_events_async(chunk.events, payload_store))
    return events


def history_event_to_dict(event: history.HistoryEvent) -> dict:
    return history.to_dict(event)


def _clone_and_convert_events(
    source_events: Iterable[pb.HistoryEvent],
    payload_store: Optional[PayloadStore],
) -> list[history.HistoryEvent]:
    events: list[history.HistoryEvent] = []
    for source_event in source_events:
        event = source_event
        if payload_store is not None:
            # deexternalize_payloads mutates messages in-place, so clone to avoid
            # mutating protobuf instances owned by gRPC/deserializer internals.
            event = pb.HistoryEvent()
            event.CopyFrom(source_event)
            payload_helpers.deexternalize_payloads(event, payload_store)
        events.append(history._from_protobuf(event))
    return events


async def _clone_and_convert_events_async(
    source_events: Iterable[pb.HistoryEvent],
    payload_store: Optional[PayloadStore],
) -> list[history.HistoryEvent]:
    events: list[history.HistoryEvent] = []
    for source_event in source_events:
        event = source_event
        if payload_store is not None:
            # Async deexternalization mutates messages in-place, so clone first.
            event = pb.HistoryEvent()
            event.CopyFrom(source_event)
            await payload_helpers.deexternalize_payloads_async(event, payload_store)
        events.append(history._from_protobuf(event))
    return events
