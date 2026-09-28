# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Process-wide payload storage configured at Function app startup."""

import json
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from itertools import chain
from typing import Any, cast, override
from uuid import UUID

from google.protobuf.wrappers_pb2 import StringValue

from durabletask import history
from durabletask.entities import EntityInstanceId
from durabletask.internal.orchestrator_service_pb2 import (
    ActivityRequest, ActivityResponse, HistoryEvent, OrchestratorRequest,
)
from durabletask.payload import (
    LargePayloadStorageOptions,
    PayloadStore,
    deexternalize_payloads,
    deexternalize_payloads_async,
    externalize_payloads,
    externalize_payloads_async,
)

_payload_store: PayloadStore | None = None


def configure_payload_store(payload_store: object) -> None:
    """Register one store per worker process, allowing identical registrations."""
    global _payload_store
    if not isinstance(payload_store, PayloadStore):
        raise TypeError("payload_store must be a PayloadStore")
    if _payload_store is not None and _payload_store is not payload_store:
        raise ValueError("A different payload store is already configured in this process")
    _payload_store = payload_store


def get_payload_store() -> PayloadStore | None:
    """Return the app's store, or None when externalization is disabled."""
    return _payload_store


class _FunctionsPayloadStore(PayloadStore):
    """Keep references valid JSON for the Functions host's payload readers."""

    def __init__(self, store: PayloadStore) -> None:
        self._store = store

    @property
    @override
    def options(self) -> LargePayloadStorageOptions:
        return self._store.options

    @override
    def upload(self, data: bytes, *, instance_id: str | None = None) -> str:
        return json.dumps(self._store.upload(data, instance_id=instance_id))

    @override
    async def upload_async(self, data: bytes, *, instance_id: str | None = None) -> str:
        return json.dumps(await self._store.upload_async(data, instance_id=instance_id))

    def _unwrap(self, token: str) -> str:
        if self._store.is_known_token(token):
            return token
        if not token.lstrip(" \t\r\n").startswith('"'):
            return token
        try:
            value = json.loads(token)
        except (ValueError, TypeError):
            return token
        return value if isinstance(value, str) else token

    @override
    def is_known_token(self, value: str) -> bool:
        return self._store.is_known_token(self._unwrap(value))

    @override
    def download(self, token: str) -> bytes:
        return self._store.download(self._unwrap(token))

    @override
    async def download_async(self, token: str) -> bytes:
        return await self._store.download_async(self._unwrap(token))


def get_transport_payload_store() -> PayloadStore | None:
    """Resolve the configured store with Functions-compatible reference encoding."""
    store = get_payload_store()
    return _FunctionsPayloadStore(store) if store is not None else None


def deexternalize_payload(value: str) -> str:
    """Resolve a reference before the Functions JSON codec reads the payload."""
    store = get_transport_payload_store()
    if store is None:
        return value
    request = ActivityRequest(input=StringValue(value=value))
    deexternalize_payloads(request, store)
    return request.input.value


@dataclass(frozen=True, slots=True)
class ActivityPayload:
    value: str


async def deexternalize_payload_async(value: str) -> str:
    store = get_transport_payload_store()
    if store is None:
        return value
    request = ActivityRequest(input=StringValue(value=value))
    await deexternalize_payloads_async(request, store)
    return request.input.value


async def externalize_activity_output_async(value: str) -> str:
    store = get_transport_payload_store()
    if store is None:
        return value
    response = ActivityResponse(result=StringValue(value=value))
    await externalize_payloads_async(response, store)
    return response.result.value


def externalize_activity_output(value: str) -> str:
    """Apply the core payload policy to a serialized activity output."""
    store = get_transport_payload_store()
    if store is None:
        return value
    response = ActivityResponse(result=StringValue(value=value))
    externalize_payloads(response, store)
    return response.result.value


def _entity_payload_fields(
        events: Sequence[history.HistoryEvent], instance_id: str, *, include_inputs: bool,
) -> Iterator[tuple[history.EventSentEvent | history.EventRaisedEvent, dict[str, Any], str]]:
    pending: set[str] = set()
    for event in events:
        if isinstance(event, history.EntityOperationCalledEvent):
            pending.add(event.request_id)
            continue
        if not isinstance(event, (history.EventSentEvent, history.EventRaisedEvent)):
            continue
        if isinstance(event, history.EventRaisedEvent):
            if event.name not in pending:
                continue
            pending.remove(event.name)
            field = "result"
        else:
            if event.name != "op" and not event.name.startswith("op@"):
                continue
            try:
                EntityInstanceId.parse(event.instance_id)
            except ValueError:
                continue
            field = "input"
        try:
            parsed = json.loads(event.input or "")
        except ValueError:
            continue
        if not isinstance(parsed, dict):
            continue
        envelope = cast(dict[str, Any], parsed)
        if isinstance(event, history.EventSentEvent):
            if not isinstance(envelope.get("op"), str) or not envelope["op"]:
                continue
            request_id = envelope.get("id")
            if not isinstance(request_id, str):
                continue
            try:
                UUID(request_id)
            except ValueError:
                continue
            if envelope.get("signal", False) is False:
                if envelope.get("parent") != instance_id:
                    continue
                pending.add(request_id)
            elif envelope.get("signal") is not True:
                continue
        if (include_inputs or field == "result") and isinstance(envelope.get(field), str):
            yield event, envelope, field


def hydrate_entity_history(
        events: list[history.HistoryEvent], store: PayloadStore | None, instance_id: str,
        *, include_inputs: bool = True,
) -> None:
    """Hydrate serialized entity protocol fields, never arbitrary object members."""
    if store is None:
        return
    for event, envelope, field in _entity_payload_fields(events, instance_id, include_inputs=include_inputs):
        request = ActivityRequest(input=StringValue(value=envelope[field]))
        deexternalize_payloads(request, store)
        if request.input.value != envelope[field]:
            envelope[field] = request.input.value
            event.input = json.dumps(envelope)


async def hydrate_entity_history_async(
        events: list[history.HistoryEvent], store: PayloadStore | None, instance_id: str,
        *, include_inputs: bool = True,
) -> None:
    """Hydrate entity history using the store's asynchronous download API."""
    if store is None:
        return
    for event, envelope, field in _entity_payload_fields(events, instance_id, include_inputs=include_inputs):
        request = ActivityRequest(input=StringValue(value=envelope[field]))
        await deexternalize_payloads_async(request, store)
        if request.input.value != envelope[field]:
            envelope[field] = request.input.value
            event.input = json.dumps(envelope)


def _entity_request_events(request: OrchestratorRequest) -> list[tuple[HistoryEvent, history.HistoryEvent]]:
    return [
        (event, history._from_protobuf(event))  # pyright: ignore[reportPrivateUsage]
        for event in chain(request.pastEvents, request.newEvents)
        if event.WhichOneof("eventType") in ("eventSent", "eventRaised", "entityOperationCalled")
    ]


def discard_scheduled_activity_inputs(request: OrchestratorRequest) -> None:
    """Omit activity inputs that replay never consumes before downloading references."""
    for event in chain(request.pastEvents, request.newEvents):
        if event.HasField("taskScheduled"):
            event.taskScheduled.ClearField("input")


def _update_entity_request_events(events: list[tuple[HistoryEvent, history.HistoryEvent]]) -> None:
    for source, event in events:
        if isinstance(event, history.EventRaisedEvent) and event.input is not None:
            source.eventRaised.input.value = event.input


def hydrate_entity_request(request: OrchestratorRequest, store: PayloadStore) -> None:
    """Hydrate entity replies for replay, leaving unused historical inputs alone."""
    events = _entity_request_events(request)
    hydrate_entity_history([event for _, event in events], store, request.instanceId, include_inputs=False)
    _update_entity_request_events(events)


async def hydrate_entity_request_async(request: OrchestratorRequest, store: PayloadStore) -> None:
    """Await entity reply payloads before replay, without downloading historical inputs."""
    events = _entity_request_events(request)
    await hydrate_entity_history_async([event for _, event in events], store, request.instanceId, include_inputs=False)
    _update_entity_request_events(events)
