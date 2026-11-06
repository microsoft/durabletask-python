# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the durable binding converters and their registration.

The ``azure-functions`` SDK exposes ``register_converter`` so this package can
own and register the Durable Functions binding converters. These tests verify
that importing the package installs our converters over the SDK defaults, and
that the converters use the durabletask-based encodings the host expects.
"""


import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from azure.functions import meta
from azure.functions.meta import get_binding_registry

import azure.durable_functions  # noqa: F401 - import triggers registration
from azure.durable_functions.constants import (
    ACTIVITY_TRIGGER,
    DURABLE_CLIENT,
    ENTITY_TRIGGER,
    ORCHESTRATION_TRIGGER,
)
from azure.durable_functions.internal.converters import (
    ActivityTriggerConverter,
    DurableClientConverter,
    EntityTriggerConverter,
    OrchestrationTriggerConverter,
    register_durable_converters,
)
from azure.durable_functions.internal import payloads
from azure.durable_functions.internal.serialization import FunctionsDataConverter
from azure.durable_functions.internal.compat.activity import wrap_activity_payloads


def _encode_activity(value):
    host_input = ActivityTriggerConverter.decode(
        meta.Datum(type="json", value="null"), trigger_metadata=None)
    result = wrap_activity_payloads(lambda payload: value, "payload")(host_input)
    return ActivityTriggerConverter.encode(result, expected_type=None)


def _decode_activity(datum, **kwargs):
    received = []
    wrapper = wrap_activity_payloads(lambda payload: received.append(payload), "payload")
    wrapper(ActivityTriggerConverter.decode(datum, trigger_metadata=None))
    return received[0]


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def test_import_registers_our_converters_over_sdk_defaults():
    registry = get_binding_registry()
    assert registry.get(ORCHESTRATION_TRIGGER) is OrchestrationTriggerConverter
    assert registry.get(ENTITY_TRIGGER) is EntityTriggerConverter
    assert registry.get(ACTIVITY_TRIGGER) is ActivityTriggerConverter
    assert registry.get(DURABLE_CLIENT) is DurableClientConverter


def test_register_durable_converters_is_idempotent():
    # A second registration must not raise (it passes overwrite=True).
    register_durable_converters()
    registry = get_binding_registry()
    assert registry.get(ORCHESTRATION_TRIGGER) is OrchestrationTriggerConverter
    assert registry.get(DURABLE_CLIENT) is DurableClientConverter


# ---------------------------------------------------------------------------
# Orchestration / entity triggers
# ---------------------------------------------------------------------------

def test_orchestration_trigger_encodes_result_as_string():
    datum = OrchestrationTriggerConverter.encode("base64response", expected_type=None)
    assert datum.type == "string"
    assert datum.value == "base64response"


def test_entity_trigger_encodes_result_as_string():
    datum = EntityTriggerConverter.encode("base64response", expected_type=None)
    assert datum.type == "string"
    assert datum.value == "base64response"


def test_orchestration_trigger_decodes_to_context_wrapping_body():
    ctx = OrchestrationTriggerConverter.decode(
        meta.Datum(type="string", value="the-body"), trigger_metadata=None)
    assert ctx.body == "the-body"


def test_entity_trigger_decodes_to_context_wrapping_body():
    ctx = EntityTriggerConverter.decode(
        meta.Datum(type="string", value="the-body"), trigger_metadata=None)
    assert ctx.body == "the-body"


def test_triggers_have_implicit_output_and_trigger_support():
    for conv in (OrchestrationTriggerConverter, EntityTriggerConverter,
                 ActivityTriggerConverter):
        assert conv.has_implicit_output() is True
        assert conv.has_trigger_support() is True


# ---------------------------------------------------------------------------
# Activity trigger
# ---------------------------------------------------------------------------

def test_activity_trigger_round_trips_json_payload():
    payload = {"a": 1, "b": ["x", "y"]}
    encoded = ActivityTriggerConverter.encode(payload, expected_type=None)
    assert encoded.type == "json"
    decoded = ActivityTriggerConverter.decode(encoded, trigger_metadata=None)
    assert decoded == payload


def test_activity_trigger_decode_falls_back_to_raw_string():
    decoded = ActivityTriggerConverter.decode(
        meta.Datum(type="string", value="not-json"), trigger_metadata=None)
    assert decoded == "not-json"


@pytest.mark.parametrize("data_type", ["string", "json"])
@pytest.mark.parametrize("value", [{"data": "x" * 200}, "x" * 200, ["x" * 200]])
def test_activity_trigger_externalizes_and_hydrates(monkeypatch, payload_store_factory, data_type, value):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    encoded = _encode_activity(value)
    token = json.loads(encoded.value)
    assert store.is_known_token(token)
    assert json.loads(store.download(token)) == value
    decoded = _decode_activity(
        meta.Datum(type=data_type, value=encoded.value), trigger_metadata=None)
    assert decoded == value
    assert _decode_activity(
        meta.Datum(type=data_type, value=token), trigger_metadata=None) == value


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("quoted", [False, True])
@pytest.mark.parametrize("version", ["v1", "v2"])
@pytest.mark.parametrize("account_wide", [False, True])
async def test_blob_payload_references_round_trip_through_functions(
        monkeypatch, use_async, quoted, version, account_wide):
    pytest.importorskip("azure.storage.blob")
    from azure.storage.blob import BlobClient
    from azure.storage.blob.aio import BlobClient as AsyncBlobClient
    from durabletask.extensions.azure_blob_payloads import BlobPayloadStore, BlobPayloadStoreOptions

    store = BlobPayloadStore(BlobPayloadStoreOptions(
        account_url="https://example.blob.core.windows.net",
        threshold_bytes=10, enable_compression=False, allow_all_containers_in_configured_account=account_wide,
    ))
    store._ensure_container_created = True
    value = json.dumps({"value": "payload" * 100})
    sync_stream = MagicMock()
    sync_stream.readall.return_value = value.encode()
    async_stream = MagicMock()
    async_stream.readall = AsyncMock(return_value=value.encode())
    monkeypatch.setattr(BlobClient, "upload_blob", MagicMock())
    monkeypatch.setattr(AsyncBlobClient, "upload_blob", AsyncMock())
    sync_download = MagicMock(return_value=sync_stream)
    async_download = AsyncMock(return_value=async_stream)
    monkeypatch.setattr(BlobClient, "download_blob", sync_download)
    monkeypatch.setattr(AsyncBlobClient, "download_blob", async_download)
    monkeypatch.setattr(payloads, "_payload_store", store)
    try:
        encoded = (await payloads.externalize_activity_output_async(value) if use_async
                   else payloads.externalize_activity_output(value))
        token = json.loads(encoded)
        assert token.startswith("blob:v2:https://example.blob.core.windows.net/durabletask-payloads/")
        if version == "v1":
            blob_name = store._get_download_destination(token)[2]
            token = f"blob:v1:durabletask-payloads:{blob_name}"
        reference = json.dumps(token) if quoted else token
        decoded = (await payloads.deexternalize_payload_async(reference) if use_async
                   else payloads.deexternalize_payload(reference))
        assert decoded == value

        sync_download.reset_mock()
        async_download.reset_mock()
        other_container = ("blob:v1:other-container:blob" if version == "v1"
                           else "blob:v2:https://example.blob.core.windows.net/other-container/blob")
        reference = json.dumps(other_container) if quoted else other_container
        if account_wide:
            decoded = (await payloads.deexternalize_payload_async(reference) if use_async
                       else payloads.deexternalize_payload(reference))
            assert decoded == value
        else:
            with pytest.raises(ValueError, match="configured container"):
                if use_async:
                    await payloads.deexternalize_payload_async(reference)
                else:
                    payloads.deexternalize_payload(reference)
            sync_download.assert_not_called()
            async_download.assert_not_called()

        sync_download.reset_mock()
        async_download.reset_mock()
        reference = "blob:v2:https://unlisted.blob.core.windows.net/durabletask-payloads/blob"
        if quoted:
            reference = json.dumps(reference)
        with pytest.raises(ValueError, match="Storage endpoint"):
            if use_async:
                await payloads.deexternalize_payload_async(reference)
            else:
                payloads.deexternalize_payload(reference)
        sync_download.assert_not_called()
        async_download.assert_not_called()
    finally:
        store.close()
        await store.close_async()


def test_activity_trigger_keeps_small_payload_inline(monkeypatch, payload_store_factory):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    encoded = _encode_activity({"small": True})
    assert json.loads(encoded.value) == {"small": True}
    assert not store._blobs


def test_activity_trigger_payload_size_limit(monkeypatch, payload_store_factory):
    store = payload_store_factory(threshold_bytes=10, max_stored_payload_bytes=100)
    monkeypatch.setattr(payloads, "_payload_store", store)
    with pytest.raises(ValueError, match="exceeds the maximum"):
        _encode_activity("x" * 200)
    assert not store._blobs


@pytest.mark.parametrize("reference", [
    "blob:v1:test-container:missing",
    json.dumps("blob:v1:test-container:missing"),
])
def test_activity_trigger_does_not_swallow_download_failure(monkeypatch, payload_store_factory, reference):
    monkeypatch.setattr(payloads, "_payload_store", payload_store_factory())
    with pytest.raises(KeyError):
        _decode_activity(
            meta.Datum(type="string", value=reference),
            trigger_metadata=None)


def test_whole_payload_token_string_is_reserved(monkeypatch, payload_store_factory):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    stored_value = {"stored": "payload contents"}
    reference = store.upload(json.dumps(stored_value).encode())

    encoded = _encode_activity(reference)

    assert encoded.value == json.dumps(reference)
    assert len(store._blobs) == 1
    assert _decode_activity(encoded) == stored_value
    assert FunctionsDataConverter().deserialize(payloads.deexternalize_payload(encoded.value)) == stored_value


@pytest.mark.parametrize("threshold_bytes", [10, 1024])
@pytest.mark.parametrize("reference_exists", [False, True])
def test_object_wrapped_reference_round_trips_as_data(
        monkeypatch, payload_store_factory, threshold_bytes, reference_exists):
    store = payload_store_factory(threshold_bytes=threshold_bytes)
    monkeypatch.setattr(payloads, "_payload_store", store)
    reference = "blob:v1:test-container:missing"
    if reference_exists:
        reference = store.upload(b'{"stored":"not the literal reference"}')
    value = {"reference": reference}

    encoded = _encode_activity(value)

    assert len(store._blobs) == int(reference_exists) + int(threshold_bytes == 10)
    assert _decode_activity(encoded) == value
    assert FunctionsDataConverter().deserialize(payloads.deexternalize_payload(encoded.value)) == value


def test_codec_does_not_access_storage(monkeypatch, payload_store_factory):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    value = {"data": "x" * 200}
    token = store.upload(json.dumps(value).encode())

    def unexpected_download(token):
        raise AssertionError("Serialization must not access storage")
    monkeypatch.setattr(store, "download", unexpected_download)
    converter = FunctionsDataConverter()
    response = converter.deserialize(json.dumps({"result": json.dumps(token)}))
    assert converter.deserialize(response["result"], str) == token


@pytest.mark.asyncio
async def test_transport_store_async_references(monkeypatch, payload_store_factory):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    transport = payloads.get_transport_payload_store()
    value = b'{"data":"example"}'
    token = await transport.upload_async(value, instance_id="test-instance")
    assert transport.is_known_token(token)
    assert store.is_known_token(json.loads(token))
    assert await transport.download_async(token) == value
    assert await transport.download_async(json.loads(token)) == value
    assert not transport.is_known_token('{"ordinary":"object"}')
    assert not transport.is_known_token('"ordinary string"')


def test_reference_detection_skips_non_string_json(monkeypatch, payload_store_factory):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    transport = payloads.get_transport_payload_store()
    token = store.upload(b'"value"')
    assert transport.is_known_token(" \r\n\t" + json.dumps(token))
    assert transport.is_known_token('"\\u0062lob:v1:test-container:blob-0"')

    def unexpected_parse(value):
        raise AssertionError("Non-string payload should not be parsed")

    monkeypatch.setattr(payloads.json, "loads", unexpected_parse)
    for value in (' {"result":"example"}', '["example"]', 'null', 'true', '42', token):
        assert transport.is_known_token(value) == (value == token)


# ---------------------------------------------------------------------------
# Durable client
# ---------------------------------------------------------------------------

def test_durable_client_accepts_client_and_string_annotations():
    from azure.durable_functions.client import (
        DurableFunctionsClient,
        SyncDurableFunctionsClient,
    )
    assert DurableClientConverter.check_input_type_annotation(DurableFunctionsClient)
    assert DurableClientConverter.check_input_type_annotation(
        SyncDurableFunctionsClient)
    assert DurableClientConverter.check_input_type_annotation(str)
    assert DurableClientConverter.check_input_type_annotation(bytes)
    assert not DurableClientConverter.check_input_type_annotation(int)


def test_durable_client_has_no_trigger_support_or_implicit_output():
    assert DurableClientConverter.has_trigger_support() is False
    assert DurableClientConverter.has_implicit_output() is False


def test_durable_client_decode_returns_host_configuration():
    result = DurableClientConverter.decode(
        meta.Datum(type="string", value="client-config"),
        trigger_metadata=None)
    assert result == "client-config"


def test_durable_client_decode_preserves_host_configuration():
    config = json.dumps({
        "taskHubName": "TestHub",
        "requiredQueryStringParameters": "code=xyz",
        "baseUrl": "http://localhost:7071/runtime/webhooks/durabletask",
        "rpcBaseUrl": "http://localhost:8080/",
    })

    value = DurableClientConverter.decode(
        meta.Datum(type="string", value=config), trigger_metadata=None)
    assert value == config


# ---------------------------------------------------------------------------
# FunctionsDataConverter reconstruction and discovery
# ---------------------------------------------------------------------------

def test_functions_data_converter_can_reconstruct_and_discovery_unaffected():
    """Azure Functions overrides can_reconstruct() to return True uniformly.

    The core SDK's expanded dict/tuple traversal in JsonDataConverter does not
    alter Azure Functions discovery behavior, because FunctionsDataConverter
    already returns True for all types (delegating decoding to df_loads).
    """
    from collections.abc import Mapping
    from dataclasses import dataclass
    from durabletask.internal import type_discovery

    @dataclass
    class Order:
        item: str
        quantity: int

    conv = FunctionsDataConverter()

    # Always returns True for affected container types
    assert conv.can_reconstruct(dict[str, Order]) is True
    assert conv.can_reconstruct(Mapping[str, Order]) is True
    assert conv.can_reconstruct(tuple[Order, ...]) is True
    assert conv.can_reconstruct(tuple[Order, int]) is True

    # Always returns True even for builtins and unknown types (distinct from core)
    assert conv.can_reconstruct(dict[str, int]) is True
    assert conv.can_reconstruct(tuple[int, str]) is True
    assert conv.can_reconstruct(int) is True
    assert conv.can_reconstruct(dict) is True
    assert conv.can_reconstruct(tuple) is True

    # Inbound and outbound type discovery with FunctionsDataConverter hands declared types through
    def sample_func(ctx, orders: dict[str, Order]) -> tuple[Order, ...]:
        pass

    assert type_discovery.orchestrator_input_type(sample_func, conv) == dict[str, Order]
    assert type_discovery.activity_input_type(sample_func, conv) == dict[str, Order]
    assert type_discovery.activity_output_type(sample_func, conv) == tuple[Order, ...]

    def builtin_func(ctx, data: dict[str, int]) -> tuple[int, ...]:
        pass

    # Builtins are also passed through to df_loads under FunctionsDataConverter
    assert type_discovery.orchestrator_input_type(builtin_func, conv) == dict[str, int]
    assert type_discovery.activity_output_type(builtin_func, conv) == tuple[int, ...]

