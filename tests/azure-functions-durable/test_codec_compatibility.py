# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Compatibility coverage for the shared Functions JSON codec."""

import base64
from contextlib import nullcontext
import inspect
import json
from unittest.mock import Mock
import warnings

import pytest
import azure.functions._durable_functions as shared_codec
from azure.functions import meta

from azure.durable_functions import DFApp
from azure.durable_functions.internal import payloads, serialization
from azure.durable_functions.internal.compat.activity import wrap_activity_payloads
from azure.durable_functions.internal.converters import ActivityTriggerConverter
from azure.durable_functions.worker import DurableFunctionsWorker
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb


class LegacyPayload:
    def __init__(self, value):
        self.value = value

    def to_json(self):
        return {"value": self.value}

    @classmethod
    def from_json(cls, data):
        return cls(data["value"])

    def __eq__(self, other):
        return isinstance(other, LegacyPayload) and self.value == other.value


@pytest.fixture(autouse=True)
def reset_codec_advisories(monkeypatch):
    monkeypatch.setattr(shared_codec, "_no_expected_type_notice_emitted", False)
    monkeypatch.setattr(shared_codec, "_loose_codec_notice_emitted", False)


def encode_orchestration_request(value):
    encoded_input = serialization.FunctionsDataConverter().serialize(value)
    request = pb.OrchestratorRequest(instanceId="codec-compat")
    request.newEvents.extend([
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(
            "codec_orchestrator", request.instanceId, encoded_input),
    ])
    return base64.b64encode(request.SerializeToString()).decode("ascii")


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("typed", [False, True])
def test_custom_object_decode_preserves_typing_mode(monkeypatch, strict, typed):
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "1" if strict else "0")
    converter = serialization.FunctionsDataConverter()
    value = LegacyPayload(7)
    encoded = converter.serialize(value)
    target_type = LegacyPayload if typed else None

    if strict and not typed:
        with pytest.raises(TypeError, match="strict mode requires expected_type"):
            converter.deserialize(encoded, target_type)
    elif strict:
        assert converter.deserialize(encoded, target_type) == value
    else:
        with pytest.warns(DeprecationWarning) as notices:
            assert converter.deserialize(encoded, target_type) == value
        messages = [str(notice.message) for notice in notices]
        assert len(messages) == (1 if typed else 2)
        assert any("loose-mode object_hook path" in message for message in messages)
        assert any("without expected_type" in message for message in messages) == (not typed)


def test_loose_advisories_are_emitted_once(monkeypatch):
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "0")
    converter = serialization.FunctionsDataConverter()
    value = LegacyPayload(7)
    encoded = converter.serialize(value)
    with pytest.warns(DeprecationWarning) as notices:
        assert converter.deserialize(encoded) == value
    assert len(notices) == 2
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        assert converter.deserialize(encoded) == value


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("target_type", [None, dict])
def test_nested_legacy_objects_preserve_wire_shape(monkeypatch, strict, target_type):
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "0")
    converter = serialization.FunctionsDataConverter()
    encoded = converter.serialize({"items": [LegacyPayload(3)]})
    assert encoded is not None
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "1" if strict else "0")

    if strict:
        assert converter.deserialize(encoded, target_type) == json.loads(encoded)
    else:
        with pytest.warns(DeprecationWarning) as notices:
            assert converter.deserialize(encoded, target_type) == {"items": [LegacyPayload(3)]}
        assert any("loose-mode object_hook path" in str(notice.message) for notice in notices)


@pytest.mark.parametrize("strict", [False, True])
def test_mismatched_destination_type_preserves_codec_policy(monkeypatch, caplog, strict):
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "1" if strict else "0")
    converter = serialization.FunctionsDataConverter()
    value = LegacyPayload(4)
    encoded = converter.serialize(value)

    if strict:
        with pytest.raises(TypeError, match="payload declares class"):
            converter.deserialize(encoded, dict)
    else:
        with pytest.warns(DeprecationWarning, match="loose-mode object_hook path"):
            assert converter.deserialize(encoded, dict) == value
        assert "payload declares class" in caplog.text


@pytest.mark.parametrize("target_type", [None, LegacyPayload])
def test_loose_custom_object_requires_an_already_loaded_module(monkeypatch, target_type):
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "0")
    encoded = json.dumps({
        "__class__": "Missing",
        "__module__": "_durable_codec_missing_test_module",
        "__data__": {},
    })

    notice = pytest.warns(DeprecationWarning, match="without expected_type") \
        if target_type is None else nullcontext()
    with notice:
        with pytest.raises(ValueError, match="not loaded in sys.modules"):
            serialization.FunctionsDataConverter().deserialize(encoded, target_type)


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("native", [False, True])
def test_orchestration_input_uses_declared_type(monkeypatch, strict, native):
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "1" if strict else "0")
    monkeypatch.setattr(payloads, "_payload_store", None)
    codec_loads = Mock(wraps=shared_codec.df_loads)
    monkeypatch.setattr(serialization, "df_loads", codec_loads)

    def native_orchestrator(context, value: LegacyPayload):
        return value.value

    def compat_orchestrator(context):
        return context.get_input().value

    if native:
        orchestrator = native_orchestrator
    else:
        # The decorator also accepts v1's one-argument shape at runtime.
        builder = DFApp().orchestration_trigger(
            context_name="context", input_type=LegacyPayload)(
                compat_orchestrator)  # pyright: ignore[reportArgumentType]
        orchestrator = getattr(builder.build().get_user_function(), "orchestrator_function")

    wire_request = encode_orchestration_request(LegacyPayload(11))
    notice = pytest.warns(DeprecationWarning, match="loose-mode object_hook path") \
        if not strict else nullcontext()
    with notice:
        wire_response = DurableFunctionsWorker().execute_orchestration_request(
            orchestrator, wire_request)
    response = pb.OrchestratorResponse.FromString(base64.b64decode(wire_response))
    completion = response.actions[0].completeOrchestration

    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(completion.result.value) == 11
    assert codec_loads.call_args_list[0].kwargs["expected_type"] is LegacyPayload


def test_v1_callsite_type_cannot_decode_initial_strict_input(monkeypatch):
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "1")
    monkeypatch.setattr(payloads, "_payload_store", None)
    called = []

    def compat_orchestrator(context):
        called.append(True)
        return context.get_input(expected_type=LegacyPayload).value

    # The decorator also accepts v1's one-argument shape at runtime.
    builder = DFApp().orchestration_trigger(context_name="context")(
        compat_orchestrator)  # pyright: ignore[reportArgumentType]
    orchestrator = getattr(builder.build().get_user_function(), "orchestrator_function")
    wire_response = DurableFunctionsWorker().execute_orchestration_request(
        orchestrator, encode_orchestration_request(LegacyPayload(11)))
    response = pb.OrchestratorResponse.FromString(base64.b64decode(wire_response))
    completion = response.actions[0].completeOrchestration

    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert "strict mode requires expected_type" in completion.failureDetails.errorMessage
    assert called == []


@pytest.mark.asyncio
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("storage", ["disabled", "inline", "externalized"])
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("annotated", [False, True])
async def test_activity_custom_input_remains_untyped(
        monkeypatch, payload_store_factory, strict, storage, asynchronous, annotated):
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "0")
    value = LegacyPayload(8)
    encoded = serialization.FunctionsDataConverter().serialize(value)
    assert encoded is not None
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", None if storage == "disabled" else store)
    if storage == "externalized":
        encoded = json.dumps(store.upload(encoded.encode("utf-8")))
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "1" if strict else "0")
    received = []

    def sync_activity(payload):
        received.append(payload)

    async def async_activity(payload):
        received.append(payload)

    activity = async_activity if asynchronous else sync_activity
    if annotated:
        activity.__annotations__["payload"] = LegacyPayload
    wrapper = wrap_activity_payloads(activity, "payload")
    if annotated:
        assert inspect.signature(wrapper).parameters["payload"].annotation is LegacyPayload

    async def invoke():
        decoded = ActivityTriggerConverter.decode(
            meta.Datum(type="json", value=encoded), trigger_metadata=None)
        result = wrapper(decoded)
        return await result if asynchronous else result

    if strict:
        with pytest.raises(ValueError) as error:
            await invoke()
        assert isinstance(error.value.__cause__, TypeError)
        assert "strict mode requires expected_type" in str(error.value.__cause__)
        assert received == []
    else:
        with pytest.warns(DeprecationWarning) as notices:
            assert await invoke() is None
        assert received == [value]
        messages = [str(notice.message) for notice in notices]
        assert any("without expected_type" in message for message in messages)
        assert any("loose-mode object_hook path" in message for message in messages)


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("externalized", [False, True])
def test_activity_annotation_does_not_validate_plain_json(
        monkeypatch, payload_store_factory, strict, externalized):
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "1" if strict else "0")
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store if externalized else None)
    encoded = json.dumps("not an integer")
    if externalized:
        encoded = json.dumps(store.upload(encoded.encode("utf-8")))
    received = []

    def activity(payload: int):
        received.append(payload)

    notice = pytest.warns(DeprecationWarning, match="without expected_type") \
        if not strict else nullcontext()
    with notice:
        decoded = ActivityTriggerConverter.decode(
            meta.Datum(type="json", value=encoded), trigger_metadata=None)
        assert wrap_activity_payloads(activity, "payload")(decoded) is None
    assert received == ["not an integer"]


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("store_enabled", [False, True])
def test_activity_non_json_string_fallback(
        monkeypatch, payload_store_factory, strict, store_enabled):
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "1" if strict else "0")
    monkeypatch.setattr(
        payloads, "_payload_store", payload_store_factory() if store_enabled else None)
    received = []

    def activity(payload):
        received.append(payload)

    notice = pytest.warns(DeprecationWarning, match="without expected_type") \
        if not strict else nullcontext()
    with notice:
        decoded = ActivityTriggerConverter.decode(
            meta.Datum(type="string", value="not-json"), trigger_metadata=None)
        assert wrap_activity_payloads(activity, "payload")(decoded) is None
    assert received == ["not-json"]
