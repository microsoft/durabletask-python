# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""App-level encoding policy and shared-codec compatibility coverage."""

import base64
import inspect
import json
from unittest.mock import AsyncMock, Mock

import pytest
from azure.functions import meta
from azure.functions._durable_functions import df_dumps as shared_dumps

import azure.durable_functions as df
from azure.durable_functions.internal import payloads, serialization
from azure.durable_functions.internal.compat.activity import wrap_activity_payloads
from azure.durable_functions.internal.converters import ActivityTriggerConverter
from azure.durable_functions.worker import DurableFunctionsWorker
from durabletask.entities import EntityInstanceId
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb


VALUE = {"\u043a\u043b\u044e\u0447": "\u041f\u0440\u0438\u0432\u0435\u0442 \U0001f680"}
CLIENT_CONFIG = json.dumps({
    "taskHubName": "unicode-tests",
    "rpcBaseUrl": "http://localhost:8080/",
})


@pytest.fixture(autouse=True)
def reset_serialization(monkeypatch):
    monkeypatch.setattr(serialization, "_ensure_ascii", None)
    monkeypatch.setattr(serialization, "_unicode_encoder", None)
    monkeypatch.setattr(payloads, "_payload_store", None)


@pytest.fixture
def configurable_codec(monkeypatch):
    calls = []

    def dumps(value, *, ensure_ascii=True):
        calls.append(ensure_ascii)
        if "ensure_ascii" in inspect.signature(shared_dumps).parameters:
            return shared_dumps(value, ensure_ascii=ensure_ascii)
        legacy = shared_dumps(value)
        if ensure_ascii:
            return legacy
        return json.dumps(json.loads(legacy), ensure_ascii=False)

    monkeypatch.setattr(serialization, "df_dumps", dumps)
    return calls


def test_legacy_sdk_keeps_default_and_explicit_true(monkeypatch):
    calls = []

    def legacy_dumps(value):
        calls.append(value)
        return shared_dumps(value)

    monkeypatch.setattr(serialization, "df_dumps", legacy_dumps)
    converter = serialization.FunctionsDataConverter()
    assert converter.serialize(VALUE) == shared_dumps(VALUE)
    df.DFApp().configure_serialization(ensure_ascii=True)
    assert ActivityTriggerConverter.encode(VALUE, expected_type=None).value == shared_dumps(VALUE)
    assert calls == [VALUE, VALUE]


def test_legacy_sdk_rejects_optin_without_changing_policy(monkeypatch):
    def legacy_dumps(value):
        return shared_dumps(value)

    monkeypatch.setattr(serialization, "df_dumps", legacy_dumps)
    app = df.DFApp()
    with pytest.raises(RuntimeError, match="requires an azure-functions SDK"):
        app.configure_serialization(ensure_ascii=False)
    assert serialization._ensure_ascii is None
    app.configure_serialization()
    assert serialization.serialize_json(VALUE) == shared_dumps(VALUE)


@pytest.mark.parametrize("invalid", [None, 0, 1, "false", [], {}])
def test_configuration_requires_bool(invalid):
    with pytest.raises(TypeError, match="ensure_ascii must be a bool"):
        df.DFApp().configure_serialization(ensure_ascii=invalid)
    assert serialization._ensure_ascii is None


def test_configuration_option_is_keyword_only():
    with pytest.raises(TypeError):
        df.DFApp().configure_serialization(False)


@pytest.mark.parametrize("ensure_ascii", [True, False])
def test_configuration_is_idempotent_but_rejects_conflicts(configurable_codec, ensure_ascii):
    first = df.DFApp()
    second = df.DFApp()
    first.configure_serialization(ensure_ascii=ensure_ascii)
    second.configure_serialization(ensure_ascii=ensure_ascii)
    with pytest.raises(ValueError, match="already configured"):
        second.configure_serialization(ensure_ascii=not ensure_ascii)
    serialized = serialization.serialize_json(VALUE)
    assert json.loads(serialized) == VALUE
    assert configurable_codec == [ensure_ascii]


def test_first_serialization_freezes_legacy_default(configurable_codec):
    assert serialization.serialize_json(VALUE) == shared_dumps(VALUE)
    with pytest.raises(ValueError, match="before serializing durable payloads"):
        df.DFApp().configure_serialization(ensure_ascii=False)
    df.DFApp().configure_serialization(ensure_ascii=True)


@pytest.mark.parametrize("strict", [False, True])
def test_shared_default_and_optin_round_trip(configurable_codec, monkeypatch, strict):
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "1" if strict else "0")
    converter = serialization.FunctionsDataConverter()
    df.DFApp().configure_serialization(ensure_ascii=False)
    for value in [
            VALUE,
            {"text": "\u0645\u0631\u062d\u0628\u0627 \u4f60\u597d e\u0301 \u2028\u2029"},
            {"text": '\x00\n\t"\\ literal \\u0416'},
            {"text": "ASCII only"},
    ]:
        encoded = converter.serialize(value)
        assert encoded == json.dumps(value, ensure_ascii=False)
        assert converter.deserialize(encoded, dict) == value
    assert configurable_codec == [False] * 4


class Label:
    def __init__(self, text):
        self.text = text

    def to_json(self):
        return {"text": self.text}

    @classmethod
    def from_json(cls, data):
        return cls(data["text"])


@pytest.mark.parametrize("strict", [False, True])
def test_optin_preserves_custom_object_rules(configurable_codec, monkeypatch, strict):
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "1" if strict else "0")
    df.DFApp().configure_serialization(ensure_ascii=False)
    converter = serialization.FunctionsDataConverter()
    value = Label("\u0416\U0001f680")
    encoded = converter.serialize(value)
    restored = converter.deserialize(encoded, Label)
    assert restored.text == value.text
    nested = {"label": value}
    if strict:
        with pytest.raises(TypeError):
            converter.serialize(nested)
    else:
        assert converter.deserialize(converter.serialize(nested), dict)["label"].text == value.text


def test_blueprints_and_existing_workers_use_app_policy(configurable_codec):
    blueprint = df.Blueprint()

    @blueprint.orchestration_trigger(context_name="context")
    def unicode_orchestrator(context):
        return context.get_input()

    app = df.DFApp()
    app.register_functions(blueprint)
    worker = DurableFunctionsWorker()
    app.configure_serialization(ensure_ascii=False)
    request = pb.OrchestratorRequest(instanceId="unicode-orchestrator")
    request.newEvents.extend([
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(
            "unicode_orchestrator", request.instanceId, shared_dumps(VALUE)),
    ])
    encoded = base64.b64encode(request.SerializeToString()).decode("ascii")
    result = worker.execute_orchestration_request(
        unicode_orchestrator._function._func.orchestrator_function, encoded)
    response = pb.OrchestratorResponse.FromString(base64.b64decode(result))
    completion = response.actions[0].completeOrchestration
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert completion.result.value == json.dumps(VALUE, ensure_ascii=False)


def test_activity_converter_uses_optin(configurable_codec):
    df.DFApp().configure_serialization(ensure_ascii=False)
    encoded = ActivityTriggerConverter.encode(VALUE, expected_type=None)
    assert encoded.type == "json"
    assert encoded.value == json.dumps(VALUE, ensure_ascii=False)
    for payload in (shared_dumps(VALUE), encoded.value):
        assert ActivityTriggerConverter.decode(
            meta.Datum(type="json", value=payload), trigger_metadata=None) == VALUE
    assert configurable_codec == [False]


@pytest.mark.parametrize("initial_ascii", [False, True])
@pytest.mark.parametrize("escaped_result", [False, True])
def test_replay_across_encoding_rollout_and_rollback(
        configurable_codec, monkeypatch, initial_ascii, escaped_result):
    df.DFApp().configure_serialization(ensure_ascii=initial_ascii)

    def unicode_replay(context):
        value = context.get_input()
        result = yield context.call_activity("echo", value)
        result = yield context.call_sub_orchestrator(
            "child", result, instance_id="unicode-child")
        context.set_custom_status(result)
        return result

    def execute(request):
        encoded = base64.b64encode(request.SerializeToString()).decode("ascii")
        result = DurableFunctionsWorker().execute_orchestration_request(unicode_replay, encoded)
        return pb.OrchestratorResponse.FromString(base64.b64decode(result))

    request = pb.OrchestratorRequest(instanceId="unicode-replay")
    request.newEvents.extend([
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(
            "unicode_replay", request.instanceId, shared_dumps(VALUE)),
    ])
    initial = execute(request)
    scheduled = initial.actions[0]
    assert scheduled.scheduleTask.input.value == json.dumps(VALUE, ensure_ascii=initial_ascii)
    request.pastEvents.extend(request.newEvents)
    request.ClearField("newEvents")
    request.pastEvents.append(helpers.new_task_scheduled_event(
        scheduled.id, "echo", scheduled.scheduleTask.input.value))
    result_payload = json.dumps(VALUE, ensure_ascii=escaped_result)
    request.newEvents.extend([
        helpers.new_orchestrator_started_event(),
        helpers.new_task_completed_event(scheduled.id, result_payload),
    ])

    # Simulate a fresh worker process deploying the other encoding policy.
    monkeypatch.setattr(serialization, "_ensure_ascii", None)
    monkeypatch.setattr(serialization, "_unicode_encoder", None)
    df.DFApp().configure_serialization(ensure_ascii=not initial_ascii)
    resumed = execute(request)
    assert len(resumed.actions) == 1
    child = resumed.actions[0]
    assert child.createSubOrchestration.input.value == json.dumps(
        VALUE, ensure_ascii=not initial_ascii)
    request.pastEvents.extend(request.newEvents)
    request.ClearField("newEvents")
    request.pastEvents.append(helpers.new_sub_orchestration_created_event(
        child.id, "child", "unicode-child", child.createSubOrchestration.input.value))
    request.newEvents.extend([
        helpers.new_orchestrator_started_event(),
        helpers.new_sub_orchestration_completed_event(child.id, result_payload),
    ])
    completed = execute(request)
    assert len(completed.actions) == 1
    completion = completed.actions[0].completeOrchestration
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    expected = json.dumps(VALUE, ensure_ascii=not initial_ascii)
    assert completion.result.value == expected
    assert completed.customStatus.value == expected


@pytest.mark.parametrize("use_async", [False, True])
async def test_clients_created_before_configuration_use_optin(
        configurable_codec, monkeypatch, use_async):
    client = (df.DurableFunctionsClient(CLIENT_CONFIG) if use_async
              else df.SyncDurableFunctionsClient(CLIENT_CONFIG))
    stub = Mock()
    stub.StartInstance = (AsyncMock if use_async else Mock)(
        return_value=pb.CreateInstanceResponse(instanceId="unicode-client"))
    stub.SignalEntity = (AsyncMock if use_async else Mock)(
        return_value=pb.SignalEntityResponse())
    if use_async:
        monkeypatch.setattr(client, "_get_stub", lambda: stub)
    else:
        monkeypatch.setattr(client, "_stub", stub)
    df.DFApp().configure_serialization(ensure_ascii=False)
    try:
        if use_async:
            await client.schedule_new_orchestration("echo", input=VALUE)
            await client.signal_entity(EntityInstanceId("counter", "key"), "set", VALUE)
        else:
            client.schedule_new_orchestration("echo", input=VALUE)
            client.signal_entity(EntityInstanceId("counter", "key"), "set", VALUE)
        assert stub.StartInstance.call_args.args[0].input.value == json.dumps(
            VALUE, ensure_ascii=False)
        assert stub.SignalEntity.call_args.args[0].input.value == json.dumps(
            VALUE, ensure_ascii=False)
        assert configurable_codec == [False, False]
    finally:
        if use_async:
            await client.close()
        else:
            client.close()


@pytest.mark.parametrize("native", [False, True])
def test_entity_state_result_and_actions_use_optin(configurable_codec, native):
    worker = DurableFunctionsWorker()
    df.DFApp().configure_serialization(ensure_ascii=False)

    def compat_entity(context):
        assert context.get_input() == VALUE
        assert context.get_state() == VALUE
        context.set_state(VALUE)
        context.set_result(VALUE)

    def native_entity(context, input):
        assert input == VALUE
        assert context.get_state() == VALUE
        context.set_state(VALUE)
        context.signal_entity(EntityInstanceId("other", "key"), "set", VALUE)
        return VALUE

    unicode_entity = native_entity if native else compat_entity

    request = pb.EntityBatchRequest(instanceId=f"@{unicode_entity.__name__}@key")
    request.entityState.value = shared_dumps(VALUE)
    request.operations.add(
        requestId="unicode-operation", operation="set",
        input=helpers.get_string_value(shared_dumps(VALUE)))
    encoded = base64.b64encode(request.SerializeToString()).decode("ascii")
    response = pb.EntityBatchResult.FromString(base64.b64decode(
        worker.execute_entity_batch_request(unicode_entity, encoded)))
    expected = json.dumps(VALUE, ensure_ascii=False)
    assert response.entityState.value == expected
    assert response.results[0].success.result.value == expected
    if native:
        assert response.actions[0].sendSignal.input.value == expected


@pytest.mark.parametrize("use_async", [False, True])
async def test_externalized_activity_bytes_and_threshold_use_optin(
        configurable_codec, monkeypatch, payload_store_factory, use_async):
    value = {"text": "\u0416" * 100}
    store = payload_store_factory(threshold_bytes=300)
    monkeypatch.setattr(payloads, "_payload_store", store)
    df.DFApp().configure_serialization(ensure_ascii=False)

    def activity(payload):
        return value

    async def async_activity(payload):
        return value

    wrapped = wrap_activity_payloads(async_activity if use_async else activity, "payload")
    input_payload = ActivityTriggerConverter.decode(
        meta.Datum(type="json", value="null"), trigger_metadata=None)
    result = await wrapped(input_payload) if use_async else wrapped(input_payload)
    encoded = ActivityTriggerConverter.encode(result, expected_type=None)
    assert encoded.value == json.dumps(value, ensure_ascii=False)
    assert store._blobs == {}
    larger = {"text": "\u0416" * 200}

    def large_activity(payload):
        return larger

    async def large_async_activity(payload):
        return larger

    wrapped = wrap_activity_payloads(
        large_async_activity if use_async else large_activity, "payload")
    result = await wrapped(input_payload) if use_async else wrapped(input_payload)
    encoded = ActivityTriggerConverter.encode(result, expected_type=None)
    token = json.loads(encoded.value)
    assert store.download(token) == json.dumps(larger, ensure_ascii=False).encode("utf-8")


def test_optin_with_actual_shared_codec():
    if "ensure_ascii" not in inspect.signature(shared_dumps).parameters:
        with pytest.raises(RuntimeError, match="requires an azure-functions SDK"):
            df.DFApp().configure_serialization(ensure_ascii=False)
        return
    df.DFApp().configure_serialization(ensure_ascii=False)
    for value in [VALUE, {"text": "\ud800\udc00\udfff\x7f"}]:
        encoded = serialization.serialize_json(value)
        assert encoded == shared_dumps(value, ensure_ascii=False)
        wire = pb.ActivityResponse(result=helpers.get_string_value(encoded)).SerializeToString()
        assert pb.ActivityResponse.FromString(wire).result.value == encoded
