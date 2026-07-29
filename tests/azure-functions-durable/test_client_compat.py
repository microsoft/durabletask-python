# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import azure.functions as func
import pytest

import azure.durable_functions as df
from azure.durable_functions import RetryOptions
from azure.durable_functions.internal.compat.orchestration_runtime_status import (
    from_durabletask_status,
    to_durabletask_status,
    to_durabletask_statuses,
)
from durabletask.client import AsyncTaskHubGrpcClient, OrchestrationStatus
from durabletask.entities import EntityInstanceId
from durabletask.task import RetryPolicy


_CLIENT_CONFIG = json.dumps({
    "taskHubName": "TestHub",
    "requiredQueryStringParameters": "code=xyz",
    "baseUrl": "http://localhost:7071/runtime/webhooks/durabletask",
    "rpcBaseUrl": "http://localhost:8080/",
    "managementUrls": {"id": "INSTANCEID"},
})

_MANAGEMENT_QUERY = "taskHub=HostHub&connection=HostStorage&code=host-key"
_MANAGEMENT_URLS = {
    "id": "INSTANCEID",
    "statusQueryGetUri": (
        "http://internal-host/custom/manage/INSTANCEID?"
        f"{_MANAGEMENT_QUERY}"),
    "sendEventPostUri": (
        "http://internal-host/custom/manage/INSTANCEID/raiseEvent/{eventName}?"
        f"{_MANAGEMENT_QUERY}"),
    "terminatePostUri": (
        "http://internal-host/custom/manage/INSTANCEID/terminate?reason={text}&"
        f"{_MANAGEMENT_QUERY}"),
    "rewindPostUri": (
        "http://internal-host/custom/manage/INSTANCEID/rewind?reason={text}&"
        f"{_MANAGEMENT_QUERY}"),
    "purgeHistoryDeleteUri": (
        "http://internal-host/custom/manage/INSTANCEID?"
        f"{_MANAGEMENT_QUERY}"),
    "restartPostUri": (
        "http://internal-host/custom/manage/INSTANCEID/restart?"
        f"{_MANAGEMENT_QUERY}"),
    "suspendPostUri": (
        "http://internal-host/custom/manage/INSTANCEID/suspend?reason={text}&"
        f"{_MANAGEMENT_QUERY}"),
    "resumePostUri": (
        "http://internal-host/custom/manage/INSTANCEID/resume?reason={text}&"
        f"{_MANAGEMENT_QUERY}"),
}


def _make_client() -> df.DurableFunctionsClient:
    return df.DurableFunctionsClient(_CLIENT_CONFIG)


def _make_template_config() -> str:
    return json.dumps({
        "taskHubName": "TestHub",
        "requiredQueryStringParameters": "code=fallback-key",
        "baseUrl": "http://fallback/runtime/webhooks/durabletask",
        "rpcBaseUrl": "http://localhost:8080/",
        "managementUrls": _MANAGEMENT_URLS,
        "useForwardedHost": True,
    })


def _make_host_config(*, use_forwarded_host: bool = False) -> str:
    return json.dumps({
        "taskHubName": "TestHub",
        "requiredQueryStringParameters": "code=host-key",
        "httpBaseUrl": "http://host-internal/custom/durable",
        "rpcBaseUrl": "http://localhost:8080/",
        "useForwardedHost": use_forwarded_host,
    })


def test_client_handles_null_max_grpc_message_size():
    # The Durable Functions host may send ``maxGrpcMessageSizeInBytes``
    # explicitly as ``null`` (not just omit it). ``dict.get(key, 0)`` returns
    # ``None`` in that case, which previously blew up the ``> 0`` guard in
    # ``__init__`` (``TypeError: '>' not supported between 'NoneType' and
    # 'int'``) and made every durable-client binding decode fail with a 500.
    config = json.dumps({
        "taskHubName": "TestHub",
        "rpcBaseUrl": "http://localhost:8080/",
        "maxGrpcMessageSizeInBytes": None,
    })
    client = df.DurableFunctionsClient(config)
    assert client.maxGrpcMessageSizeInBytes == 0


def test_durable_clients_use_propagate_only_tracing():
    sync_client = df.SyncDurableFunctionsClient(_CLIENT_CONFIG)
    try:
        assert sync_client.emit_trace_spans is False
    finally:
        sync_client.close()

    with patch.object(AsyncTaskHubGrpcClient, "__init__", return_value=None) as init:
        df.DurableFunctionsClient(_CLIENT_CONFIG)

    assert init.call_args is not None
    assert init.call_args.kwargs["emit_trace_spans"] is False


def test_client_handles_all_config_fields_sent_as_null():
    # Newer host extension bundles serialize the full client configuration and
    # can send any field explicitly as ``null``. Every field must collapse to
    # its (falsy) default rather than ``None`` so construction never crashes and
    # downstream code sees well-typed values.
    config = json.dumps({
        "taskHubName": None,
        "connectionName": None,
        "creationUrls": None,
        "managementUrls": None,
        "baseUrl": None,
        "requiredQueryStringParameters": None,
        "rpcBaseUrl": None,
        "httpBaseUrl": None,
        "useForwardedHost": None,
        "maxGrpcMessageSizeInBytes": None,
        "grpcHttpClientTimeout": None,
    })
    client = df.DurableFunctionsClient(config)
    assert client.taskHubName == ""
    assert client.connectionName == ""
    assert client.creationUrls == {}
    assert client.managementUrls == {}
    assert client.baseUrl == ""
    assert client.requiredQueryStringParameters == ""
    assert client.rpcBaseUrl == ""
    assert client.httpBaseUrl == ""
    assert client.useForwardedHost is False
    assert client.maxGrpcMessageSizeInBytes == 0
    assert client.grpcHttpClientTimeout == timedelta(seconds=30)


# ---------------------------------------------------------------------------
# RetryOptions shim
# ---------------------------------------------------------------------------

def test_retry_options_is_retry_policy_subclass():
    assert issubclass(RetryOptions, RetryPolicy)


def test_retry_options_maps_milliseconds_to_timedelta():
    with pytest.warns(DeprecationWarning):
        options = RetryOptions(
            first_retry_interval_in_milliseconds=1500,
            max_number_of_attempts=3)
    assert options.first_retry_interval == timedelta(milliseconds=1500)
    assert options.max_number_of_attempts == 3
    assert options.first_retry_interval_in_milliseconds == 1500


def test_retry_options_rejects_non_positive_interval():
    with pytest.warns(DeprecationWarning):
        with pytest.raises(ValueError):
            RetryOptions(
                first_retry_interval_in_milliseconds=0,
                max_number_of_attempts=3)


def test_retry_policy_is_exported():
    assert df.RetryPolicy is RetryPolicy


# ---------------------------------------------------------------------------
# create_http_management_payload signature compatibility
# ---------------------------------------------------------------------------

async def test_create_http_management_payload_v1_signature():
    client = _make_client()
    try:
        payload = client.create_http_management_payload("inst1")
        assert payload.urls["id"] == "inst1"
        assert payload.urls["statusQueryGetUri"] == (
            "http://localhost:7071/runtime/webhooks/durabletask/instances/inst1?code=xyz")
    finally:
        await client.close()


async def test_create_http_management_payload_v2_signature():
    client = _make_client()
    try:
        request = func.HttpRequest(
            method="POST", url="http://localhost:7071/api/start", body=b"")
        payload = client.create_http_management_payload(request, "inst2")
        assert payload.urls["id"] == "inst2"
        assert payload.urls["statusQueryGetUri"] == (
            "http://localhost:7071/runtime/webhooks/durabletask/instances/inst2?code=xyz")
    finally:
        await client.close()


async def test_create_http_management_payload_requires_instance_id():
    client = _make_client()
    try:
        with pytest.raises(TypeError):
            client.create_http_management_payload()
    finally:
        await client.close()


@pytest.mark.parametrize(
    ("headers", "expected_origin"),
    [
        ({}, "http://request-internal:7071"),
        ({"Forwarded": 'for=10.0.0.1;proto=https;host="public.example:8443"'},
         "https://public.example:8443"),
        ({"X-Forwarded-Proto": "https", "X-Forwarded-Host": "proxy.example"},
         "https://proxy.example"),
    ],
)
async def test_management_payload_uses_host_templates_and_external_origin(
        headers, expected_origin):
    config = _make_template_config()
    async_client = df.DurableFunctionsClient(config)
    sync_client = df.SyncDurableFunctionsClient(config)
    request = func.HttpRequest(
        method="POST",
        url="http://request-internal:7071/api/start",
        headers=headers,
        body=b"")
    instance_id = "folder/instance ?"
    encoded_instance_id = "folder%2Finstance%20%3F"

    try:
        async_payload = async_client.create_http_management_payload(
            request, instance_id)
        sync_payload = sync_client.create_http_management_payload(
            request, instance_id)

        assert async_payload == sync_payload
        assert async_payload["id"] == instance_id
        assert async_payload["statusQueryGetUri"] == (
            f"{expected_origin}/custom/manage/{encoded_instance_id}?"
            f"{_MANAGEMENT_QUERY}")
        assert async_payload["sendEventPostUri"] == (
            f"{expected_origin}/custom/manage/{encoded_instance_id}/"
            f"raiseEvent/{{eventName}}?{_MANAGEMENT_QUERY}")
        assert async_payload["terminatePostUri"] == (
            f"{expected_origin}/custom/manage/{encoded_instance_id}/"
            f"terminate?reason={{text}}&{_MANAGEMENT_QUERY}")
        assert async_payload["rewindPostUri"] == (
            f"{expected_origin}/custom/manage/{encoded_instance_id}/"
            f"rewind?reason={{text}}&{_MANAGEMENT_QUERY}")
        assert async_payload["purgeHistoryDeleteUri"] == (
            async_payload["statusQueryGetUri"])
        assert async_payload["restartPostUri"] == (
            f"{expected_origin}/custom/manage/{encoded_instance_id}/"
            f"restart?{_MANAGEMENT_QUERY}")
        assert async_payload["suspendPostUri"] == (
            f"{expected_origin}/custom/manage/{encoded_instance_id}/"
            f"suspend?reason={{text}}&{_MANAGEMENT_QUERY}")
        assert async_payload["resumePostUri"] == (
            f"{expected_origin}/custom/manage/{encoded_instance_id}/"
            f"resume?reason={{text}}&{_MANAGEMENT_QUERY}")
        assert async_payload.urls == async_payload.to_json()

        async_response = async_client.create_check_status_response(
            request, instance_id)
        sync_response = sync_client.create_check_status_response(
            request, instance_id)
        assert json.loads(async_response.get_body()) == async_payload
        assert json.loads(sync_response.get_body()) == sync_payload
        assert json.loads(async_response.get_body())["rewindPostUri"] == (
            async_payload["rewindPostUri"])
    finally:
        await async_client.close()
        sync_client.close()


async def test_management_payload_without_request_preserves_template_origin():
    client = df.DurableFunctionsClient(_make_template_config())
    try:
        payload = client.create_http_management_payload("instance")
        assert payload["statusQueryGetUri"] == (
            f"http://internal-host/custom/manage/instance?{_MANAGEMENT_QUERY}")
    finally:
        await client.close()


async def test_host_config_uses_http_base_url_and_ignores_untrusted_forwarding():
    client = df.DurableFunctionsClient(_make_host_config())
    request = func.HttpRequest(
        method="POST",
        url="http://request-internal:7071/api/start",
        headers={
            "X-Forwarded-Proto": "https",
            "X-Forwarded-Host": "attacker.example",
        },
        body=b"")

    try:
        payload = client.create_http_management_payload(
            request, "folder/instance")
        assert payload["statusQueryGetUri"] == (
            "http://request-internal:7071/custom/durable/instances/"
            "folder%2Finstance?code=host-key")
    finally:
        await client.close()


async def test_host_config_honors_forwarding_when_enabled():
    client = df.DurableFunctionsClient(
        _make_host_config(use_forwarded_host=True))
    request = func.HttpRequest(
        method="POST",
        url="http://request-internal:7071/api/start",
        headers={
            "Forwarded": "proto=https;host=public.example",
        },
        body=b"")

    try:
        payload = client.create_http_management_payload(request, "instance")
        assert payload["statusQueryGetUri"] == (
            "https://public.example/custom/durable/instances/"
            "instance?code=host-key")
    finally:
        await client.close()


async def test_host_config_without_request_uses_http_base_url():
    client = df.DurableFunctionsClient(_make_host_config())
    try:
        payload = client.create_http_management_payload("instance")
        assert payload["statusQueryGetUri"] == (
            "http://host-internal/custom/durable/instances/"
            "instance?code=host-key")
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Deprecated client method aliases
# ---------------------------------------------------------------------------

async def test_start_new_delegates_to_schedule_new_orchestration():
    client = _make_client()
    try:
        with patch.object(client, "schedule_new_orchestration",
                          new=AsyncMock(return_value="new-id")) as mock:
            with pytest.warns(DeprecationWarning):
                result = await client.start_new(
                    "MyOrchestrator", instance_id="abc", client_input={"x": 1})
        assert result == "new-id"
        mock.assert_awaited_once_with(
            "MyOrchestrator", input={"x": 1}, instance_id="abc", version=None)
    finally:
        await client.close()


async def test_get_status_delegates_to_get_orchestration_state():
    client = _make_client()
    try:
        with patch.object(client, "get_orchestration_state",
                          new=AsyncMock(return_value=None)) as mock:
            with pytest.warns(DeprecationWarning):
                await client.get_status("abc", show_input=True)
        mock.assert_awaited_once_with("abc", fetch_payloads=True)
    finally:
        await client.close()


async def test_get_status_all_delegates():
    client = _make_client()
    try:
        with patch.object(client, "get_all_orchestration_states",
                          new=AsyncMock(return_value=[])) as mock:
            with pytest.warns(DeprecationWarning):
                await client.get_status_all()
        mock.assert_awaited_once_with()
    finally:
        await client.close()


async def test_raise_event_delegates():
    client = _make_client()
    try:
        with patch.object(client, "raise_orchestration_event",
                          new=AsyncMock()) as mock:
            with pytest.warns(DeprecationWarning):
                await client.raise_event("abc", "evt", event_data={"k": "v"})
        mock.assert_awaited_once_with("abc", "evt", data={"k": "v"})
    finally:
        await client.close()


async def test_terminate_delegates():
    client = _make_client()
    try:
        with patch.object(client, "terminate_orchestration",
                          new=AsyncMock()) as mock:
            with pytest.warns(DeprecationWarning):
                await client.terminate("abc", "because")
        mock.assert_awaited_once_with("abc", output="because")
    finally:
        await client.close()


async def test_purge_instance_history_delegates():
    client = _make_client()
    try:
        with patch.object(client, "purge_orchestration",
                          new=AsyncMock()) as mock:
            with pytest.warns(DeprecationWarning):
                await client.purge_instance_history("abc")
        mock.assert_awaited_once_with("abc")
    finally:
        await client.close()


async def test_suspend_resume_delegate():
    client = _make_client()
    try:
        with patch.object(client, "suspend_orchestration",
                          new=AsyncMock()) as suspend_mock:
            with pytest.warns(DeprecationWarning):
                await client.suspend("abc", "reason")
        suspend_mock.assert_awaited_once_with("abc")

        with patch.object(client, "resume_orchestration",
                          new=AsyncMock()) as resume_mock:
            with pytest.warns(DeprecationWarning):
                await client.resume("abc", "reason")
        resume_mock.assert_awaited_once_with("abc")
    finally:
        await client.close()


async def test_restart_delegates():
    client = _make_client()
    try:
        with patch.object(client, "restart_orchestration",
                          new=AsyncMock(return_value="abc")) as mock:
            with pytest.warns(DeprecationWarning):
                await client.restart("abc")
        mock.assert_awaited_once_with("abc", restart_with_new_instance_id=True)
    finally:
        await client.close()


async def test_read_entity_state_delegates_to_get_entity():
    client = _make_client()
    try:
        with patch.object(client, "get_entity",
                          new=AsyncMock(return_value=None)) as mock:
            with pytest.warns(DeprecationWarning):
                await client.read_entity_state("@counter@one")
        mock.assert_awaited_once_with("@counter@one")
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# OrchestrationRuntimeStatus mapping
# ---------------------------------------------------------------------------

def test_orchestration_runtime_status_is_exported():
    assert df.OrchestrationRuntimeStatus.Running.value == "Running"


def test_to_durabletask_status_maps_known_values():
    assert to_durabletask_status(
        df.OrchestrationRuntimeStatus.Running) == OrchestrationStatus.RUNNING
    assert to_durabletask_status(
        df.OrchestrationRuntimeStatus.ContinuedAsNew) == OrchestrationStatus.CONTINUED_AS_NEW


def test_to_durabletask_status_rejects_canceled():
    with pytest.raises(ValueError):
        to_durabletask_status(df.OrchestrationRuntimeStatus.Canceled)


def test_to_durabletask_statuses_preserves_none():
    assert to_durabletask_statuses(None) is None
    assert to_durabletask_statuses(
        [df.OrchestrationRuntimeStatus.Failed]) == [OrchestrationStatus.FAILED]


async def test_get_status_by_maps_statuses():
    client = _make_client()
    try:
        with patch.object(client, "get_all_orchestration_states",
                          new=AsyncMock(return_value=[])) as mock:
            with pytest.warns(DeprecationWarning):
                await client.get_status_by(
                    runtime_status=[df.OrchestrationRuntimeStatus.Running])
        query = mock.await_args.args[0]
        assert query.runtime_status == [OrchestrationStatus.RUNNING]
    finally:
        await client.close()


async def test_purge_instance_history_by_maps_statuses():
    client = _make_client()
    try:
        with patch.object(client, "purge_orchestrations_by",
                          new=AsyncMock()) as mock:
            with pytest.warns(DeprecationWarning):
                await client.purge_instance_history_by(
                    runtime_status=[df.OrchestrationRuntimeStatus.Completed])
        assert mock.await_args.kwargs["runtime_status"] == [OrchestrationStatus.COMPLETED]
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# signal_entity v1 keyword compatibility
# ---------------------------------------------------------------------------

async def test_signal_entity_accepts_operation_input():
    client = _make_client()
    try:
        with patch.object(AsyncTaskHubGrpcClient, "signal_entity",
                          new=AsyncMock()) as mock:
            await client.signal_entity(
                "@counter@one", "add", operation_input=5, task_hub_name="hub")
        mock.assert_awaited_once_with(
            "@counter@one", "add", input=5, signal_time=None)
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# wait_for_completion_or_create_check_status_response
# ---------------------------------------------------------------------------

def _make_request() -> func.HttpRequest:
    return func.HttpRequest(
        method="GET", url="http://localhost:7071/api/status", body=b"")


async def test_wait_for_completion_returns_output_when_completed():
    client = _make_client()
    try:
        state = SimpleNamespace(
            runtime_status=OrchestrationStatus.COMPLETED,
            serialized_output='"done"')
        with patch.object(client, "wait_for_orchestration_completion",
                          new=AsyncMock(return_value=state)):
            with pytest.warns(DeprecationWarning):
                response = await client.wait_for_completion_or_create_check_status_response(
                    _make_request(), "abc")
        assert response.status_code == 200
        assert response.get_body() == b'"done"'
    finally:
        await client.close()


async def test_wait_for_completion_returns_check_status_on_timeout():
    client = _make_client()
    try:
        with patch.object(client, "wait_for_orchestration_completion",
                          new=AsyncMock(side_effect=TimeoutError)):
            with pytest.warns(DeprecationWarning):
                response = await client.wait_for_completion_or_create_check_status_response(
                    _make_request(), "abc")
        assert response.status_code == 202
    finally:
        await client.close()


async def test_wait_for_completion_surfaces_failure_details_when_failed():
    client = _make_client()
    try:
        # A failed orchestration carries its error in failure_details;
        # serialized_output is typically None. v1 returns the full status JSON
        # (runtimeStatus, instanceId, timestamps, output) for terminal states.
        state = SimpleNamespace(
            name="orch",
            instance_id="abc",
            created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            last_updated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            runtime_status=OrchestrationStatus.FAILED,
            serialized_input=None,
            serialized_output=None,
            serialized_custom_status=None,
            failure_details=SimpleNamespace(
                message="boom", error_type="ValueError", stack_trace="tb"))
        with patch.object(client, "wait_for_orchestration_completion",
                          new=AsyncMock(return_value=state)):
            with pytest.warns(DeprecationWarning):
                response = await client.wait_for_completion_or_create_check_status_response(
                    _make_request(), "abc")
        assert response.status_code == 500
        body = json.loads(response.get_body())
        assert body["runtimeStatus"] == "Failed"
        assert body["instanceId"] == "abc"
        # The failure message is surfaced under "output" so the error is not lost.
        assert body["output"] == "boom"
    finally:
        await client.close()


async def test_create_check_status_response_location_includes_query_string():
    client = _make_client()
    try:
        response = client.create_check_status_response(_make_request(), "abc")
        assert response.status_code == 202
        location = response.headers["Location"]
        # The Location must carry the required query string so a client that
        # follows it is authorized, and it matches the body's statusQueryGetUri.
        assert "code=xyz" in location
        body = json.loads(response.get_body())
        assert location == body["statusQueryGetUri"]
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# rewind
# ---------------------------------------------------------------------------

async def test_rewind_delegates_to_rewind_orchestration():
    client = _make_client()
    try:
        with patch.object(client, "rewind_orchestration",
                          new=AsyncMock()) as mock:
            with pytest.warns(DeprecationWarning):
                await client.rewind("abc", "reason")
        mock.assert_awaited_once_with("abc", reason="reason")
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# get_client_response_links
# ---------------------------------------------------------------------------

async def test_get_client_response_links_delegates():
    client = _make_client()
    try:
        with pytest.warns(DeprecationWarning):
            payload = client.get_client_response_links(None, "abc")
        assert payload.urls["id"] == "abc"
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Exported class aliases
# ---------------------------------------------------------------------------

def test_durable_orchestration_client_is_subclass():
    assert issubclass(df.DurableOrchestrationClient, df.DurableFunctionsClient)


def test_entity_id_maps_to_entity_instance_id():
    with pytest.warns(DeprecationWarning):
        entity_id = df.EntityId("Counter", "one")
    assert isinstance(entity_id, EntityInstanceId)
    assert entity_id.name == "counter"
    assert str(entity_id) == "@counter@one"


def test_entity_id_url_path():
    with pytest.warns(DeprecationWarning):
        entity_id = df.EntityId("Counter", "one")
    assert df.EntityId.get_entity_id_url_path(entity_id) == "entities/counter/one"


def test_managed_identity_token_source_shim():
    source = df.ManagedIdentityTokenSource("https://management.core.windows.net")
    assert source.resource == "https://management.core.windows.net"
    assert source.to_json()["kind"] == "AzureManagedIdentity"


def test_entity_class_raises_not_implemented():
    with pytest.warns(DeprecationWarning):
        with pytest.raises(NotImplementedError):
            df.Entity(lambda ctx: None)


# ---------------------------------------------------------------------------
# Return-type shims: DurableOrchestrationStatus
# ---------------------------------------------------------------------------

def _fake_state():
    return SimpleNamespace(
        name="orch",
        instance_id="abc",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        last_updated_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        runtime_status=OrchestrationStatus.RUNNING,
        serialized_input='{"in": 1}',
        serialized_output='{"out": 2}',
        serialized_custom_status='"cs"',
        get_input=lambda: {"in": 1},
        get_output=lambda: {"out": 2},
        get_custom_status=lambda: "cs",
    )


def test_from_durabletask_status_reverse_mapping():
    assert from_durabletask_status(OrchestrationStatus.RUNNING) == df.OrchestrationRuntimeStatus.Running
    assert from_durabletask_status(
        OrchestrationStatus.CONTINUED_AS_NEW) == df.OrchestrationRuntimeStatus.ContinuedAsNew


async def test_get_status_returns_wrapped_status():
    client = _make_client()
    try:
        with patch.object(client, "get_orchestration_state",
                          new=AsyncMock(return_value=_fake_state())):
            with pytest.warns(DeprecationWarning):
                status = await client.get_status("abc")
        assert bool(status) is True
        assert status.name == "orch"
        assert status.instance_id == "abc"
        assert status.runtime_status == df.OrchestrationRuntimeStatus.Running
        assert status.input_ == {"in": 1}
        assert status.output == {"out": 2}
        assert status.custom_status == "cs"
        assert status.to_json()["runtimeStatus"] == "Running"
    finally:
        await client.close()


async def test_get_status_missing_instance_is_falsy():
    client = _make_client()
    try:
        with patch.object(client, "get_orchestration_state",
                          new=AsyncMock(return_value=None)):
            with pytest.warns(DeprecationWarning):
                status = await client.get_status("missing")
        assert bool(status) is False
        assert status.runtime_status is None
        assert status.output is None
    finally:
        await client.close()


async def test_get_status_all_returns_wrapped_list():
    client = _make_client()
    try:
        with patch.object(client, "get_all_orchestration_states",
                          new=AsyncMock(return_value=[_fake_state()])):
            with pytest.warns(DeprecationWarning):
                statuses = await client.get_status_all()
        assert len(statuses) == 1
        assert statuses[0].runtime_status == df.OrchestrationRuntimeStatus.Running
    finally:
        await client.close()


async def test_get_status_by_returns_wrapped_list():
    client = _make_client()
    try:
        with patch.object(client, "get_all_orchestration_states",
                          new=AsyncMock(return_value=[_fake_state()])):
            with pytest.warns(DeprecationWarning):
                statuses = await client.get_status_by(
                    runtime_status=[df.OrchestrationRuntimeStatus.Running])
        assert statuses[0].instance_id == "abc"
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Return-type shims: PurgeHistoryResult
# ---------------------------------------------------------------------------

async def test_purge_instance_history_returns_purge_history_result():
    client = _make_client()
    try:
        result = SimpleNamespace(deleted_instance_count=3, is_complete=True)
        with patch.object(client, "purge_orchestration",
                          new=AsyncMock(return_value=result)):
            with pytest.warns(DeprecationWarning):
                purge = await client.purge_instance_history("abc")
        assert purge.instances_deleted == 3
    finally:
        await client.close()


async def test_purge_instance_history_by_returns_purge_history_result():
    client = _make_client()
    try:
        result = SimpleNamespace(deleted_instance_count=5, is_complete=True)
        with patch.object(client, "purge_orchestrations_by",
                          new=AsyncMock(return_value=result)):
            with pytest.warns(DeprecationWarning):
                purge = await client.purge_instance_history_by(
                    runtime_status=[df.OrchestrationRuntimeStatus.Completed])
        assert purge.instances_deleted == 5
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Return-type shims: EntityStateResponse
# ---------------------------------------------------------------------------

async def test_read_entity_state_wraps_metadata_when_present():
    client = _make_client()
    try:
        metadata = SimpleNamespace(
            includes_state=True, get_typed_state=lambda: {"count": 5})
        with patch.object(client, "get_entity",
                          new=AsyncMock(return_value=metadata)):
            with pytest.warns(DeprecationWarning):
                response = await client.read_entity_state("@counter@one")
        assert response.entity_exists is True
        assert response.entity_state == {"count": 5}
    finally:
        await client.close()


async def test_read_entity_state_when_missing():
    client = _make_client()
    try:
        with patch.object(client, "get_entity",
                          new=AsyncMock(return_value=None)):
            with pytest.warns(DeprecationWarning):
                response = await client.read_entity_state("@counter@one")
        assert response.entity_exists is False
        assert response.entity_state is None
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# HttpManagementPayload dict-like access
# ---------------------------------------------------------------------------

async def test_http_management_payload_is_mapping_like():
    client = _make_client()
    try:
        payload = client.create_http_management_payload("inst1")
        assert payload["id"] == "inst1"
        assert "statusQueryGetUri" in payload
        assert "rewindPostUri" in payload
        assert payload.urls["rewindPostUri"] == payload.to_json()["rewindPostUri"]
        assert "id" in list(payload.keys())
        assert dict(payload.items())["id"] == "inst1"
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# call_http
# ---------------------------------------------------------------------------

def test_call_http_schedules_sub_orchestrator():
    from unittest.mock import MagicMock

    from azure.durable_functions.http.builtin import (
        BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME,
    )
    from azure.durable_functions.internal.compat.orchestration_context import (
        DurableOrchestrationContext,
    )

    fake_ctx = MagicMock()
    adapter = DurableOrchestrationContext(fake_ctx)
    adapter.call_http("GET", "http://example.com")
    assert (fake_ctx.call_sub_orchestrator.call_args.args[0]
            == BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME)


def test_token_source_is_still_constructible():
    source = df.ManagedIdentityTokenSource("https://graph.microsoft.com")
    assert source.resource == "https://graph.microsoft.com"
