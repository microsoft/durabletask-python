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


def _make_client() -> df.DurableFunctionsClient:
    return df.DurableFunctionsClient(_CLIENT_CONFIG)


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


# ---------------------------------------------------------------------------
# rewind (not implemented)
# ---------------------------------------------------------------------------

async def test_rewind_raises_not_implemented():
    client = _make_client()
    try:
        with pytest.warns(DeprecationWarning):
            with pytest.raises(NotImplementedError):
                await client.rewind("abc", "reason")
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


def test_managed_identity_token_source_shim():
    with pytest.warns(DeprecationWarning):
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
        assert "id" in list(payload.keys())
        assert dict(payload.items())["id"] == "inst1"
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# call_http not implemented
# ---------------------------------------------------------------------------

def test_call_http_raises_not_implemented():
    # call_http ignores self, so invoke via the class to avoid instantiating
    # the abstract context.
    with pytest.raises(NotImplementedError):
        df.DurableOrchestrationContext.call_http(None, "GET", "http://example.com")


def test_token_source_is_still_constructible():
    with pytest.warns(DeprecationWarning):
        source = df.ManagedIdentityTokenSource("https://graph.microsoft.com")
    assert source.resource == "https://graph.microsoft.com"
