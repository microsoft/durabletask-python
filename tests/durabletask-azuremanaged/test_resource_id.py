# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock, call, patch

import grpc
import pytest
from azure.core.credentials import AccessToken

from durabletask.azuremanaged.client import (
    AsyncDurableTaskSchedulerClient,
    DurableTaskSchedulerClient,
)
from durabletask.azuremanaged.internal.access_token_manager import (
    AccessTokenManager,
    AsyncAccessTokenManager,
)
from durabletask.azuremanaged.preview.sandboxes.client import SandboxActivitiesClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker


_PUBLIC = "https://durabletask.io"
_GOVERNMENT = "https://durabletask.azure.us"
_RESOURCE_CASES = [
    (None, None, _PUBLIC),
    ("", None, _PUBLIC),
    ("westus2", None, _PUBLIC),
    ("chinaeast2", None, _PUBLIC),
    ("notusgov", None, _PUBLIC),
    ("notusdod", None, _PUBLIC),
    ("usgovvirginia", None, _GOVERNMENT),
    ("USGOVARIZONA", None, _GOVERNMENT),
    ("UsGovTexas", None, _GOVERNMENT),
    ("usdodcentral", None, _GOVERNMENT),
    ("USDODEAST", None, _GOVERNMENT),
    ("UsDodCentral", None, _GOVERNMENT),
    (None, "", _PUBLIC),
    ("usgovvirginia", "", _GOVERNMENT),
    ("usdodcentral", "", _GOVERNMENT),
    ("usgovvirginia", _PUBLIC, _PUBLIC),
    ("usdodcentral", _PUBLIC, _PUBLIC),
    ("westus2", _GOVERNMENT, _GOVERNMENT),
    ("chinaeast2", "https://durabletask.example", "https://durabletask.example"),
    (None, _GOVERNMENT + "/", _GOVERNMENT),
    (None, _GOVERNMENT + "/.default", _GOVERNMENT),
    (None, _GOVERNMENT + "//.default//", _GOVERNMENT),
    (None, " \t" + _GOVERNMENT + "/.default/ \t", _GOVERNMENT),
    ("usgovvirginia", "api://CustomAudience/resource/.DEFAULT/", "api://CustomAudience/resource"),
    (None, "api://custom/.default/.default", "api://custom/.default"),
]
_INVALID_RESOURCE_IDS = [" \t ", "///", "/.default", " /.DEFAULT/// "]
_EXPIRED = datetime.now(timezone.utc) - timedelta(hours=1)


def _set_region(monkeypatch: pytest.MonkeyPatch, region: str | None) -> None:
    if region is None:
        monkeypatch.delenv("REGION_NAME", raising=False)
    else:
        monkeypatch.setenv("REGION_NAME", region)


@pytest.mark.parametrize(("region", "resource_id", "expected_resource"), _RESOURCE_CASES)
def test_sync_token_scope_and_refresh(
        monkeypatch: pytest.MonkeyPatch, region: str | None,
        resource_id: str | None, expected_resource: str) -> None:
    _set_region(monkeypatch, region)
    token = AccessToken("test-token", 9999999999)
    credential = Mock()
    credential.get_token.return_value = token
    manager = AccessTokenManager(credential, resource_id=resource_id)

    credential.get_token.assert_not_called()
    assert manager.get_access_token() == token
    assert manager.get_access_token() == token
    credential.get_token.assert_called_once_with(f"{expected_resource}/.default")

    manager.expiry_time = _EXPIRED
    assert manager.get_access_token() == token
    assert credential.get_token.call_args_list == [call(f"{expected_resource}/.default")] * 2


@pytest.mark.parametrize(("region", "resource_id", "expected_resource"), _RESOURCE_CASES)
async def test_async_token_scope_and_refresh(
        monkeypatch: pytest.MonkeyPatch, region: str | None,
        resource_id: str | None, expected_resource: str) -> None:
    _set_region(monkeypatch, region)
    token = AccessToken("test-token", 9999999999)
    credential = Mock()
    credential.get_token = AsyncMock(return_value=token)
    manager = AsyncAccessTokenManager(credential, resource_id=resource_id)

    credential.get_token.assert_not_called()
    assert await manager.get_access_token() == token
    assert await manager.get_access_token() == token
    credential.get_token.assert_awaited_once_with(f"{expected_resource}/.default")

    manager.expiry_time = _EXPIRED
    assert await manager.get_access_token() == token
    assert credential.get_token.await_args_list == [call(f"{expected_resource}/.default")] * 2


@pytest.mark.parametrize("resource_id", _INVALID_RESOURCE_IDS)
@pytest.mark.parametrize("manager_type", [AccessTokenManager, AsyncAccessTokenManager])
def test_invalid_resource_ids_are_rejected_without_credentials(
        resource_id: str, manager_type: type[AccessTokenManager] | type[AsyncAccessTokenManager]) -> None:
    with pytest.raises(ValueError, match="resource_id cannot be empty after normalization"):
        manager_type(None, resource_id=resource_id)


async def test_region_default_is_resolved_per_manager_and_pinned_for_refresh(
        monkeypatch: pytest.MonkeyPatch) -> None:
    credential = Mock()
    credential.get_token.return_value = AccessToken("sync-token", 9999999999)
    async_credential = Mock()
    async_credential.get_token = AsyncMock(return_value=AccessToken("async-token", 9999999999))

    monkeypatch.setenv("REGION_NAME", "usgovvirginia")
    government = AccessTokenManager(credential)
    async_government = AsyncAccessTokenManager(async_credential)
    monkeypatch.setenv("REGION_NAME", "westus2")
    public = AccessTokenManager(credential)
    async_public = AsyncAccessTokenManager(async_credential)
    monkeypatch.setenv("REGION_NAME", "usdodcentral")

    for manager in (government, public):
        manager.get_access_token()
        manager.expiry_time = _EXPIRED
        manager.get_access_token()
    for async_manager in (async_government, async_public):
        await async_manager.get_access_token()
        async_manager.expiry_time = _EXPIRED
        await async_manager.get_access_token()

    expected_calls = [call(f"{_GOVERNMENT}/.default")] * 2 + [call(f"{_PUBLIC}/.default")] * 2
    assert credential.get_token.call_args_list == expected_calls
    assert async_credential.get_token.await_args_list == expected_calls


@pytest.mark.parametrize(("factory", "base_init", "is_async"), [
    (DurableTaskSchedulerClient, "durabletask.azuremanaged.client.TaskHubGrpcClient.__init__", False),
    (DurableTaskSchedulerWorker, "durabletask.azuremanaged.worker.TaskHubGrpcWorker.__init__", False),
    (AsyncDurableTaskSchedulerClient, "durabletask.azuremanaged.client.AsyncTaskHubGrpcClient.__init__", True),
    (SandboxActivitiesClient, "durabletask.azuremanaged.preview.sandboxes.transport.shared.get_grpc_channel", False),
])
@pytest.mark.parametrize(("region", "resource_id", "expected_resource"), [
    (None, None, _PUBLIC),
    ("UsGovVirginia", None, _GOVERNMENT),
    ("USDODEAST", "", _GOVERNMENT),
    ("usgovvirginia", " \t" + _PUBLIC + "/.DEFAULT/ \t", _PUBLIC),
    ("westus2", _GOVERNMENT, _GOVERNMENT),
    ("usgovvirginia", "api://custom/resource", "api://custom/resource"),
    ("westus2", "api://custom/.default/.default", "api://custom/.default"),
])
async def test_public_clients_and_worker_request_configured_scope(
        monkeypatch: pytest.MonkeyPatch, factory: Callable[..., object], base_init: str,
        is_async: bool, region: str | None, resource_id: str | None,
        expected_resource: str) -> None:
    _set_region(monkeypatch, region)
    token = AccessToken("configured-token", 9999999999)
    credential = Mock()
    credential.get_token = AsyncMock(return_value=token) if is_async else Mock(return_value=token)

    with patch(base_init) as init:
        init.return_value = Mock() if factory is SandboxActivitiesClient else None
        factory(
            host_address="localhost:4001",
            taskhub="test-hub",
            token_credential=credential,
            resource_id=resource_id,
        )

    credential.get_token.assert_not_called()
    assert init.call_args.kwargs["host_address"] == "localhost:4001"
    interceptor = init.call_args.kwargs["interceptors"][-1]
    details = Mock(
        spec=grpc.ClientCallDetails, method="/test", timeout=None, metadata=(),
        credentials=None, wait_for_ready=False, compression=None,
    )
    if is_async:
        result = await interceptor._intercept_call(details)
        credential.get_token.assert_awaited_once_with(f"{expected_resource}/.default")
    else:
        result = interceptor._intercept_call(details)
        credential.get_token.assert_called_once_with(f"{expected_resource}/.default")
    metadata = dict(result.metadata)
    assert metadata["taskhub"] == "test-hub"
    assert metadata["authorization"] == "Bearer configured-token"
    if factory is DurableTaskSchedulerWorker:
        assert metadata["workerid"]


@pytest.mark.parametrize("factory", [
    DurableTaskSchedulerClient, AsyncDurableTaskSchedulerClient,
    DurableTaskSchedulerWorker, SandboxActivitiesClient,
])
@pytest.mark.parametrize("resource_id", _INVALID_RESOURCE_IDS)
def test_public_constructors_reject_invalid_resource_without_credentials(
        factory: Callable[..., object], resource_id: str) -> None:
    with pytest.raises(ValueError, match="resource_id cannot be empty after normalization"):
        factory(host_address="localhost:4001", taskhub="test-hub",
                token_credential=None, resource_id=resource_id)
