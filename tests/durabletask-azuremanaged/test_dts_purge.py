# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from durabletask.azuremanaged.client import (
    AsyncDurableTaskSchedulerClient,
    DurableTaskSchedulerClient,
)
from durabletask.client import PurgeInstancesResult
from durabletask.internal import orchestrator_service_pb2 as pb


@pytest.mark.parametrize("recursive", [None, True, False])
def test_dts_purge_orchestration_request(recursive: bool | None) -> None:
    stub = MagicMock()
    stub.PurgeInstances.return_value = pb.PurgeInstancesResponse(deletedInstanceCount=3)

    with patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub):
        with DurableTaskSchedulerClient(
                host_address="localhost:4001", taskhub="hub",
                token_credential=None, channel=MagicMock()) as client:
            if recursive is None:
                result = client.purge_orchestration("instance")
            else:
                result = client.purge_orchestration("instance", recursive=recursive)

    stub.PurgeInstances.assert_called_once_with(pb.PurgeInstancesRequest(
        instanceId="instance", recursive=True if recursive is None else recursive,
        isOrchestration=True))
    assert result == PurgeInstancesResult(deleted_instance_count=3, is_complete=None)


@pytest.mark.asyncio
@pytest.mark.parametrize("recursive", [None, True, False])
async def test_async_dts_purge_orchestration_request(recursive: bool | None) -> None:
    stub = MagicMock()
    stub.PurgeInstances = AsyncMock(return_value=pb.PurgeInstancesResponse(deletedInstanceCount=3))

    with patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub):
        async with AsyncDurableTaskSchedulerClient(
                host_address="localhost:4001", taskhub="hub",
                token_credential=None, channel=MagicMock()) as client:
            if recursive is None:
                result = await client.purge_orchestration("instance")
            else:
                result = await client.purge_orchestration("instance", recursive=recursive)

    stub.PurgeInstances.assert_awaited_once_with(pb.PurgeInstancesRequest(
        instanceId="instance", recursive=True if recursive is None else recursive,
        isOrchestration=True))
    assert result == PurgeInstancesResult(deleted_instance_count=3, is_complete=None)
