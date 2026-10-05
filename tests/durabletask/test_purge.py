# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest
from google.protobuf import wrappers_pb2

import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask.client import AsyncTaskHubGrpcClient, PurgeInstancesResult, TaskHubGrpcClient


@pytest.mark.parametrize("recursive", [None, True, False])
@pytest.mark.parametrize("is_complete", [None, True, False])
def test_sync_purge_orchestration_request_and_result(
        recursive: bool | None, is_complete: bool | None) -> None:
    response = pb.PurgeInstancesResponse(deletedInstanceCount=3)
    if is_complete is not None:
        response.isComplete.CopyFrom(wrappers_pb2.BoolValue(value=is_complete))
    stub = MagicMock()
    stub.PurgeInstances.return_value = response

    with patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub):
        with TaskHubGrpcClient(channel=MagicMock()) as client:
            if recursive is None:
                result = client.purge_orchestration("instance")
            else:
                result = client.purge_orchestration("instance", recursive=recursive)

    stub.PurgeInstances.assert_called_once()
    request = stub.PurgeInstances.call_args.args[0]
    assert request.instanceId == "instance"
    assert request.recursive is (True if recursive is None else recursive)
    assert request.isOrchestration is True
    assert not request.HasField("purgeInstanceFilter")
    assert result == PurgeInstancesResult(deleted_instance_count=3, is_complete=is_complete)


@pytest.mark.asyncio
@pytest.mark.parametrize("recursive", [None, True, False])
@pytest.mark.parametrize("is_complete", [None, True, False])
async def test_async_purge_orchestration_request_and_result(
        recursive: bool | None, is_complete: bool | None) -> None:
    response = pb.PurgeInstancesResponse(deletedInstanceCount=3)
    if is_complete is not None:
        response.isComplete.CopyFrom(wrappers_pb2.BoolValue(value=is_complete))
    stub = MagicMock()
    stub.PurgeInstances = AsyncMock(return_value=response)

    with patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub):
        async with AsyncTaskHubGrpcClient(channel=MagicMock()) as client:
            if recursive is None:
                result = await client.purge_orchestration("instance")
            else:
                result = await client.purge_orchestration("instance", recursive=recursive)

    stub.PurgeInstances.assert_awaited_once()
    request = stub.PurgeInstances.call_args.args[0]
    assert request.instanceId == "instance"
    assert request.recursive is (True if recursive is None else recursive)
    assert request.isOrchestration is True
    assert not request.HasField("purgeInstanceFilter")
    assert result == PurgeInstancesResult(deleted_instance_count=3, is_complete=is_complete)


def test_sync_purge_orchestration_propagates_rpc_error() -> None:
    error = grpc.RpcError("purge failed")
    stub = MagicMock()
    stub.PurgeInstances.side_effect = error

    with patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub):
        with TaskHubGrpcClient(channel=MagicMock()) as client:
            with pytest.raises(grpc.RpcError) as raised:
                client.purge_orchestration("instance")

    assert raised.value is error
    stub.PurgeInstances.assert_called_once()


@pytest.mark.asyncio
async def test_async_purge_orchestration_propagates_rpc_error() -> None:
    error = grpc.RpcError("purge failed")
    stub = MagicMock()
    stub.PurgeInstances = AsyncMock(side_effect=error)

    with patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub):
        async with AsyncTaskHubGrpcClient(channel=MagicMock()) as client:
            with pytest.raises(grpc.RpcError) as raised:
                await client.purge_orchestration("instance")

    assert raised.value is error
    stub.PurgeInstances.assert_awaited_once()
