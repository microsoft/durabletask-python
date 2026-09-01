# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.protobuf import wrappers_pb2

import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask.client import AsyncTaskHubGrpcClient, TaskHubGrpcClient


def test_sync_filtered_purge_serializes_timeout():
    stub = MagicMock()
    stub.PurgeInstances.return_value = pb.PurgeInstancesResponse(
        deletedInstanceCount=1,
        isComplete=wrappers_pb2.BoolValue(value=False),
    )

    with (
        patch("durabletask.client.shared.get_grpc_channel", return_value=MagicMock()),
        patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub),
    ):
        client = TaskHubGrpcClient()
        result = client.purge_orchestrations_by(timeout=timedelta(seconds=1, microseconds=500000))

    request = stub.PurgeInstances.call_args.args[0]
    assert request.purgeInstanceFilter.timeout.seconds == 1
    assert request.purgeInstanceFilter.timeout.nanos == 500000000
    assert result.is_complete is False


def test_sync_filtered_purge_omits_timeout_when_not_supplied():
    stub = MagicMock()
    stub.PurgeInstances.return_value = pb.PurgeInstancesResponse(
        isComplete=wrappers_pb2.BoolValue(value=True),
    )

    with (
        patch("durabletask.client.shared.get_grpc_channel", return_value=MagicMock()),
        patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub),
    ):
        client = TaskHubGrpcClient()
        client.purge_orchestrations_by()

    request = stub.PurgeInstances.call_args.args[0]
    assert not request.purgeInstanceFilter.HasField("timeout")


def test_sync_filtered_purge_preserves_unknown_completion_state():
    stub = MagicMock()
    stub.PurgeInstances.return_value = pb.PurgeInstancesResponse()

    with (
        patch("durabletask.client.shared.get_grpc_channel", return_value=MagicMock()),
        patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub),
    ):
        client = TaskHubGrpcClient()
        result = client.purge_orchestrations_by()

    assert result.is_complete is None


@pytest.mark.parametrize("timeout", [timedelta(), timedelta(seconds=-1)])
def test_sync_filtered_purge_rejects_non_positive_timeout(timeout):
    client = TaskHubGrpcClient(channel=MagicMock())

    with pytest.raises(ValueError, match="timeout must be greater than zero"):
        client.purge_orchestrations_by(timeout=timeout)


@pytest.mark.asyncio
async def test_async_filtered_purge_serializes_timeout():
    stub = MagicMock()
    stub.PurgeInstances = AsyncMock(return_value=pb.PurgeInstancesResponse(
        deletedInstanceCount=1,
        isComplete=wrappers_pb2.BoolValue(value=False),
    ))
    channel = MagicMock()
    channel.close = AsyncMock()

    with (
        patch("durabletask.client.shared.get_async_grpc_channel", return_value=channel),
        patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub),
    ):
        client = AsyncTaskHubGrpcClient()
        result = await client.purge_orchestrations_by(timeout=timedelta(milliseconds=250))
        await client.close()

    request = stub.PurgeInstances.call_args.args[0]
    assert request.purgeInstanceFilter.timeout.seconds == 0
    assert request.purgeInstanceFilter.timeout.nanos == 250000000
    assert result.is_complete is False


@pytest.mark.asyncio
async def test_async_filtered_purge_rejects_non_positive_timeout():
    client = AsyncTaskHubGrpcClient(channel=MagicMock())

    with pytest.raises(ValueError, match="timeout must be greater than zero"):
        await client.purge_orchestrations_by(timeout=timedelta())
