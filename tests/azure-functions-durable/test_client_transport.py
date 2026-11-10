# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import grpc
import pytest
from google.protobuf.wrappers_pb2 import StringValue

from azure.durable_functions import DurableFunctionsClient, SyncDurableFunctionsClient
from durabletask.client import AsyncTaskHubGrpcClient, OrchestrationQuery, OrchestrationStatus, TaskHubGrpcClient
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.internal import orchestrator_service_pb2_grpc as rpc


def _config(address: str) -> str:
    return json.dumps({
        "rpcBaseUrl": address,
        "taskHubName": "TestHub",
        "connectionName": "TestStorage",
        "requiredQueryStringParameters": "code=test-key",
        "maxGrpcMessageSizeInBytes": 8388608,
    })


@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("address, expected", [
    ("http://127.0.0.1:8080/durabletask/", "http://127.0.0.1:8080"),
    ("https://host:443/durabletask/?code=key#fragment", "https://host:443"),
    ("http://[::1]:8080/durabletask/", "http://[::1]:8080"),
    ("HTTP://localhost:8080/", "http://localhost:8080"),
    ("localhost:8080", "localhost:8080"),
    ("dns:///localhost:8080", "dns:///localhost:8080"),
    ("unix:///tmp/durabletask.sock", "unix:///tmp/durabletask.sock"),
    ("grpc://localhost:8080", "grpc://localhost:8080"),
    ("", ""),
])
def test_functions_client_normalizes_only_http_urls(use_async, address, expected):
    base = AsyncTaskHubGrpcClient if use_async else TaskHubGrpcClient
    client_type = DurableFunctionsClient if use_async else SyncDurableFunctionsClient
    with patch.object(base, "__init__", return_value=None) as init:
        client = client_type(_config(address))

    assert client.rpcBaseUrl == address
    assert client.connectionName == "TestStorage"
    kwargs = init.call_args.kwargs
    assert kwargs["host_address"] == expected
    assert kwargs["metadata"] is None
    assert kwargs["channel_options"].max_receive_message_length == 8388608
    assert kwargs["channel_options"].max_send_message_length == 8388608
    interceptor, = kwargs["interceptors"]
    assert ("taskhub", "TestHub") in interceptor._metadata
    assert interceptor.required_query_string_parameters == "code=test-key"


@pytest.fixture
def local_grpc_server():
    metadata = []

    class Service(rpc.TaskHubSidecarServiceServicer):
        def GetInstance(self, request, context):
            metadata.append(dict(context.invocation_metadata()))
            return pb.GetInstanceResponse(exists=False)

    with ThreadPoolExecutor(max_workers=1) as executor:
        server = grpc.server(executor)
        rpc.add_TaskHubSidecarServiceServicer_to_server(Service(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            yield f"http://127.0.0.1:{port}/durabletask/", metadata
        finally:
            server.stop(0).wait()


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_functions_client_reaches_grpc_server_with_url_path(local_grpc_server, use_async):
    address, metadata = local_grpc_server
    if use_async:
        client = DurableFunctionsClient(_config(address))
        try:
            response = await client._get_stub().GetInstance(
                pb.GetInstanceRequest(instanceId="instance"), timeout=2)
        finally:
            await client.close()
    else:
        client = SyncDurableFunctionsClient(_config(address))
        try:
            response = client._stub.GetInstance(
                pb.GetInstanceRequest(instanceId="instance"), timeout=2)
        finally:
            client.close()
    assert not response.exists
    assert metadata[0]["taskhub"] == "TestHub"
    assert metadata[0]["x-user-agent"].startswith("durabletask-python/")


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("page_size, expected", [
    ((1 << 31) - 1, 1000), (None, 1000), (1001, 1000),
    (1000, 1000), (25, 25), (0, 0),
])
async def test_functions_query_limits_page_size_without_changing_query(use_async, page_size, expected):
    query = OrchestrationQuery(
        created_time_from=datetime(2026, 1, 1, tzinfo=UTC),
        created_time_to=datetime(2026, 2, 1, tzinfo=UTC),
        runtime_status=[OrchestrationStatus.RUNNING],
        max_instance_count=page_size,
        fetch_inputs_and_outputs=True,
        instance_id_prefix="prefix",
        task_hub_names=["TestHub"],
    )
    stub = Mock()
    client = (DurableFunctionsClient(_config("localhost:8080")) if use_async
              else SyncDurableFunctionsClient(_config("localhost:8080")))
    response = pb.QueryInstancesResponse()
    try:
        if use_async:
            stub.QueryInstances = AsyncMock(return_value=response)
            with patch.object(client, "_get_stub", return_value=stub):
                assert await client.get_all_orchestration_states(query) == []
        else:
            stub.QueryInstances.return_value = response
            with patch.object(client, "_stub", stub):
                assert client.get_all_orchestration_states(query) == []
        request = stub.QueryInstances.call_args.args[0].query
        assert request.maxInstanceCount == expected
        assert request.fetchInputsAndOutputs
        assert request.instanceIdPrefix.value == "prefix"
        assert list(request.runtimeStatus) == [pb.ORCHESTRATION_STATUS_RUNNING]
        assert [hub.value for hub in request.taskHubNames] == ["TestHub"]
        assert request.createdTimeFrom.ToDatetime(UTC) == query.created_time_from
        assert request.createdTimeTo.ToDatetime(UTC) == query.created_time_to
        assert query.max_instance_count == page_size
    finally:
        if use_async:
            await client.close()
        else:
            client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("method, page_size", [
    ("async", None), ("sync", None), ("get_status_all", None),
    ("async", 25), ("sync", 25),
])
async def test_functions_query_reads_all_pages_including_empty_pages(method, page_size):
    first_page_count = page_size or 1000
    query = OrchestrationQuery(max_instance_count=page_size) if page_size else None
    responses = iter([
        pb.QueryInstancesResponse(
            orchestrationState=[
                pb.OrchestrationState(instanceId=str(i), name="test",
                                      orchestrationStatus=pb.ORCHESTRATION_STATUS_RUNNING)
                for i in range(first_page_count)
            ],
            continuationToken=StringValue(value="next")),
        pb.QueryInstancesResponse(continuationToken=StringValue(value="after-empty")),
        pb.QueryInstancesResponse(
            orchestrationState=[
                pb.OrchestrationState(instanceId=str(i), name="test",
                                      orchestrationStatus=pb.ORCHESTRATION_STATUS_RUNNING)
                for i in range(first_page_count, first_page_count + 2)
            ]),
    ])
    tokens = []

    def query_instances(request):
        assert request.query.maxInstanceCount == first_page_count
        tokens.append(request.query.continuationToken.value)
        assert request.query.fetchInputsAndOutputs == (method == "get_status_all")
        return next(responses)

    stub = Mock()
    if method == "sync":
        client = SyncDurableFunctionsClient(_config("localhost:8080"))
        stub.QueryInstances.side_effect = query_instances
        try:
            with patch.object(client, "_stub", stub):
                states = client.get_all_orchestration_states(query)
        finally:
            client.close()
    else:
        client = DurableFunctionsClient(_config("localhost:8080"))
        stub.QueryInstances = AsyncMock(side_effect=query_instances)
        try:
            with patch.object(client, "_get_stub", return_value=stub):
                if method == "get_status_all":
                    with pytest.warns(DeprecationWarning):
                        states = await client.get_status_all()
                else:
                    states = await client.get_all_orchestration_states(query)
        finally:
            await client.close()

    assert [state.instance_id for state in states] == [str(i) for i in range(first_page_count + 2)]
    assert tokens == ["", "next", "after-empty"]
