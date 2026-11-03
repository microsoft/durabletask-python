# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Capture real gRPC requests, including calls through Functions bindings."""

import asyncio
import inspect
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from uuid import uuid4

import grpc
import pytest

import azure.durable_functions as df
from azure.durable_functions import client as client_module
from azure.durable_functions.internal import invocation
from azure.durable_functions.internal.azurefunctions_grpc_interceptor import (
    AzureFunctionsAsyncDefaultClientInterceptorImpl,
    AzureFunctionsDefaultClientInterceptorImpl,
)
from durabletask.client import AsyncTaskHubGrpcClient, TaskHubGrpcClient
from durabletask.internal import orchestrator_service_pb2 as pb

_HEADER = "x-azure-functions-invocationid"


@pytest.fixture
def transport():
    received = []

    def handler(method, response_type):
        def receive(request, context):
            received.append((method, request.instanceId, list(context.invocation_metadata())))
            return response_type()
        return receive

    with ThreadPoolExecutor(max_workers=4) as executor:
        server = grpc.server(executor)
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
            "TaskHubSidecarService",
            {
                name: grpc.unary_unary_rpc_method_handler(
                    handler(name, response),
                    request_deserializer=request.FromString,
                    response_serializer=response.SerializeToString,
                )
                for name, request, response in (
                    ("TerminateInstance", pb.TerminateRequest, pb.TerminateResponse),
                    ("PurgeInstances", pb.PurgeInstancesRequest, pb.PurgeInstancesResponse),
                )
            },
        ),))
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        config = json.dumps({"rpcBaseUrl": f"http://127.0.0.1:{port}", "taskHubName": "TestHub"})
        try:
            yield config, received
        finally:
            cached = client_module._sync_client_cache.pop(config, None)
            if cached is not None:
                cached.close()
            server.stop(0).wait()


def _context(identifier):
    return SimpleNamespace(invocation_id=identifier, thread_local_storage=threading.local())


def _assert_headers(received, expected):
    assert received
    for _, instance_id, metadata in received:
        assert [value for key, value in metadata if key == _HEADER] == (
            [expected[instance_id]] if expected[instance_id] is not None else [])
        assert dict(metadata)["taskhub"] == "TestHub"
        assert dict(metadata)["x-user-agent"].startswith("durabletask-python/")


@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("explicit_context", [False, True])
async def test_bound_clients_use_current_invocation_on_real_transport(transport, use_async, explicit_context):
    config, received = transport
    app = df.DFApp()
    clients = []

    def sync_function(instance_id, client, **kwargs):
        assert ("context" in kwargs) == explicit_context
        if explicit_context:
            assert kwargs["context"].invocation_id == instance_id
        clients.append(client)
        client.terminate_orchestration(instance_id)
        client.purge_orchestration(instance_id)

    async def async_function(instance_id, client, **kwargs):
        assert ("context" in kwargs) == explicit_context
        if explicit_context:
            assert kwargs["context"].invocation_id == instance_id
        clients.append(client)
        await asyncio.sleep(0)
        await client.terminate_orchestration(instance_id)
        await client.purge_orchestration(instance_id)
        with pytest.warns(DeprecationWarning):
            await client.terminate(instance_id)
        with pytest.warns(DeprecationWarning):
            await client.purge_instance_history(instance_id)

    function = async_function if use_async else sync_function
    if explicit_context:
        parameters = list(inspect.signature(function).parameters.values())
        parameters.insert(-1, inspect.Parameter("context", inspect.Parameter.KEYWORD_ONLY))
        function.__signature__ = inspect.signature(function).replace(parameters=parameters)
    registered = app.durable_client_input(client_name="client")(function)._function._func
    assert inspect.iscoroutinefunction(registered) == use_async

    async def call(identifier):
        context = _context(identifier)
        if use_async:
            await registered(instance_id=identifier, client=config, context=context)
        else:
            await asyncio.to_thread(registered, instance_id=identifier, client=config, context=context)
        assert invocation.get_invocation_id() is None

    identifiers = [str(uuid4()) for _ in range(4)]
    await call(identifiers[0])
    await asyncio.gather(*(call(identifier) for identifier in identifiers[1:]))
    if not use_async:
        assert all(client is clients[0] for client in clients)
        # A cached client used after the function returns must not retain its ID.
        await asyncio.to_thread(clients[0].terminate_orchestration, "outside")
    expected = {identifier: identifier for identifier in identifiers}
    if not use_async:
        expected["outside"] = None
    _assert_headers(received, expected)
    assert len(received) == (16 if use_async else 9)


@pytest.mark.parametrize("use_async", [False, True])
async def test_reused_client_resolves_context_per_call_and_preserves_metadata(transport, use_async):
    config, received = transport
    address = json.loads(config)["rpcBaseUrl"]
    metadata = [("custom", "value"), (_HEADER, "stale")]
    interceptor_type = (AzureFunctionsAsyncDefaultClientInterceptorImpl if use_async
                        else AzureFunctionsDefaultClientInterceptorImpl)
    client_type = AsyncTaskHubGrpcClient if use_async else TaskHubGrpcClient
    client = client_type(
        host_address=address,
        interceptors=[interceptor_type("TestHub", "code=key")])
    stub = client._get_stub() if use_async else client._stub

    async def execute(instance_id):
        request = pb.TerminateRequest(instanceId=instance_id)
        if use_async:
            await stub.TerminateInstance(request, metadata=metadata)
        else:
            await invocation.run_sync(stub.TerminateInstance, request, metadata=metadata)

    registered = invocation.wrap_invocation(execute)
    expected = {}
    try:
        for _ in range(2):
            identifier = str(uuid4())
            expected[identifier] = identifier
            await registered(identifier, context=_context(identifier))
        identifiers = [str(uuid4()) for _ in range(2)]
        expected.update({identifier: identifier for identifier in identifiers})
        await asyncio.gather(*(registered(identifier, context=_context(identifier)) for identifier in identifiers))
        assert invocation.get_invocation_id() is None
        assert metadata == [("custom", "value"), (_HEADER, "stale")]
        _assert_headers(received, expected)
        assert all(dict(headers)["custom"] == "value" for _, _, headers in received)
    finally:
        if use_async:
            await client.close()
        else:
            client.close()


@pytest.mark.parametrize("use_async", [False, True])
async def test_no_context_omits_invocation_header(transport, use_async):
    config, received = transport
    client = df.DurableFunctionsClient(config) if use_async else df.SyncDurableFunctionsClient(config)
    try:
        if use_async:
            await client.terminate_orchestration("outside")
        else:
            await asyncio.to_thread(client.terminate_orchestration, "outside")
        _assert_headers(received, {"outside": None})
    finally:
        if use_async:
            await client.close()
        else:
            client.close()


@pytest.mark.parametrize("use_async", [False, True])
async def test_invocation_context_restored_after_failure(use_async):
    def fail(client):
        assert invocation.get_invocation_id() == identifier
        raise ValueError("function failed")

    async def async_fail(client):
        fail(client)

    identifier = str(uuid4())
    registered = df.DFApp().durable_client_input(client_name="client")(
        async_fail if use_async else fail)._function._func
    with pytest.raises(ValueError, match="function failed"):
        if use_async:
            await registered(client="{}", context=_context(identifier))
        else:
            registered(client="{}", context=_context(identifier))
    assert invocation.get_invocation_id() is None
