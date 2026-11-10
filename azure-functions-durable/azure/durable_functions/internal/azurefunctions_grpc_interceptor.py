# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from importlib.metadata import version

import grpc
import grpc.aio

from durabletask.internal.grpc_interceptor import (
    DefaultAsyncClientInterceptorImpl,
    DefaultClientInterceptorImpl,
    _AsyncClientCallDetails,  # pyright: ignore[reportPrivateUsage]
    _ClientCallDetails,  # pyright: ignore[reportPrivateUsage]
    _MetadataLike,  # pyright: ignore[reportPrivateUsage]
)

from .invocation import get_invocation_id


def _with_invocation_id(metadata: _MetadataLike | None) -> _MetadataLike | None:
    invocation_id = get_invocation_id()
    if not invocation_id:
        return metadata
    key = "x-azure-functions-invocationid"
    # Resolve at call time: synchronous clients and their interceptors are cached.
    headers = [(name, value) for name, value in (metadata or ()) if name.lower() != key]
    headers.append((key, invocation_id))
    return headers


def _build_metadata(taskhub_name: str) -> list[tuple[str, str]]:
    """Build the gRPC metadata headers sent on every Durable Functions call."""
    try:
        # Get the installed version of the azure-functions-durable package
        sdk_version = version('azure-functions-durable')
    except Exception:
        # Fallback if version cannot be determined
        sdk_version = "unknown"
    user_agent = f"durabletask-python/{sdk_version}"
    return [
        ("taskhub", taskhub_name),
        ("x-user-agent", user_agent)]  # 'user-agent' is a reserved header in grpc, so we use 'x-user-agent' instead


class AzureFunctionsDefaultClientInterceptorImpl(DefaultClientInterceptorImpl):
    """The class implements a UnaryUnaryClientInterceptor, UnaryStreamClientInterceptor,
    StreamUnaryClientInterceptor and StreamStreamClientInterceptor from grpc to add an
    interceptor to add additional headers to all calls as needed."""
    required_query_string_parameters: str

    def __init__(self, taskhub_name: str, required_query_string_parameters: str):
        self.required_query_string_parameters = required_query_string_parameters
        self._metadata = _build_metadata(taskhub_name)
        super().__init__(self._metadata)

    def _intercept_call(self, client_call_details: grpc.ClientCallDetails) -> grpc.ClientCallDetails:
        details = super()._intercept_call(client_call_details)
        return _ClientCallDetails(
            details.method, details.timeout, _with_invocation_id(details.metadata),
            details.credentials, details.wait_for_ready, details.compression)


class AzureFunctionsAsyncDefaultClientInterceptorImpl(DefaultAsyncClientInterceptorImpl):
    """Async version of AzureFunctionsDefaultClientInterceptorImpl for use with grpc.aio channels.

    This class implements async gRPC interceptors to add Durable Functions headers
    (task hub name and user agent) to all async calls."""
    required_query_string_parameters: str

    def __init__(self, taskhub_name: str, required_query_string_parameters: str):
        self.required_query_string_parameters = required_query_string_parameters
        self._metadata = _build_metadata(taskhub_name)
        super().__init__(self._metadata)

    async def _intercept_call(self, client_call_details: grpc.aio.ClientCallDetails) -> grpc.aio.ClientCallDetails:
        details = await super()._intercept_call(client_call_details)
        return _AsyncClientCallDetails(
            details.method, details.timeout, _with_invocation_id(details.metadata),
            details.credentials, details.wait_for_ready)
