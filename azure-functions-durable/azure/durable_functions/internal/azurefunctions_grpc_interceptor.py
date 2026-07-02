# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from importlib.metadata import version

from durabletask.internal.grpc_interceptor import (
    DefaultAsyncClientInterceptorImpl,
    DefaultClientInterceptorImpl,
)


def _build_metadata(taskhub_name: str) -> list[tuple[str, str]]:
    """Build the gRPC metadata headers sent on every Durable Functions call."""
    try:
        # Get the version of the azurefunctions package
        sdk_version = version('durabletask-azurefunctions')
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


class AzureFunctionsAsyncDefaultClientInterceptorImpl(DefaultAsyncClientInterceptorImpl):
    """Async version of AzureFunctionsDefaultClientInterceptorImpl for use with grpc.aio channels.

    This class implements async gRPC interceptors to add Durable Functions headers
    (task hub name and user agent) to all async calls."""
    required_query_string_parameters: str

    def __init__(self, taskhub_name: str, required_query_string_parameters: str):
        self.required_query_string_parameters = required_query_string_parameters
        self._metadata = _build_metadata(taskhub_name)
        super().__init__(self._metadata)
