# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for internal infrastructure shims.

Covers the no-op sidecar stub used to satisfy the base worker's completion
callbacks, and the gRPC interceptors that inject the Durable Functions task-hub
and user-agent headers onto every client call.
"""

from unittest.mock import patch

from azure.durable_functions.internal.azurefunctions_grpc_interceptor import (
    AzureFunctionsAsyncDefaultClientInterceptorImpl,
    AzureFunctionsDefaultClientInterceptorImpl,
    _build_metadata,
)
from azure.durable_functions.internal.azurefunctions_null_stub import (
    AzureFunctionsNullStub,
)


# ---------------------------------------------------------------------------
# AzureFunctionsNullStub
# ---------------------------------------------------------------------------

def test_null_stub_any_method_is_a_noop():
    stub = AzureFunctionsNullStub()
    assert stub.CompleteOrchestratorTask("anything") is None
    assert stub.CompleteEntityTask(1, 2, key="value") is None
    # An arbitrary, never-defined attribute also resolves to a callable no-op.
    assert stub.SomeMethodThatDoesNotExist() is None


def test_null_stub_returns_callable_for_any_attribute():
    stub = AzureFunctionsNullStub()
    assert callable(stub.ArbitraryName)


# ---------------------------------------------------------------------------
# _build_metadata
# ---------------------------------------------------------------------------

def test_build_metadata_contains_taskhub_and_user_agent():
    metadata = _build_metadata("myhub")
    as_dict = dict(metadata)
    assert as_dict["taskhub"] == "myhub"
    assert as_dict["x-user-agent"].startswith("durabletask-python/")


def test_build_metadata_falls_back_when_version_lookup_fails():
    with patch(
        "azure.durable_functions.internal.azurefunctions_grpc_interceptor.version",
        side_effect=Exception("no package"),
    ):
        metadata = dict(_build_metadata("hub"))
    assert metadata["x-user-agent"] == "durabletask-python/unknown"


# ---------------------------------------------------------------------------
# Interceptors
# ---------------------------------------------------------------------------

def test_sync_interceptor_stores_metadata_and_query_params():
    interceptor = AzureFunctionsDefaultClientInterceptorImpl("hub", "code=abc")
    assert interceptor.required_query_string_parameters == "code=abc"
    assert dict(interceptor._metadata)["taskhub"] == "hub"


def test_async_interceptor_stores_metadata_and_query_params():
    interceptor = AzureFunctionsAsyncDefaultClientInterceptorImpl("hub", "code=xyz")
    assert interceptor.required_query_string_parameters == "code=xyz"
    assert dict(interceptor._metadata)["taskhub"] == "hub"
