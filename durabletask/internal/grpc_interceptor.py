# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from collections.abc import Callable, Iterable, Sequence
from typing import Any, NamedTuple, cast

import grpc
import grpc.aio

_MetadataValue = str | bytes
_MetadataEntry = tuple[str, _MetadataValue]
_Metadata = Sequence[_MetadataEntry]
_MetadataLike = _Metadata | grpc.aio.Metadata


class _ClientCallDetailsBase(NamedTuple):
    method: Any
    timeout: Any
    metadata: Any
    credentials: Any
    wait_for_ready: Any
    compression: Any


class _ClientCallDetails(_ClientCallDetailsBase, grpc.ClientCallDetails):
    """This is an implementation of the ClientCallDetails interface needed for interceptors.
    This class takes six named values and inherits the ClientCallDetails from grpc package.
    This class encloses the values that describe a RPC to be invoked.
    """
    pass


class _AsyncClientCallDetailsBase(NamedTuple):
    method: Any
    timeout: Any
    metadata: Any
    credentials: Any
    wait_for_ready: Any


class _AsyncClientCallDetails(_AsyncClientCallDetailsBase, grpc.aio.ClientCallDetails):
    """This is an implementation of the aio ClientCallDetails interface needed for async interceptors.
    This class takes five named values and inherits the ClientCallDetails from grpc.aio package.
    This class encloses the values that describe a RPC to be invoked.
    """
    pass


def _apply_metadata(
        client_call_details: grpc.ClientCallDetails | grpc.aio.ClientCallDetails,
        metadata: _Metadata | None) -> _MetadataLike | None:
    """Shared logic for applying metadata to call details. Returns the updated metadata list."""
    existing_metadata = cast(_MetadataLike | None, client_call_details.metadata)
    if metadata is None:
        return existing_metadata

    if existing_metadata is not None:
        new_metadata = list(cast(Iterable[_MetadataEntry], existing_metadata))
    else:
        new_metadata: list[_MetadataEntry] = []

    new_metadata.extend(metadata)
    return new_metadata


class DefaultClientInterceptorImpl (
        grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor,
        grpc.StreamUnaryClientInterceptor, grpc.StreamStreamClientInterceptor):
    """The class implements a UnaryUnaryClientInterceptor, UnaryStreamClientInterceptor,
    StreamUnaryClientInterceptor and StreamStreamClientInterceptor from grpc to add an
    interceptor to add additional headers to all calls as needed."""

    def __init__(self, metadata: list[tuple[str, str]]):
        super().__init__()
        self._metadata = metadata

    def _intercept_call(
            self, client_call_details: grpc.ClientCallDetails) -> grpc.ClientCallDetails:
        """Internal intercept_call implementation which adds metadata to grpc metadata in the RPC
            call details."""
        new_metadata = _apply_metadata(client_call_details, self._metadata)
        if new_metadata is client_call_details.metadata:
            return client_call_details

        return _ClientCallDetails(
            client_call_details.method, client_call_details.timeout, new_metadata,
            client_call_details.credentials, client_call_details.wait_for_ready, client_call_details.compression)

    def intercept_unary_unary(
            self,
            continuation: Callable[[grpc.ClientCallDetails, Any], Any],
            client_call_details: grpc.ClientCallDetails,
            request: Any) -> Any:
        new_client_call_details = self._intercept_call(client_call_details)
        return continuation(new_client_call_details, request)

    def intercept_unary_stream(
            self,
            continuation: Callable[[grpc.ClientCallDetails, Any], Any],
            client_call_details: grpc.ClientCallDetails,
            request: Any) -> Any:
        new_client_call_details = self._intercept_call(client_call_details)
        return continuation(new_client_call_details, request)

    def intercept_stream_unary(
            self,
            continuation: Callable[[grpc.ClientCallDetails, Any], Any],
            client_call_details: grpc.ClientCallDetails,
            request_iterator: Any) -> Any:
        new_client_call_details = self._intercept_call(client_call_details)
        return continuation(new_client_call_details, request_iterator)

    def intercept_stream_stream(
            self,
            continuation: Callable[[grpc.ClientCallDetails, Any], Any],
            client_call_details: grpc.ClientCallDetails,
            request_iterator: Any) -> Any:
        new_client_call_details = self._intercept_call(client_call_details)
        return continuation(new_client_call_details, request_iterator)


class DefaultAsyncClientInterceptorImpl(
        grpc.aio.UnaryUnaryClientInterceptor, grpc.aio.UnaryStreamClientInterceptor,
        grpc.aio.StreamUnaryClientInterceptor, grpc.aio.StreamStreamClientInterceptor):
    """Async gRPC interceptor that adds metadata headers to all calls."""

    def __init__(self, metadata: list[tuple[str, str]]):
        self._metadata = metadata

    async def _intercept_call(
            self, client_call_details: grpc.aio.ClientCallDetails) -> grpc.aio.ClientCallDetails:
        """Internal intercept_call implementation which adds metadata to grpc metadata in the RPC
            call details. This method is async to allow subclasses to perform async operations
            (e.g., refreshing auth tokens) during interception."""
        new_metadata = _apply_metadata(client_call_details, self._metadata)
        if new_metadata is client_call_details.metadata:
            return client_call_details

        return _AsyncClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            new_metadata,
            client_call_details.credentials,
            client_call_details.wait_for_ready,
        )

    async def intercept_unary_unary(
            self,
            continuation: Callable[[grpc.aio.ClientCallDetails, Any], Any],
            client_call_details: grpc.aio.ClientCallDetails,
            request: Any) -> Any:
        new_client_call_details = await self._intercept_call(client_call_details)
        return await continuation(new_client_call_details, request)

    async def intercept_unary_stream(
            self,
            continuation: Callable[[grpc.aio.ClientCallDetails, Any], Any],
            client_call_details: grpc.aio.ClientCallDetails,
            request: Any) -> Any:
        new_client_call_details = await self._intercept_call(client_call_details)
        return await continuation(new_client_call_details, request)

    async def intercept_stream_unary(
            self,
            continuation: Callable[[grpc.aio.ClientCallDetails, Any], Any],
            client_call_details: grpc.aio.ClientCallDetails,
            request_iterator: Any) -> Any:
        new_client_call_details = await self._intercept_call(client_call_details)
        return await continuation(new_client_call_details, request_iterator)

    async def intercept_stream_stream(
            self,
            continuation: Callable[[grpc.aio.ClientCallDetails, Any], Any],
            client_call_details: grpc.aio.ClientCallDetails,
            request_iterator: Any) -> Any:
        new_client_call_details = await self._intercept_call(client_call_details)
        return await continuation(new_client_call_details, request_iterator)
