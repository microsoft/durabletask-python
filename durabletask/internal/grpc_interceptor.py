# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from collections import namedtuple
from typing import List, Tuple

import grpc


class _ClientCallDetails(
        namedtuple(
            '_ClientCallDetails',
            ['method', 'timeout', 'metadata', 'credentials', 'wait_for_ready', 'compression']),
        grpc.ClientCallDetails):
    """This is an implementation of the ClientCallDetails interface needed for interceptors.
    This class takes six named values and inherits the ClientCallDetails from grpc package.
    This class encloses the values that describe a RPC to be invoked.
    """
    pass

def _intercept_call(
            client_call_details: _ClientCallDetails, metadata: List[Tuple[str, str]]) -> _ClientCallDetails:
    """Internal intercept_call implementation which adds metadata to grpc metadata in the RPC
        call details."""
    new_metadata = []
    new_client_call_details = client_call_details
    if client_call_details.metadata is not None:
        new_metadata = list(metadata)
    if metadata is not None:
        new_metadata.extend(metadata)
        new_client_call_details = _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            metadata=new_metadata,
            credentials=client_call_details.credentials,
            wait_for_ready=client_call_details.wait_for_ready,
            compression=client_call_details.compression,
        )
    return new_client_call_details

class UnaryUnaryClientInterceptorImpl (grpc.UnaryUnaryClientInterceptor):
    """The class implements a UnaryUnaryClientInterceptor from grpc to add an interceptor to add
    additional headers to all calls as needed."""

    def __init__(self, metadata: List[Tuple[str, str]]):
        super().__init__()
        self._metadata = metadata

    def intercept_unary_unary(self, continuation, client_call_details, request):
        new_client_call_details = _intercept_call(client_call_details, self._metadata)
        return continuation(new_client_call_details, request)

class UnaryStreamClientInterceptorImpl (grpc.UnaryStreamClientInterceptor):
    """The class implements a UnaryStreamClientInterceptor from grpc to add an interceptor to add
    additional headers to all calls as needed."""

    def __init__(self, metadata: List[Tuple[str, str]]):
        super().__init__()
        self._metadata = metadata

    def intercept_unary_stream(self, continuation, client_call_details, request):
        # client_call_details = client_call_details._replace(metadata=self._metadata)
        # return continuation(client_call_details, request)
        new_client_call_details = _intercept_call(client_call_details, self._metadata)
        return continuation(new_client_call_details, request)

class StreamUnaryClientInterceptorImpl (grpc.StreamUnaryClientInterceptor):
    """The class implements a StreamUnaryClientInterceptor from grpc to add an interceptor to add
    additional headers to all calls as needed."""

    def __init__(self, metadata: List[Tuple[str, str]]):
        super().__init__()
        self._metadata = metadata

    def intercept_stream_unary(self, continuation, client_call_details, request):
        # client_call_details = client_call_details._replace(metadata=self._metadata)
        # return continuation(client_call_details, request)
        new_client_call_details = _intercept_call(client_call_details, self._metadata)
        return continuation(new_client_call_details, request)
    
class StreamStreamClientInterceptorImpl (grpc.StreamStreamClientInterceptor):
    """The class implements a StreamStreamClientInterceptor from grpc to add an interceptor to add
    additional headers to all calls as needed."""

    def __init__(self, metadata: List[Tuple[str, str]]):
        super().__init__()
        self._metadata = metadata

    def intercept_stream_stream(self, continuation, client_call_details, request):
        # client_call_details = client_call_details._replace(metadata=self._metadata)
        # return continuation(client_call_details, request)
        new_client_call_details = _intercept_call(client_call_details, self._metadata)
        return continuation(new_client_call_details, request)
