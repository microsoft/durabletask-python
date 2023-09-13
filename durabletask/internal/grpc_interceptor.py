# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Any, Dict, Optional

import grpc


class UnaryUnaryClientInterceptorImpl (grpc.UnaryUnaryClientInterceptor):
    """The class implements a UnaryUnaryClientInterceptor from grpc to add an interceptor to add
    additional headers to all calls as needed."""

    def __init__(self, metadata: Optional[Dict[str, Any]]):
        super().__init__()
        self._metadata = metadata

    def intercept_unary_unary(self, continuation, client_call_details, request):
        if self._metadata:
            client_call_details = client_call_details._replace(metadata=self._metadata)
        return continuation(client_call_details, request)

class UnaryStreamClientInterceptorImpl (grpc.UnaryStreamClientInterceptor):
    """The class implements a UnaryStreamClientInterceptor from grpc to add an interceptor to add
    additional headers to all calls as needed."""

    def __init__(self, metadata: Optional[Dict[str, Any]]):
        super().__init__()
        self._metadata = metadata

    def intercept_unary_stream(self, continuation, client_call_details, request):
        client_call_details = client_call_details._replace(metadata=self._metadata)
        return continuation(client_call_details, request)

class StreamUnaryClientInterceptorImpl (grpc.StreamUnaryClientInterceptor):
    """The class implements a StreamUnaryClientInterceptor from grpc to add an interceptor to add
    additional headers to all calls as needed."""

    def __init__(self, metadata: Optional[Dict[str, Any]]):
        super().__init__()
        self._metadata = metadata

    def intercept_stream_unary(self, continuation, client_call_details, request):
        client_call_details = client_call_details._replace(metadata=self._metadata)
        return continuation(client_call_details, request)
    
class StreamStreamClientInterceptorImpl (grpc.StreamStreamClientInterceptor):
    """The class implements a StreamStreamClientInterceptor from grpc to add an interceptor to add
    additional headers to all calls as needed."""

    def __init__(self, metadata: Optional[Dict[str, Any]]):
        super().__init__()
        self._metadata = metadata

    def intercept_stream_stream(self, continuation, client_call_details, request):
        client_call_details = client_call_details._replace(metadata=self._metadata)
        return continuation(client_call_details, request)
