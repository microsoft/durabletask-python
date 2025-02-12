# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from durabletask.internal.grpc_interceptor import _ClientCallDetails, DefaultClientInterceptorImpl
from durabletask.azuremanaged.internal.access_token_manager import AccessTokenManager

import grpc

class DTSDefaultClientInterceptorImpl (DefaultClientInterceptorImpl):
    """The class implements a UnaryUnaryClientInterceptor, UnaryStreamClientInterceptor,
    StreamUnaryClientInterceptor and StreamStreamClientInterceptor from grpc to add an 
    interceptor to add additional headers to all calls as needed."""

    def __init__(self, metadata: list[tuple[str, str]]):
        super().__init__(metadata)
        self._token_manager = AccessTokenManager(metadata=self._metadata)

    def _intercept_call(
                self, client_call_details: _ClientCallDetails) -> grpc.ClientCallDetails:
        """Internal intercept_call implementation which adds metadata to grpc metadata in the RPC
            call details."""
        # Refresh the auth token if it is present and needed
        if self._metadata is not None:
            for i, (key, _) in enumerate(self._metadata):
                if key.lower() == "authorization":  # Ensure case-insensitive comparison
                    new_token = self._token_manager.get_access_token()  # Get the new token
                    self._metadata[i] = ("authorization", new_token)  # Update the token

        return super()._intercept_call(client_call_details)
