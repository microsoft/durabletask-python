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

        use_managed_identity = False
        client_id = None
        
        # Check what authentication we are using
        if metadata:
            for key, value in metadata:
                if key.lower() == "use_managed_identity":
                    self.use_managed_identity = value.strip().lower() == "true"  # Convert to boolean
                elif key.lower() == "client_id":
                    self.client_id = value

        self._token_manager = AccessTokenManager(use_managed_identity=use_managed_identity,
                                                 client_id=client_id)

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
