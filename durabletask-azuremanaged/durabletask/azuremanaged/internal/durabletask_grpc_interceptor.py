# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from importlib.metadata import version
from typing import Optional

import grpc
from azure.core.credentials import TokenCredential

from durabletask.azuremanaged.internal.access_token_manager import AccessTokenManager
from durabletask.internal.grpc_interceptor import (
    DefaultClientInterceptorImpl,
    _ClientCallDetails,
)


class DTSDefaultClientInterceptorImpl (DefaultClientInterceptorImpl):
    """The class implements a UnaryUnaryClientInterceptor, UnaryStreamClientInterceptor,
    StreamUnaryClientInterceptor and StreamStreamClientInterceptor from grpc to add an
    interceptor to add additional headers to all calls as needed."""

    def __init__(self, token_credential: Optional[TokenCredential], taskhub_name: str):
        try:
            # Get the version of the azuremanaged package
            sdk_version = version('durabletask-azuremanaged')
        except Exception:
            # Fallback if version cannot be determined
            sdk_version = "unknown"
        user_agent = f"durabletask-python/{sdk_version}"
        self._metadata = [
            ("taskhub", taskhub_name),
            ("x-user-agent", user_agent)]  # 'user-agent' is a reserved header in grpc, so we use 'x-user-agent' instead
        super().__init__(self._metadata)

        if token_credential is not None:
            self._token_credential = token_credential
            self._token_manager = AccessTokenManager(token_credential=self._token_credential)
            access_token = self._token_manager.get_access_token()
            if access_token is not None:
                self._metadata.append(("authorization", f"Bearer {access_token.token}"))

    def _intercept_call(
            self, client_call_details: _ClientCallDetails) -> grpc.ClientCallDetails:
        """Internal intercept_call implementation which adds metadata to grpc metadata in the RPC
            call details."""
        # Refresh the auth token if it is present and needed
        if self._metadata is not None:
            for i, (key, _) in enumerate(self._metadata):
                if key.lower() == "authorization":  # Ensure case-insensitive comparison
                    new_token = self._token_manager.get_access_token()  # Get the new token
                    if new_token is not None:
                        self._metadata[i] = ("authorization", f"Bearer {new_token.token}")  # Update the token

        return super()._intercept_call(client_call_details)
