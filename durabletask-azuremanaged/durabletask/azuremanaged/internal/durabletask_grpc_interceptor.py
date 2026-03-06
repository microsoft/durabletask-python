# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from importlib.metadata import version
from typing import Optional

import grpc
from azure.core.credentials import TokenCredential
from azure.core.credentials_async import AsyncTokenCredential

from durabletask.azuremanaged.internal.access_token_manager import (
    AccessTokenManager,
    AsyncAccessTokenManager,
)
from durabletask.internal.grpc_interceptor import (
    DefaultAsyncClientInterceptorImpl,
    DefaultClientInterceptorImpl,
    _AsyncClientCallDetails,
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

        self._token_manager = None
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
        # Refresh the auth token if a credential was provided. The call to
        # get_access_token() is generally cheap, checking the expiry time and returning
        # the cached value without a network call when still valid.
        if self._token_manager is not None:
            access_token = self._token_manager.get_access_token()
            if access_token is not None:
                # Update the existing authorization header
                found = False
                for i, (key, _) in enumerate(self._metadata):
                    if key.lower() == "authorization":
                        self._metadata[i] = ("authorization", f"Bearer {access_token.token}")
                        found = True
                        break
                if not found:
                    self._metadata.append(("authorization", f"Bearer {access_token.token}"))

        return super()._intercept_call(client_call_details)


class DTSAsyncDefaultClientInterceptorImpl(DefaultAsyncClientInterceptorImpl):
    """Async version of DTSDefaultClientInterceptorImpl for use with grpc.aio channels.

    This class implements async gRPC interceptors to add DTS-specific headers
    (task hub name, user agent, and authentication token) to all async calls."""

    def __init__(self, token_credential: Optional[AsyncTokenCredential], taskhub_name: str):
        try:
            # Get the version of the azuremanaged package
            sdk_version = version('durabletask-azuremanaged')
        except Exception:
            # Fallback if version cannot be determined
            sdk_version = "unknown"
        user_agent = f"durabletask-python/{sdk_version}"
        self._metadata = [
            ("taskhub", taskhub_name),
            ("x-user-agent", user_agent)]
        super().__init__(self._metadata)

        # Token acquisition is deferred to the first _intercept_call invocation
        # rather than happening in __init__, because get_token() on an
        # AsyncTokenCredential is async and cannot be awaited in a constructor.
        self._token_manager = None
        if token_credential is not None:
            self._token_credential = token_credential
            self._token_manager = AsyncAccessTokenManager(token_credential=self._token_credential)

    async def _intercept_call(
            self, client_call_details: _AsyncClientCallDetails) -> grpc.aio.ClientCallDetails:
        """Internal intercept_call implementation which adds metadata to grpc metadata in the RPC
            call details."""
        # Refresh the auth token if a credential was provided. The call to
        # get_access_token() is generally cheap, checking the expiry time and returning
        # the cached value without a network call when still valid.
        if self._token_manager is not None:
            access_token = await self._token_manager.get_access_token()
            if access_token is not None:
                # Update the existing authorization header, or append one if this
                # is the first successful token acquisition (token is lazily
                # fetched on the first call since async constructors aren't possible).
                found = False
                for i, (key, _) in enumerate(self._metadata):
                    if key.lower() == "authorization":
                        self._metadata[i] = ("authorization", f"Bearer {access_token.token}")
                        found = True
                        break
                if not found:
                    self._metadata.append(("authorization", f"Bearer {access_token.token}"))

        return await super()._intercept_call(client_call_details)
