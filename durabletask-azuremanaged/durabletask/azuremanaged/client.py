# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Optional
from durabletask.client import TaskHubGrpcClient, OrchestrationStatus
from durabletask.azuremanaged.internal.access_token_manager import AccessTokenManager
from durabletask.azuremanaged.durabletask_grpc_interceptor import DTSDefaultClientInterceptorImpl
from azure.core.credentials import TokenCredential

# Client class used for Durable Task Scheduler (DTS)
class DurableTaskSchedulerClient(TaskHubGrpcClient):
    def __init__(self, *,
                 host_address: str,
                 taskhub: str,
                 token_credential: TokenCredential = None,
                 secure_channel: Optional[bool] = True):

        if taskhub == None:
            raise ValueError("Taskhub value cannot be empty. Please provide a value for your taskhub")

        self._interceptors = [DTSDefaultClientInterceptorImpl(token_credential, taskhub)]

        # We pass in None for the metadata so we don't construct an additional interceptor in the parent class
        # Since the parent class doesn't use anything metadata for anything else, we can set it as None
        super().__init__(
            host_address=host_address,
            secure_channel=secure_channel,
            metadata=None,
            interceptors=self._interceptors)
