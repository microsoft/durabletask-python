# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Optional

from azure.core.credentials import TokenCredential

from durabletask.azuremanaged.internal.durabletask_grpc_interceptor import (
    DTSDefaultClientInterceptorImpl,
)
from durabletask.client import TaskHubGrpcClient


# Client class used for Durable Task Scheduler (DTS)
class DurableTaskSchedulerClient(TaskHubGrpcClient):
    def __init__(self, *,
                 host_address: str,
                 taskhub: str,
                 token_credential: Optional[TokenCredential],
                 secure_channel: bool = True):

        if not taskhub:
            raise ValueError("Taskhub value cannot be empty. Please provide a value for your taskhub")

        interceptors = [DTSDefaultClientInterceptorImpl(token_credential, taskhub)]

        # We pass in None for the metadata so we don't construct an additional interceptor in the parent class
        # Since the parent class doesn't use anything metadata for anything else, we can set it as None
        super().__init__(
            host_address=host_address,
            secure_channel=secure_channel,
            metadata=None,
            interceptors=interceptors)
