# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Optional
from durabletask.worker import TaskHubGrpcWorker
from durabletask.azuremanaged.internal.access_token_manager import AccessTokenManager
from durabletask.azuremanaged.durabletask_grpc_interceptor import DTSDefaultClientInterceptorImpl
from azure.core.credentials import TokenCredential

# Worker class used for Durable Task Scheduler (DTS)
class DurableTaskSchedulerWorker(TaskHubGrpcWorker):
    def __init__(self, *,
                 host_address: str,
                 taskhub: str,
                 secure_channel: Optional[bool] = True,
                 metadata: Optional[list[tuple[str, str]]] = None,
                 token_credential: Optional[TokenCredential] = None):
        
        if taskhub == None:
            raise ValueError("Taskhub value cannot be empty. Please provide a value for your taskhub")

        # Ensure metadata is a list
        metadata = metadata or []
        self._metadata = metadata.copy()  # Copy to prevent modifying input

        # Append DurableTask-specific metadata
        self._metadata.append(("taskhub", taskhub))
        self._metadata.append(("dts", "True"))
        self._metadata.append(("token_credential", token_credential))
        interceptors = [DTSDefaultClientInterceptorImpl(self._metadata)]

        # We pass in None for the metadata so we don't construct an additional interceptor in the parent class
        # Since the parent class doesn't use anything metadata for anything else, we can set it as None
        super().__init__(
            host_address=host_address,
            secure_channel=secure_channel,
            metadata=None, 
            interceptors=interceptors)