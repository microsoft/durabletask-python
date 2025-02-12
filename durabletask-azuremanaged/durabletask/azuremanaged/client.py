# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Optional
from durabletask.client import TaskHubGrpcClient, OrchestrationStatus
from durabletask.azuremanaged.internal.access_token_manager import AccessTokenManager
from durabletask.azuremanaged.durabletask_grpc_interceptor import DTSDefaultClientInterceptorImpl
from azure.identity import DefaultAzureCredential

# Client class used for Durable Task Scheduler (DTS)
class DurableTaskSchedulerClient(TaskHubGrpcClient):
    def __init__(self, *,
                 host_address: str,
                 taskhub: str,
                 secure_channel: Optional[bool] = True,
                 metadata: Optional[list[tuple[str, str]]] = None,
                 use_managed_identity: Optional[bool] = False,
                 client_id: Optional[str] = None):

        if taskhub == None:
            raise ValueError("Taskhub value cannot be empty. Please provide a value for your taskhub")

        # Ensure metadata is a list
        metadata = metadata or []
        self._metadata = metadata.copy()  # Use a copy to avoid modifying original

        # Append DurableTask-specific metadata
        self._metadata.append(("taskhub", taskhub))
        self._metadata.append(("dts", "True"))
        self._metadata.append(("use_managed_identity", str(use_managed_identity)))
        self._metadata.append(("client_id", str(client_id or "None")))

        self._access_token_manager = AccessTokenManager(use_managed_identity=use_managed_identity,
                                                        client_id=client_id)
        token = self._access_token_manager.get_access_token()
        self._metadata.append(("authorization", token))

        self._interceptors = [DTSDefaultClientInterceptorImpl(self._metadata)]

        # We pass in None for the metadata so we don't construct an additional interceptor in the parent class
        # Since the parent class doesn't use anything metadata for anything else, we can set it as None
        super().__init__(
            host_address=host_address,
            secure_channel=secure_channel,
            metadata=None,
            interceptors=self._interceptors)
