# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Optional
from durabletask.worker import TaskHubGrpcWorker
from externalpackages.durabletaskscheduler.access_token_manager import AccessTokenManager
from externalpackages.durabletaskscheduler.durabletask_grpc_interceptor import DTSDefaultClientInterceptorImpl

# Worker class used for Durable Task Scheduler (DTS)
class DurableTaskSchedulerWorker(TaskHubGrpcWorker):
    def __init__(self,
                 host_address: str,
                 secure_channel: bool,
                 metadata: Optional[list[tuple[str, str]]] = None,
                 use_managed_identity: Optional[bool] = False,
                 client_id: Optional[str] = None,
                 taskhub: str = None,
                 **kwargs):
        
        # Ensure metadata is a list
        metadata = metadata or []
        self._metadata = metadata.copy()  # Copy to prevent modifying input

        # Append DurableTask-specific metadata
        self._metadata.append(("taskhub", taskhub or "default-taskhub"))
        self._metadata.append(("dts", "True"))
        self._metadata.append(("use_managed_identity", str(use_managed_identity)))
        self._metadata.append(("client_id", str(client_id or "None")))

        self._access_token_manager = AccessTokenManager(metadata=self._metadata)
        self.__update_metadata_with_token()
        interceptors = [DTSDefaultClientInterceptorImpl(self._metadata)]

        super().__init__(
            host_address=host_address,
            secure_channel=secure_channel,
            metadata=self._metadata,
            interceptors=interceptors,
            **kwargs
        )

    def __update_metadata_with_token(self):
        """
        Add or update the `authorization` key in the metadata with the current access token.
        """
        token = self._access_token_manager.get_access_token()

        # Ensure that self._metadata is initialized
        if self._metadata is None:
            self._metadata = []  # Initialize it if it's still None
        
        # Check if "authorization" already exists in the metadata
        updated = False
        for i, (key, _) in enumerate(self._metadata):
            if key == "authorization":
                self._metadata[i] = ("authorization", token)
                updated = True
                break
        
        # If not updated, add a new entry
        if not updated:
            self._metadata.append(("authorization", token))
