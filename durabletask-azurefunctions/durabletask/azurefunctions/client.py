# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json

from datetime import timedelta
from typing import Any, Optional
import azure.functions as func

from durabletask.entities import EntityInstanceId
from durabletask.client import TaskHubGrpcClient
from durabletask.azurefunctions.internal.azurefunctions_grpc_interceptor import AzureFunctionsDefaultClientInterceptorImpl


# Client class used for Durable Functions
class DurableFunctionsClient(TaskHubGrpcClient):
    taskHubName: str
    connectionName: str
    creationUrls: dict[str, str]
    managementUrls: dict[str, str]
    baseUrl: str
    requiredQueryStringParameters: str
    rpcBaseUrl: str
    httpBaseUrl: str
    maxGrpcMessageSizeInBytes: int
    grpcHttpClientTimeout: timedelta

    def __init__(self, client_as_string: str):
        client = json.loads(client_as_string)

        self.taskHubName = client.get("taskHubName", "")
        self.connectionName = client.get("connectionName", "")
        self.creationUrls = client.get("creationUrls", {})
        self.managementUrls = client.get("managementUrls", {})
        self.baseUrl = client.get("baseUrl", "")
        self.requiredQueryStringParameters = client.get("requiredQueryStringParameters", "")
        self.rpcBaseUrl = client.get("rpcBaseUrl", "")
        self.httpBaseUrl = client.get("httpBaseUrl", "")
        self.maxGrpcMessageSizeInBytes = client.get("maxGrpcMessageSizeInBytes", 0)
        # TODO: convert the string value back to timedelta - annoying regex?
        self.grpcHttpClientTimeout = client.get("grpcHttpClientTimeout", timedelta(seconds=30))
        interceptors = [AzureFunctionsDefaultClientInterceptorImpl(self.taskHubName, self.requiredQueryStringParameters)]

        # We pass in None for the metadata so we don't construct an additional interceptor in the parent class
        # Since the parent class doesn't use anything metadata for anything else, we can set it as None
        super().__init__(
            host_address=self.rpcBaseUrl,
            secure_channel=False,
            metadata=None,
            interceptors=interceptors)

    def create_check_status_response(self, request: func.HttpRequest, instance_id: str) -> func.HttpResponse:
        """Creates an HTTP response for checking the status of a Durable Function instance.

        Args:
            request (func.HttpRequest): The incoming HTTP request.
            instance_id (str): The ID of the Durable Function instance.
        """
        raise NotImplementedError("This method is not implemented yet.")

    def create_http_management_payload(self, instance_id: str) -> dict[str, str]:
        """Creates an HTTP management payload for a Durable Function instance.

        Args:
            instance_id (str): The ID of the Durable Function instance.
        """
        raise NotImplementedError("This method is not implemented yet.")

    def read_entity_state(
        self,
        entity_id: EntityInstanceId,
        task_hub_name: Optional[str],
        connection_name: Optional[str]
    ) -> tuple[bool, Any]:
        """Reads the state of a Durable Entity.

        Args:
            entity_id (str): The ID of the Durable Entity.
            task_hub_name (Optional[str]): The name of the task hub.
            connection_name (Optional[str]): The name of the connection.

        Returns:
            (bool, Any): A tuple containing a boolean indicating if the entity exists and its state.
        """
        raise NotImplementedError("This method is not implemented yet.")
