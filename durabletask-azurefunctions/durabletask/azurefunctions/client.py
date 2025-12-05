# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json

from datetime import timedelta
from typing import Any, Optional
import azure.functions as func
from urllib.parse import urlparse, quote

from durabletask.entities import EntityInstanceId
from durabletask.client import TaskHubGrpcClient
from durabletask.azurefunctions.internal.azurefunctions_grpc_interceptor import AzureFunctionsDefaultClientInterceptorImpl
from durabletask.azurefunctions.http import HttpManagementPayload


# Client class used for Durable Functions
class DurableFunctionsClient(TaskHubGrpcClient):
    """A gRPC client passed to Durable Functions durable client bindings.

    Connects to the Durable Functions runtime using gRPC and provides methods
    for creating and managing Durable orchestrations, interacting with Durable entities, 
    and creating HTTP management payloads and check status responses for use with Durable Functions invocations.
    """
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
        """Initializes a DurableFunctionsClient instance from a JSON string.

        This string will be provided by the Durable Functions host extension upon invocation of the client trigger.

        Args:
            client_as_string (str): A JSON string containing the Durable Functions client configuration.

        Raises:
            json.JSONDecodeError: If the provided string is not valid JSON.
        """
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
        location_url = self._get_instance_status_url(request, instance_id)
        return func.HttpResponse(
            body=str(self._get_client_response_links(request, instance_id)),
            status_code=501,
            headers={
                'content-type': 'application/json',
                'Location': location_url,
            },
        )

    def create_http_management_payload(self, request: func.HttpRequest, instance_id: str) -> HttpManagementPayload:
        """Creates an HTTP management payload for a Durable Function instance.

        Args:
            instance_id (str): The ID of the Durable Function instance.
        """
        return self._get_client_response_links(request, instance_id)

    def _get_client_response_links(self, request: func.HttpRequest, instance_id: str) -> HttpManagementPayload:
        instance_status_url = self._get_instance_status_url(request, instance_id)
        return HttpManagementPayload(instance_id, instance_status_url, self.requiredQueryStringParameters)

    @staticmethod
    def _get_instance_status_url(request: func.HttpRequest, instance_id: str) -> str:
        request_url = urlparse(request.url)
        location_url = f"{request_url.scheme}://{request_url.netloc}"
        encoded_instance_id = quote(instance_id)
        location_url = location_url + "/runtime/webhooks/durabletask/instances/" + encoded_instance_id
        return location_url
