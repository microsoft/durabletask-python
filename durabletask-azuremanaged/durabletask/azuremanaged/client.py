# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging

from typing import Optional

from azure.core.credentials import TokenCredential

from durabletask.azuremanaged.internal.durabletask_grpc_interceptor import (
    DTSAsyncDefaultClientInterceptorImpl,
    DTSDefaultClientInterceptorImpl,
)
from durabletask.client import AsyncTaskHubGrpcClient, TaskHubGrpcClient


# Client class used for Durable Task Scheduler (DTS)
class DurableTaskSchedulerClient(TaskHubGrpcClient):
    def __init__(self, *,
                 host_address: str,
                 taskhub: str,
                 token_credential: Optional[TokenCredential],
                 secure_channel: bool = True,
                 default_version: Optional[str] = None,
                 log_handler: Optional[logging.Handler] = None,
                 log_formatter: Optional[logging.Formatter] = None):

        if not taskhub:
            raise ValueError("Taskhub value cannot be empty. Please provide a value for your taskhub")

        interceptors = [DTSDefaultClientInterceptorImpl(token_credential, taskhub)]

        # We pass in None for the metadata so we don't construct an additional interceptor in the parent class
        # Since the parent class doesn't use anything metadata for anything else, we can set it as None
        super().__init__(
            host_address=host_address,
            secure_channel=secure_channel,
            metadata=None,
            log_handler=log_handler,
            log_formatter=log_formatter,
            interceptors=interceptors,
            default_version=default_version)


# Async client class used for Durable Task Scheduler (DTS)
class AsyncDurableTaskSchedulerClient(AsyncTaskHubGrpcClient):
    """An async client implementation for Azure Durable Task Scheduler (DTS).

    This class extends AsyncTaskHubGrpcClient to provide integration with Azure's
    Durable Task Scheduler service using async gRPC. It handles authentication via
    Azure credentials and configures the necessary gRPC interceptors for DTS
    communication.

    Args:
        host_address (str): The gRPC endpoint address of the DTS service.
        taskhub (str): The name of the task hub. Cannot be empty.
        token_credential (Optional[TokenCredential]): Azure credential for authentication.
            If None, anonymous authentication will be used.
        secure_channel (bool, optional): Whether to use a secure gRPC channel (TLS).
            Defaults to True.
        default_version (Optional[str], optional): Default version string for orchestrations.
        log_handler (Optional[logging.Handler], optional): Custom logging handler for client logs.
        log_formatter (Optional[logging.Formatter], optional): Custom log formatter for client logs.

    Raises:
        ValueError: If taskhub is empty or None.

    Example:
        >>> from azure.identity.aio import DefaultAzureCredential
        >>> from durabletask.azuremanaged import AsyncDurableTaskSchedulerClient
        >>>
        >>> credential = DefaultAzureCredential()
        >>> async with AsyncDurableTaskSchedulerClient(
        ...     host_address="my-dts-service.azure.com:443",
        ...     taskhub="my-task-hub",
        ...     token_credential=credential
        ... ) as client:
        ...     instance_id = await client.schedule_new_orchestration("my_orchestrator")
    """

    def __init__(self, *,
                 host_address: str,
                 taskhub: str,
                 token_credential: Optional[TokenCredential],
                 secure_channel: bool = True,
                 default_version: Optional[str] = None,
                 log_handler: Optional[logging.Handler] = None,
                 log_formatter: Optional[logging.Formatter] = None):

        if not taskhub:
            raise ValueError("Taskhub value cannot be empty. Please provide a value for your taskhub")

        interceptors = [DTSAsyncDefaultClientInterceptorImpl(token_credential, taskhub)]

        # We pass in None for the metadata so we don't construct an additional interceptor in the parent class
        # Since the parent class doesn't use anything metadata for anything else, we can set it as None
        super().__init__(
            host_address=host_address,
            secure_channel=secure_channel,
            metadata=None,
            log_handler=log_handler,
            log_formatter=log_formatter,
            interceptors=interceptors,
            default_version=default_version)
