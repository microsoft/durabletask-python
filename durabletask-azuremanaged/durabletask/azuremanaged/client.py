# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
from collections.abc import Sequence

import grpc
import grpc.aio
from azure.core.credentials import TokenCredential
from azure.core.credentials_async import AsyncTokenCredential

from durabletask.azuremanaged.internal.durabletask_grpc_interceptor import (
    DTSAsyncDefaultClientInterceptorImpl,
    DTSDefaultClientInterceptorImpl,
)
from durabletask.client import AsyncTaskHubGrpcClient, TaskHubGrpcClient
from durabletask.grpc_options import (
    GrpcChannelOptions,
    GrpcClientResiliencyOptions,
)
import durabletask.internal.shared as shared
from durabletask.payload.store import PayloadStore
from durabletask.serialization import DataConverter


# Client class used for Durable Task Scheduler (DTS)
class DurableTaskSchedulerClient(TaskHubGrpcClient):
    def __init__(self, *,
                 host_address: str,
                 taskhub: str,
                 token_credential: TokenCredential | None,
                 channel: grpc.Channel | None = None,
                 secure_channel: bool = True,
                 interceptors: Sequence[shared.ClientInterceptor] | None = None,
                 channel_options: GrpcChannelOptions | None = None,
                 resiliency_options: GrpcClientResiliencyOptions | None = None,
                 default_version: str | None = None,
                 payload_store: PayloadStore | None = None,
                 data_converter: DataConverter | None = None,
                 log_handler: logging.Handler | None = None,
                 log_formatter: logging.Formatter | None = None):

        if not taskhub:
            raise ValueError("Taskhub value cannot be empty. Please provide a value for your taskhub")

        resolved_interceptors: list[shared.ClientInterceptor] = (
            list(interceptors) if interceptors is not None else []
        )
        resolved_interceptors.append(DTSDefaultClientInterceptorImpl(token_credential, taskhub))

        # We pass in None for the metadata so we don't construct an additional interceptor in the parent class
        # Since the parent class doesn't use anything metadata for anything else, we can set it as None
        super().__init__(
            host_address=host_address,
            channel=channel,
            secure_channel=secure_channel,
            metadata=None,
            log_handler=log_handler,
            log_formatter=log_formatter,
            interceptors=resolved_interceptors,
            channel_options=channel_options,
            resiliency_options=resiliency_options,
            default_version=default_version,
            payload_store=payload_store,
            data_converter=data_converter)


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
        token_credential (TokenCredential | None): Azure credential for authentication.
            If None, anonymous authentication will be used.
        secure_channel (bool, optional): Whether to use a secure gRPC channel (TLS).
            Defaults to True.
        resiliency_options (GrpcClientResiliencyOptions | None, optional): Client-side
            gRPC resiliency settings forwarded to the base async client.
        default_version (str | None, optional): Default version string for orchestrations.
        payload_store (PayloadStore | None, optional): A payload store for
            externalizing large payloads. If None, payloads are sent inline.
        log_handler (logging.Handler | None, optional): Custom logging handler for client logs.
        log_formatter (logging.Formatter | None, optional): Custom log formatter for client logs.

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
                 token_credential: AsyncTokenCredential | None,
                 channel: grpc.aio.Channel | None = None,
                 secure_channel: bool = True,
                 interceptors: Sequence[shared.AsyncClientInterceptor] | None = None,
                 channel_options: GrpcChannelOptions | None = None,
                 resiliency_options: GrpcClientResiliencyOptions | None = None,
                 default_version: str | None = None,
                 payload_store: PayloadStore | None = None,
                 data_converter: DataConverter | None = None,
                 log_handler: logging.Handler | None = None,
                 log_formatter: logging.Formatter | None = None):

        if not taskhub:
            raise ValueError("Taskhub value cannot be empty. Please provide a value for your taskhub")

        resolved_interceptors: list[shared.AsyncClientInterceptor] = (
            list(interceptors) if interceptors is not None else []
        )
        resolved_interceptors.append(DTSAsyncDefaultClientInterceptorImpl(token_credential, taskhub))

        # We pass in None for the metadata so we don't construct an additional interceptor in the parent class
        # Since the parent class doesn't use anything metadata for anything else, we can set it as None
        super().__init__(
            host_address=host_address,
            channel=channel,
            secure_channel=secure_channel,
            metadata=None,
            log_handler=log_handler,
            log_formatter=log_formatter,
            interceptors=resolved_interceptors,
            channel_options=channel_options,
            resiliency_options=resiliency_options,
            default_version=default_version,
            payload_store=payload_store,
            data_converter=data_converter)
