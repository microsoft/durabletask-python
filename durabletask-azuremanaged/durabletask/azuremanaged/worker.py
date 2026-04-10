# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import os
import socket
import uuid

from typing import Optional, Sequence

import grpc
from azure.core.credentials import TokenCredential

from durabletask.azuremanaged.internal.durabletask_grpc_interceptor import \
    DTSDefaultClientInterceptorImpl
from durabletask.grpc_options import GrpcChannelOptions
import durabletask.internal.shared as shared
from durabletask.payload.store import PayloadStore
from durabletask.worker import ConcurrencyOptions, TaskHubGrpcWorker


# Worker class used for Durable Task Scheduler (DTS)
class DurableTaskSchedulerWorker(TaskHubGrpcWorker):
    """A worker implementation for Azure Durable Task Scheduler (DTS).

    This class extends TaskHubGrpcWorker to provide integration with Azure's
    Durable Task Scheduler service. It handles authentication via Azure credentials
    and configures the necessary gRPC interceptors for DTS communication.

    Args:
        host_address (str): The gRPC endpoint address of the DTS service.
        taskhub (str): The name of the task hub. Cannot be empty.
        token_credential (Optional[TokenCredential]): Azure credential for authentication.
            If None, anonymous authentication will be used.
        secure_channel (bool, optional): Whether to use a secure gRPC channel (TLS).
            Defaults to True.
        concurrency_options (Optional[ConcurrencyOptions], optional): Configuration
            for controlling worker concurrency limits. If None, default concurrency
            settings will be used.
        payload_store (Optional[PayloadStore], optional): A payload store for
            externalizing large payloads. If None, payloads are sent inline.
        log_handler (Optional[logging.Handler], optional): Custom logging handler for worker logs.
        log_formatter (Optional[logging.Formatter], optional): Custom log formatter for worker logs.

    Raises:
        ValueError: If taskhub is empty or None.

    Example:
        >>> from azure.identity import DefaultAzureCredential
        >>> from durabletask.azuremanaged import DurableTaskSchedulerWorker
        >>> from durabletask.worker import ConcurrencyOptions
        >>>
        >>> credential = DefaultAzureCredential()
        >>> concurrency = ConcurrencyOptions(max_concurrent_activities=10)
        >>> worker = DurableTaskSchedulerWorker(
        ...     host_address="my-dts-service.azure.com:443",
        ...     taskhub="my-task-hub",
        ...     token_credential=credential,
        ...     concurrency_options=concurrency
        ... )

    Note:
        This worker automatically configures DTS-specific gRPC interceptors
        for authentication and task hub routing. The parent class metadata
        parameter is set to None since authentication is handled by the
        DTS interceptor.
    """

    def __init__(self, *,
                 host_address: str,
                 taskhub: str,
                 token_credential: Optional[TokenCredential],
                 channel: Optional[grpc.Channel] = None,
                 secure_channel: bool = True,
                 interceptors: Optional[Sequence[shared.ClientInterceptor]] = None,
                 channel_options: Optional[GrpcChannelOptions] = None,
                 concurrency_options: Optional[ConcurrencyOptions] = None,
                 payload_store: Optional[PayloadStore] = None,
                 log_handler: Optional[logging.Handler] = None,
                 log_formatter: Optional[logging.Formatter] = None):

        if not taskhub:
            raise ValueError("The taskhub value cannot be empty.")

        worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4()}"
        resolved_interceptors: list[shared.ClientInterceptor] = (
            list(interceptors) if interceptors is not None else []
        )
        resolved_interceptors.append(
            DTSDefaultClientInterceptorImpl(token_credential, taskhub, worker_id=worker_id)
        )

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
            concurrency_options=concurrency_options,
            # DTS natively supports long timers so chunking is unnecessary
            maximum_timer_interval=None,
            payload_store=payload_store
        )
