# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Optional

from azure.core.credentials import TokenCredential

from durabletask.azuremanaged.internal.durabletask_grpc_interceptor import \
    DTSDefaultClientInterceptorImpl
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
                 secure_channel: bool = True,
                 concurrency_options: Optional[ConcurrencyOptions] = None):

        if not taskhub:
            raise ValueError("The taskhub value cannot be empty.")

        interceptors = [DTSDefaultClientInterceptorImpl(token_credential, taskhub)]

        # We pass in None for the metadata so we don't construct an additional interceptor in the parent class
        # Since the parent class doesn't use anything metadata for anything else, we can set it as None
        super().__init__(
            host_address=host_address,
            secure_channel=secure_channel,
            metadata=None,
            interceptors=interceptors,
            concurrency_options=concurrency_options)
