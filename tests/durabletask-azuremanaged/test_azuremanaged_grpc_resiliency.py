# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from unittest.mock import patch
from collections.abc import Mapping
from typing import Any

from durabletask.azuremanaged.client import (
    AsyncDurableTaskSchedulerClient,
    DurableTaskSchedulerClient,
)
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from durabletask.grpc_options import (
    GrpcClientResiliencyOptions,
    GrpcWorkerResiliencyOptions,
)


def test_dts_client_passes_resiliency_options_to_base_client():
    resiliency = GrpcClientResiliencyOptions()
    with patch("durabletask.azuremanaged.client.TaskHubGrpcClient.__init__", return_value=None) as mock_init:
        DurableTaskSchedulerClient(
            host_address="localhost:4001",
            taskhub="hub",
            token_credential=None,
            resiliency_options=resiliency,
        )
    assert mock_init.call_args.kwargs["resiliency_options"] is resiliency


def test_dts_worker_passes_resiliency_options_to_base_worker():
    resiliency = GrpcWorkerResiliencyOptions()
    with patch("durabletask.azuremanaged.worker.TaskHubGrpcWorker.__init__", return_value=None) as mock_init:
        DurableTaskSchedulerWorker(
            host_address="localhost:4001",
            taskhub="hub",
            token_credential=None,
            resiliency_options=resiliency,
        )
    assert mock_init.call_args.kwargs["resiliency_options"] is resiliency


def test_dts_worker_passes_exception_properties_provider_to_base_worker():
    class PropertiesProvider:
        def get_exception_properties(self, exception: Exception) -> Mapping[str, Any] | None:
            return None

    provider = PropertiesProvider()
    with patch("durabletask.azuremanaged.worker.TaskHubGrpcWorker.__init__", return_value=None) as mock_init:
        DurableTaskSchedulerWorker(
            host_address="localhost:4001",
            taskhub="hub",
            token_credential=None,
            exception_properties_provider=provider,
        )
    assert mock_init.call_args.kwargs["exception_properties_provider"] is provider


def test_async_dts_client_passes_resiliency_options_to_base_client():
    resiliency = GrpcClientResiliencyOptions()
    with patch(
            "durabletask.azuremanaged.client.AsyncTaskHubGrpcClient.__init__",
            return_value=None,
    ) as mock_init:
        AsyncDurableTaskSchedulerClient(
            host_address="localhost:4001",
            taskhub="hub",
            token_credential=None,
            resiliency_options=resiliency,
        )
    assert mock_init.call_args.kwargs["resiliency_options"] is resiliency
