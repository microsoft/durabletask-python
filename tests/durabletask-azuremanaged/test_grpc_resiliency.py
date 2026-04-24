# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from unittest.mock import patch

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
