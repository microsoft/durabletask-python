# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging

import pytest

from azure.durable_functions import client as client_module
from azure.durable_functions.client import (
    DurableFunctionsClient,
    SyncDurableFunctionsClient,
)
from azure.durable_functions import worker as worker_module
from azure.durable_functions.worker import DurableFunctionsWorker


def _assert_host_managed_logger(
        logger: logging.Logger,
        expected: logging.Logger,
        expected_handlers: list[logging.Handler],
        expected_propagation: bool) -> None:
    assert logger is expected
    assert logger.handlers == expected_handlers
    assert logger.propagate is expected_propagation


def test_functions_worker_uses_module_logger():
    expected_handlers = worker_module._LOGGER.handlers.copy()
    expected_propagation = worker_module._LOGGER.propagate
    worker = DurableFunctionsWorker()

    _assert_host_managed_logger(
        worker._logger,
        worker_module._LOGGER,
        expected_handlers,
        expected_propagation,
    )


@pytest.mark.asyncio
async def test_functions_clients_use_module_logger():
    expected_handlers = client_module._LOGGER.handlers.copy()
    expected_propagation = client_module._LOGGER.propagate
    async_client = DurableFunctionsClient("{}")
    sync_client = SyncDurableFunctionsClient("{}")

    _assert_host_managed_logger(
        async_client._logger,
        client_module._LOGGER,
        expected_handlers,
        expected_propagation,
    )
    _assert_host_managed_logger(
        sync_client._logger,
        client_module._LOGGER,
        expected_handlers,
        expected_propagation,
    )

    await async_client.close()
    sync_client.close()
