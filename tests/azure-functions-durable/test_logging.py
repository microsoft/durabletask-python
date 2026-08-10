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


def _assert_host_managed_logger(logger: logging.Logger, expected: logging.Logger) -> None:
    assert logger is expected
    assert logger.handlers == []
    assert logger.propagate is True


def test_functions_worker_uses_module_logger():
    worker = DurableFunctionsWorker()

    _assert_host_managed_logger(worker._logger, worker_module._LOGGER)


@pytest.mark.asyncio
async def test_functions_clients_use_module_logger():
    async_client = DurableFunctionsClient("{}")
    sync_client = SyncDurableFunctionsClient("{}")

    _assert_host_managed_logger(async_client._logger, client_module._LOGGER)
    _assert_host_managed_logger(sync_client._logger, client_module._LOGGER)

    await async_client.close()
    sync_client.close()
