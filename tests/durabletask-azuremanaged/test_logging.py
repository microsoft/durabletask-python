# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
from unittest.mock import MagicMock

import pytest

from durabletask.azuremanaged.client import (
    AsyncDurableTaskSchedulerClient,
    DurableTaskSchedulerClient,
)
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker


def test_azure_managed_components_forward_supplied_logger():
    logger = logging.Logger("test.azuremanaged")
    handler = logging.NullHandler()
    logger.addHandler(handler)

    client = DurableTaskSchedulerClient(
        host_address="localhost:4001",
        taskhub="test",
        token_credential=None,
        channel=MagicMock(),
        logger=logger,
    )
    async_client = AsyncDurableTaskSchedulerClient(
        host_address="localhost:4001",
        taskhub="test",
        token_credential=None,
        channel=MagicMock(),
        logger=logger,
    )
    worker = DurableTaskSchedulerWorker(
        host_address="localhost:4001",
        taskhub="test",
        token_credential=None,
        channel=MagicMock(),
        logger=logger,
    )

    assert client._logger is logger
    assert async_client._logger is logger
    assert worker._logger is logger
    assert logger.handlers == [handler]


def test_azure_managed_legacy_logging_warning_points_to_caller():
    with pytest.warns(DeprecationWarning, match="log_handler") as warnings:
        DurableTaskSchedulerClient(
            host_address="localhost:4001",
            taskhub="test",
            token_credential=None,
            channel=MagicMock(),
            log_handler=logging.NullHandler(),
        )

    warning = next(warning for warning in warnings if "log_handler" in str(warning.message))
    assert warning.filename == __file__
