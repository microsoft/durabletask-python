# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
from unittest.mock import MagicMock

import pytest

from durabletask.client import AsyncTaskHubGrpcClient, TaskHubGrpcClient
from durabletask.worker import TaskHubGrpcWorker


def test_core_components_use_supplied_logger_without_modifying_it():
    logger = logging.Logger("test.durabletask")
    logger.setLevel(logging.ERROR)
    logger.propagate = False
    handler = logging.NullHandler()
    logger.addHandler(handler)

    client = TaskHubGrpcClient(channel=MagicMock(), logger=logger)
    async_client = AsyncTaskHubGrpcClient(channel=MagicMock(), logger=logger)
    worker = TaskHubGrpcWorker(channel=MagicMock(), logger=logger)

    assert client._logger is logger
    assert async_client._logger is logger
    assert worker._logger is logger
    assert logger.level == logging.ERROR
    assert logger.propagate is False
    assert logger.handlers == [handler]


@pytest.mark.parametrize(
    "legacy_options",
    [
        {"log_handler": logging.NullHandler()},
        {"log_formatter": logging.Formatter("%(message)s")},
    ],
)
def test_legacy_logging_options_warn_and_remain_supported(legacy_options):
    expected_handler = legacy_options.get("log_handler")

    with pytest.warns(DeprecationWarning, match="log_handler") as warnings:
        client = TaskHubGrpcClient(channel=MagicMock(), **legacy_options)

    assert warnings[0].filename == __file__
    if expected_handler is not None:
        assert client._logger.handlers == [expected_handler]


def test_logger_cannot_be_combined_with_legacy_logging_options():
    with pytest.raises(ValueError, match="cannot be combined"):
        TaskHubGrpcClient(
            channel=MagicMock(),
            logger=logging.Logger("test.durabletask"),
            log_handler=logging.NullHandler(),
        )
