# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
from unittest.mock import MagicMock

import pytest

from durabletask.client import AsyncTaskHubGrpcClient, TaskHubGrpcClient
from durabletask.internal.shared import get_logger
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

    warning = next(warning for warning in warnings if "log_handler" in str(warning.message))
    assert warning.filename == __file__
    if expected_handler is not None:
        assert client._logger.handlers == [expected_handler]


def test_logger_cannot_be_combined_with_legacy_logging_options():
    with pytest.raises(ValueError, match="cannot be combined"):
        TaskHubGrpcClient(
            channel=MagicMock(),
            logger=logging.Logger("test.durabletask"),
            log_handler=logging.NullHandler(),
        )


def test_logger_conflict_does_not_create_a_sync_grpc_channel(monkeypatch):
    def unexpected_channel_creation(*args, **kwargs):
        raise AssertionError("A gRPC channel should not be created for invalid logging options.")

    monkeypatch.setattr("durabletask.client.shared.get_grpc_channel", unexpected_channel_creation)

    with pytest.raises(ValueError, match="cannot be combined"):
        TaskHubGrpcClient(
            logger=logging.Logger("test.durabletask"),
            log_handler=logging.NullHandler(),
        )


def test_logger_conflict_does_not_create_an_async_grpc_channel(monkeypatch):
    def unexpected_channel_creation(*args, **kwargs):
        raise AssertionError("A gRPC channel should not be created for invalid logging options.")

    monkeypatch.setattr("durabletask.client.shared.get_async_grpc_channel", unexpected_channel_creation)

    with pytest.raises(ValueError, match="cannot be combined"):
        AsyncTaskHubGrpcClient(
            logger=logging.Logger("test.durabletask"),
            log_handler=logging.NullHandler(),
        )


def test_legacy_logging_warning_does_not_skip_similarly_named_application_module():
    namespace = {
        "__name__": "durabletask_app",
        "get_logger": get_logger,
        "logging": logging,
    }
    exec(
        "def create_logger():\n"
        "    return get_logger('test', log_handler=logging.NullHandler())\n",
        namespace,
    )

    with pytest.warns(DeprecationWarning, match="log_handler") as warnings:
        namespace["create_logger"]()

    warning = next(warning for warning in warnings if "log_handler" in str(warning.message))
    assert warning.filename == "<string>"
    assert warning.lineno == 2
