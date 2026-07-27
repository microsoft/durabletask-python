# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the ``Orchestrator`` handle wrapper.

``Orchestrator`` is the thin adapter registered with the Azure Functions host:
its generated ``handle`` receives the host's transport context and delegates to
a fresh :class:`DurableFunctionsWorker` per invocation.
"""

from unittest.mock import MagicMock, patch

from azure.durable_functions.orchestrator import Orchestrator


def test_handle_delegates_to_worker():
    def user_orchestrator(context):
        return "result"

    context = MagicMock()
    with patch(
        "azure.durable_functions.orchestrator.DurableFunctionsWorker"
    ) as worker_cls:
        worker_cls.return_value.execute_orchestration_request.return_value = "encoded"
        result = Orchestrator(user_orchestrator).handle(context)

    assert result == "encoded"
    worker_cls.return_value.execute_orchestration_request.assert_called_once_with(
        user_orchestrator, context)


def test_handle_stores_durable_context():
    def user_orchestrator(context):
        return None

    context = MagicMock()
    orchestrator = Orchestrator(user_orchestrator)
    with patch("azure.durable_functions.orchestrator.DurableFunctionsWorker"):
        orchestrator.handle(context)
    assert orchestrator.durable_context is context


def test_create_returns_callable_handle_exposing_original_fn():
    def user_orchestrator(context):
        return None

    handle = Orchestrator.create(user_orchestrator)
    assert callable(handle)
    assert handle.orchestrator_function is user_orchestrator


def test_created_handle_delegates_to_worker():
    def user_orchestrator(context):
        return None

    handle = Orchestrator.create(user_orchestrator)
    context = MagicMock()
    with patch(
        "azure.durable_functions.orchestrator.DurableFunctionsWorker"
    ) as worker_cls:
        worker_cls.return_value.execute_orchestration_request.return_value = "encoded"
        result = handle(context)

    assert result == "encoded"
    worker_cls.return_value.execute_orchestration_request.assert_called_once_with(
        user_orchestrator, context)
