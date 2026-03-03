# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Unit tests for the order-processing workflows.

These tests use the in-memory backend so they run entirely in-process
with no external dependencies — no sidecar, no emulator, no Azure.

Run from the example root (in_memory_backend_example/):
    pytest test/
"""

import json

import pytest

from durabletask import client, worker
from durabletask.testing import create_test_backend

from src.workflows import (
    Order,
    OrderItem,
    calculate_total,
    order_with_approval,
    process_order,
    process_payment,
    send_confirmation,
    ship_item,
    validate_order,
)

HOST = "localhost:50061"


@pytest.fixture(autouse=True)
def backend():
    """Start and stop the in-memory backend for each test."""
    b = create_test_backend(port=50061)
    yield b
    b.stop()
    b.reset()


def _create_worker() -> worker.TaskHubGrpcWorker:
    """Create a worker with all orchestrators and activities registered."""
    w = worker.TaskHubGrpcWorker(host_address=HOST)
    w.add_orchestrator(process_order)
    w.add_orchestrator(order_with_approval)
    w.add_activity(validate_order)
    w.add_activity(calculate_total)
    w.add_activity(process_payment)
    w.add_activity(send_confirmation)
    w.add_activity(ship_item)
    return w


# ---------------------------------------------------------------------------
# process_order tests
# ---------------------------------------------------------------------------


class TestProcessOrder:
    """Tests for the process_order orchestrator."""

    def test_single_item_order(self):
        """A single-item order should complete with the correct total."""
        order = Order(
            customer="Alice",
            items=[OrderItem(name="Widget", quantity=2, unit_price=10.00)],
        )

        with _create_worker() as w:
            w.start()
            c = client.TaskHubGrpcClient(host_address=HOST)
            instance_id = c.schedule_new_orchestration(process_order, input=order)
            state = c.wait_for_orchestration_completion(instance_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None

        result = json.loads(state.serialized_output)
        assert result["total"] == 20.0
        assert result["items_processed"] == 1
        assert result["status"] == "completed"

    def test_multi_item_order(self):
        """An order with multiple items should fan-out shipping and aggregate results."""
        order = Order(
            customer="Bob",
            items=[
                OrderItem(name="Widget", quantity=3, unit_price=25.00),
                OrderItem(name="Gadget", quantity=1, unit_price=99.99),
                OrderItem(name="Doohickey", quantity=5, unit_price=4.50),
            ],
        )

        with _create_worker() as w:
            w.start()
            c = client.TaskHubGrpcClient(host_address=HOST)
            instance_id = c.schedule_new_orchestration(process_order, input=order)
            state = c.wait_for_orchestration_completion(instance_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None

        result = json.loads(state.serialized_output)
        expected_total = 3 * 25.00 + 1 * 99.99 + 5 * 4.50  # 197.49
        assert result["total"] == expected_total
        assert result["items_processed"] == 3
        assert result["status"] == "completed"

    def test_empty_order_fails(self):
        """An order with no items should fail validation."""
        order = Order(customer="Eve", items=[])

        with _create_worker() as w:
            w.start()
            c = client.TaskHubGrpcClient(host_address=HOST)
            instance_id = c.schedule_new_orchestration(process_order, input=order)
            state = c.wait_for_orchestration_completion(instance_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.FAILED
        assert state.failure_details is not None
        assert "at least one item" in state.failure_details.message

    def test_invalid_quantity_fails(self):
        """An item with zero quantity should fail validation."""
        order = Order(
            customer="Mallory",
            items=[OrderItem(name="Widget", quantity=0, unit_price=10.00)],
        )

        with _create_worker() as w:
            w.start()
            c = client.TaskHubGrpcClient(host_address=HOST)
            instance_id = c.schedule_new_orchestration(process_order, input=order)
            state = c.wait_for_orchestration_completion(instance_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.FAILED
        assert state.failure_details is not None
        assert "Invalid quantity" in state.failure_details.message


# ---------------------------------------------------------------------------
# order_with_approval tests
# ---------------------------------------------------------------------------


class TestOrderWithApproval:
    """Tests for the order_with_approval orchestrator (human-interaction pattern)."""

    def test_low_value_auto_approved(self):
        """Orders under $500 should be auto-approved without waiting for an event."""
        order = Order(
            customer="Alice",
            items=[OrderItem(name="Pen", quantity=10, unit_price=2.00)],
        )

        with _create_worker() as w:
            w.start()
            c = client.TaskHubGrpcClient(host_address=HOST)
            instance_id = c.schedule_new_orchestration(order_with_approval, input=order)
            state = c.wait_for_orchestration_completion(instance_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None

        result = json.loads(state.serialized_output)
        assert result["status"] == "completed"
        assert result["total"] == 20.0

    def test_high_value_approved(self):
        """A high-value order should complete when the approval event is raised."""
        order = Order(
            customer="Bob",
            items=[OrderItem(name="Server", quantity=1, unit_price=1500.00)],
        )

        with _create_worker() as w:
            w.start()
            c = client.TaskHubGrpcClient(host_address=HOST)
            instance_id = c.schedule_new_orchestration(order_with_approval, input=order)

            # Raise the approval event
            c.raise_orchestration_event(instance_id, "approval", data=True)
            state = c.wait_for_orchestration_completion(instance_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None

        result = json.loads(state.serialized_output)
        assert result["status"] == "completed"
        assert result["total"] == 1500.00

    def test_high_value_rejected(self):
        """A high-value order should be rejected when the approval event is False."""
        order = Order(
            customer="Charlie",
            items=[OrderItem(name="Server", quantity=2, unit_price=1500.00)],
        )

        with _create_worker() as w:
            w.start()
            c = client.TaskHubGrpcClient(host_address=HOST)
            instance_id = c.schedule_new_orchestration(order_with_approval, input=order)

            # Reject the order
            c.raise_orchestration_event(instance_id, "approval", data=False)
            state = c.wait_for_orchestration_completion(instance_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None

        result = json.loads(state.serialized_output)
        assert result["status"] == "rejected"
        assert result["items_processed"] == 0

    def test_high_value_timeout(self):
        """A high-value order should be cancelled when approval times out."""
        order = Order(
            customer="Diana",
            items=[OrderItem(name="Server", quantity=1, unit_price=750.00)],
        )

        with _create_worker() as w:
            w.start()
            c = client.TaskHubGrpcClient(host_address=HOST)
            instance_id = c.schedule_new_orchestration(order_with_approval, input=order)

            # Don't raise any event — let the timer fire
            state = c.wait_for_orchestration_completion(instance_id, timeout=60)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None

        result = json.loads(state.serialized_output)
        assert result["status"] == "cancelled-timeout"
        assert result["items_processed"] == 0
