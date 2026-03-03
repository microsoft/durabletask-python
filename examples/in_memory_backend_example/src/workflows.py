# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Orchestrators and activities for a simple order-processing workflow.

This module defines pure workflow logic with no infrastructure dependencies,
making it easy to test with the in-memory backend.

Note on serialization
---------------------
The Durable Task SDK serializes dataclass and namedtuple inputs to JSON.
When deserialized on the receiving side, top-level objects become
``SimpleNamespace`` instances while nested objects become plain ``dict``s.
Activities that receive complex inputs should therefore use dict-style
access (``item["quantity"]``) for nested data.
"""

from dataclasses import dataclass
from datetime import timedelta

from durabletask import task


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------
# These dataclasses document the expected shape of the data. At runtime,
# they are serialized to JSON and arrive in activities as SimpleNamespace
# (top-level) or dict (nested) objects.


@dataclass
class OrderItem:
    """A single item in an order (arrives as a ``dict`` inside activities)."""
    name: str
    quantity: int
    unit_price: float


@dataclass
class Order:
    """An order containing one or more items (arrives as ``SimpleNamespace``)."""
    customer: str
    items: list[OrderItem]


# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------


def validate_order(ctx: task.ActivityContext, order) -> None:
    """Validate that the order has items and all quantities/prices are valid.

    Raises ``ValueError`` on invalid input.
    """
    if not order.items:
        raise ValueError("Order must contain at least one item")
    for item in order.items:
        if item["quantity"] <= 0:
            raise ValueError(
                f"Invalid quantity for '{item['name']}': {item['quantity']}")
        if item["unit_price"] < 0:
            raise ValueError(
                f"Invalid price for '{item['name']}': {item['unit_price']}")


def calculate_total(ctx: task.ActivityContext, items: list) -> float:
    """Return the total cost for a list of item dicts."""
    return sum(item["quantity"] * item["unit_price"] for item in items)


def process_payment(ctx: task.ActivityContext, amount: float) -> str:
    """Process a payment and return a confirmation ID."""
    if amount <= 0:
        raise ValueError("Payment amount must be positive")
    # In a real app this would call a payment gateway
    return f"PAY-{int(amount * 100)}"


def send_confirmation(ctx: task.ActivityContext, message: str) -> None:
    """Send an order confirmation (e.g. email or push notification)."""
    # In a real app this would send an actual notification
    pass


def ship_item(ctx: task.ActivityContext, item_name: str) -> str:
    """Ship a single item and return a tracking ID."""
    return f"TRACK-{item_name.upper()}"


# ---------------------------------------------------------------------------
# Orchestrators
# ---------------------------------------------------------------------------


def process_order(ctx: task.OrchestrationContext, order):
    """Process a complete order: validate, pay, ship items in parallel, confirm.

    Demonstrates:
    - Activity chaining  (validate → calculate → pay)
    - Fan-out / fan-in   (ship each item in parallel)
    - Error propagation  (validation failures surface automatically)
    """
    # 1. Validate
    yield ctx.call_activity(validate_order, input=order)

    # 2. Calculate total
    total: float = yield ctx.call_activity(
        calculate_total, input=order.items)

    # 3. Process payment
    payment_id: str = yield ctx.call_activity(process_payment, input=total)

    # 4. Ship all items in parallel (fan-out / fan-in)
    ship_tasks: list[task.Task[str]] = [
        ctx.call_activity(ship_item, input=item["name"])
        for item in order.items
    ]
    tracking_ids: list[str] = yield task.when_all(ship_tasks)

    # 5. Send confirmation
    confirmation_msg = (
        f"Order for {order.customer} confirmed. "
        f"Payment: {payment_id}. Tracking: {', '.join(tracking_ids)}"
    )
    yield ctx.call_activity(send_confirmation, input=confirmation_msg)

    return {
        "order_id": payment_id,
        "total": total,
        "items_processed": len(tracking_ids),
        "status": "completed",
    }


def order_with_approval(ctx: task.OrchestrationContext, order):
    """Order workflow that requires manager approval for high-value orders.

    Demonstrates:
    - External events  (human-interaction pattern)
    - Timers           (approval timeout)
    - Sub-orchestration (delegates to ``process_order``)
    """
    # Calculate total to decide whether approval is needed
    total: float = yield ctx.call_activity(
        calculate_total, input=order.items)

    if total >= 500:
        # Wait for approval or timeout after 30 seconds
        approval_event = ctx.wait_for_external_event("approval")
        timeout_event = ctx.create_timer(timedelta(seconds=30))
        winner = yield task.when_any([approval_event, timeout_event])

        if winner == timeout_event:
            return {
                "order_id": "N/A",
                "total": total,
                "items_processed": 0,
                "status": "cancelled-timeout",
            }

        approved: bool = approval_event.get_result()
        if not approved:
            return {
                "order_id": "N/A",
                "total": total,
                "items_processed": 0,
                "status": "rejected",
            }

    # Proceed with the full order flow via sub-orchestration
    result = yield ctx.call_sub_orchestrator(process_order, input=order)
    return result
