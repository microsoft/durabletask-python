# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Pydantic models and the workflow that exercises them.

These models are plain :class:`pydantic.BaseModel` subclasses -- *not*
dataclasses -- which is what makes them a meaningful proof: without the custom
:class:`~src.converter.PydanticDataConverter`, the SDK's default codec would not
know how to validate or reconstruct them. The orchestrator and activities
annotate their inputs/outputs with these model types, so the SDK hands those
types to the converter at every boundary.
"""

from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, field_validator

from durabletask import task


# ---------------------------------------------------------------------------
# Pydantic models (validated at every serialization boundary)
# ---------------------------------------------------------------------------
# These are plain ``pydantic.BaseModel`` subclasses -- no special hooks. The
# custom ``PydanticDataConverter`` both serializes them and (because it
# overrides ``can_reconstruct``) teaches the SDK to reconstruct them for
# inbound orchestrator/activity inputs.


class OrderItem(BaseModel):
    """A single line item. Pydantic validates the constraints below."""

    name: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0)


class Order(BaseModel):
    """An order placed by a customer at a point in time."""

    customer: str = Field(min_length=1)
    placed_at: datetime
    items: list[OrderItem]

    @field_validator("items")
    @classmethod
    def _must_have_items(cls, items: list[OrderItem]) -> list[OrderItem]:
        if not items:
            raise ValueError("an order must contain at least one item")
        return items


class Receipt(BaseModel):
    """The orchestration's typed result."""

    customer: str
    total: float
    item_count: int
    confirmation_id: str


# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------
# Each activity annotates its input parameter with a pydantic model type. The
# worker passes that type to ``PydanticDataConverter.deserialize``, so the
# activity body receives a fully validated model instance (attribute access),
# never a raw dict.


def calculate_total(ctx: task.ActivityContext, order: Order) -> float:
    """Return the order's total cost from the validated model."""
    return round(sum(item.quantity * item.unit_price for item in order.items), 2)


def charge_payment(ctx: task.ActivityContext, amount: float) -> str:
    """Pretend to charge a payment processor and return a confirmation ID."""
    if amount <= 0:
        raise ValueError("payment amount must be positive")
    return f"PAY-{int(amount * 100)}"


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def process_order(ctx: task.OrchestrationContext, order: Order) -> Generator[task.Task[Any], Any, Receipt]:
    """Validate, total, and charge an order, returning a typed :class:`Receipt`.

    The orchestrator's ``order`` parameter is reconstructed as a validated
    ``Order`` by the custom converter. ``call_activity(..., return_type=Order)``
    and the ``Receipt`` return value likewise round-trip through pydantic.
    """
    total: float = yield ctx.call_activity(calculate_total, input=order)
    confirmation_id: str = yield ctx.call_activity(charge_payment, input=total)

    return Receipt(
        customer=order.customer,
        total=total,
        item_count=sum(item.quantity for item in order.items),
        confirmation_id=confirmation_id,
    )


def sample_order() -> Order:
    """A valid sample order used by both the runnable app and the tests."""
    return Order(
        customer="Contoso",
        placed_at=datetime.now(timezone.utc),
        items=[
            OrderItem(name="Widget", quantity=3, unit_price=25.0),
            OrderItem(name="Gadget", quantity=1, unit_price=99.99),
        ],
    )
