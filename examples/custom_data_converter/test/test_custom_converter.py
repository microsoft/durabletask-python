# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""End-to-end proof that the pydantic ``DataConverter`` plugs into the SDK.

These tests run entirely in-process against the in-memory backend -- no
sidecar, emulator, or Azure resources. They prove that:

1. A pydantic model passed as orchestration input round-trips through the wire
   and arrives at the orchestrator/activity as a *validated model instance*.
2. The orchestration's pydantic result is reconstructed, typed, on the client
   via ``state.get_output(Receipt)``.
3. The wire payload is real pydantic JSON (``model_dump_json``), confirming the
   custom converter -- not the default codec -- handled the model.
4. The same converter must be supplied to both worker and client.

Run from the example root (custom_data_converter/):
    pytest test/
"""

from __future__ import annotations

import json

import pytest

from durabletask import client, worker
from durabletask.testing import create_test_backend

from src.converter import PydanticDataConverter
from src.workflows import (
    Order,
    OrderItem,
    Receipt,
    calculate_total,
    charge_payment,
    process_order,
    sample_order,
)

HOST = "localhost:50071"
PORT = 50071


@pytest.fixture(autouse=True)
def backend():
    """Start and stop the in-memory backend for each test."""
    b = create_test_backend(port=PORT)
    yield b
    b.stop()
    b.reset()


def _worker(converter: PydanticDataConverter) -> worker.TaskHubGrpcWorker:
    w = worker.TaskHubGrpcWorker(host_address=HOST, data_converter=converter)
    w.add_orchestrator(process_order)
    w.add_activity(calculate_total)
    w.add_activity(charge_payment)
    return w


def _run(order: Order):
    """Run ``process_order`` with the pydantic converter wired into both sides."""
    converter = PydanticDataConverter()
    with _worker(converter) as w:
        w.start()
        with client.TaskHubGrpcClient(host_address=HOST, data_converter=converter) as c:
            instance_id = c.schedule_new_orchestration(process_order, input=order)
            state = c.wait_for_orchestration_completion(instance_id, timeout=30)
    return state


def test_pydantic_model_round_trips_and_output_is_typed():
    """A valid order completes and the client reads back a typed Receipt."""
    order = sample_order()
    state = _run(order)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.COMPLETED

    # The output is reconstructed as a validated pydantic model, not a dict.
    receipt = state.get_output(Receipt)
    assert isinstance(receipt, Receipt)
    assert receipt.customer == "Contoso"
    assert receipt.item_count == 4  # 3 widgets + 1 gadget
    assert receipt.total == pytest.approx(174.99)
    assert receipt.confirmation_id == "PAY-17499"


def test_input_is_validated_pydantic_model_on_the_wire():
    """The serialized input is genuine pydantic JSON, proving the converter ran."""
    converter = PydanticDataConverter()
    order = sample_order()

    serialized = converter.serialize(order)
    assert serialized is not None
    # ``model_dump_json`` emits the model fields; ``placed_at`` is an ISO string.
    payload = json.loads(serialized)
    assert payload["customer"] == "Contoso"
    assert isinstance(payload["placed_at"], str)
    assert payload["items"][0]["name"] == "Widget"

    # And the SDK reconstructs it back into a validated model given the type.
    restored = converter.deserialize(serialized, Order)
    assert isinstance(restored, Order)
    assert isinstance(restored.items[0], OrderItem)
    assert restored.items[0].quantity == 3


def test_invalid_order_input_fails_validation_in_orchestration():
    """An order that violates a pydantic constraint surfaces as a failure.

    The activity/orchestrator receives the input via the converter, so the
    pydantic ``ValidationError`` raised while reconstructing the model fails the
    orchestration rather than silently passing bad data through.
    """
    # ``quantity`` must be > 0; construct the bad payload past pydantic's own
    # constructor guard via ``model_construct`` so the failure happens on the
    # wire (during reconstruction) inside the orchestration.
    bad_order = Order.model_construct(
        customer="Contoso",
        placed_at=sample_order().placed_at,
        items=[OrderItem.model_construct(name="Widget", quantity=-1, unit_price=25.0)],
    )

    state = _run(bad_order)

    assert state is not None
    assert state.runtime_status == client.OrchestrationStatus.FAILED
    assert state.failure_details is not None
    assert "validation" in (state.failure_details.message or "").lower() \
        or "quantity" in (state.failure_details.message or "").lower()


def test_default_converter_cannot_serialize_pydantic_model():
    """Contrast: the default codec has no idea how to serialize a pydantic model.

    This is what motivates the custom converter. Pydantic models are not
    dataclasses and expose no ``to_json()`` hook, so the default
    ``JsonDataConverter`` raises ``TypeError`` when asked to serialize one. The
    ``PydanticDataConverter`` is what makes serialization work (via
    ``model_dump_json``).
    """
    from durabletask.serialization import JsonDataConverter

    default = JsonDataConverter()
    with pytest.raises(TypeError):
        default.serialize(sample_order())
