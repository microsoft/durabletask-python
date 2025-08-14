#!/usr/bin/env python3

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Example demonstrating entity locking in durable task orchestrations.

This example shows how to use entity locking to prevent race conditions
when multiple orchestrations need to modify the same entities.
"""

import durabletask as dt
from typing import Any, Optional


def counter_entity(ctx: dt.EntityContext, input: Any) -> Optional[Any]:
    """A counter entity that supports locking and counting operations."""
    operation = ctx.operation_name

    if operation == "__acquire_lock__":
        # Store the lock ID to track who has the lock
        lock_id = input
        current_lock = ctx.get_state(key="__lock__")
        if current_lock is not None:
            raise ValueError(f"Entity {ctx.instance_id} is already locked by {current_lock}")
        ctx.set_state(lock_id, key="__lock__")
        return None

    elif operation == "__release_lock__":
        # Release the lock if it matches the provided lock ID
        lock_id = input
        current_lock = ctx.get_state(key="__lock__")
        if current_lock is None:
            raise ValueError(f"Entity {ctx.instance_id} is not locked")
        if current_lock != lock_id:
            raise ValueError(f"Lock ID mismatch for entity {ctx.instance_id}")
        ctx.set_state(None, key="__lock__")
        return None

    elif operation == "increment":
        # Only allow increment if entity is locked
        current_lock = ctx.get_state(key="__lock__")
        if current_lock is None:
            raise ValueError(f"Entity {ctx.instance_id} must be locked before increment")

        current_count = ctx.get_state(key="count") or 0
        new_count = current_count + (input or 1)
        ctx.set_state(new_count, key="count")
        return new_count

    elif operation == "get":
        # Get can be called without locking
        return ctx.get_state(key="count") or 0

    elif operation == "reset":
        # Reset requires locking
        current_lock = ctx.get_state(key="__lock__")
        if current_lock is None:
            raise ValueError(f"Entity {ctx.instance_id} must be locked before reset")

        ctx.set_state(0, key="count")
        return 0


def bank_account_entity(ctx: dt.EntityContext, input: Any) -> Optional[Any]:
    """A bank account entity that supports locking for safe transfers."""
    operation = ctx.operation_name

    if operation == "__acquire_lock__":
        lock_id = input
        current_lock = ctx.get_state(key="__lock__")
        if current_lock is not None:
            raise ValueError(f"Account {ctx.instance_id} is already locked by {current_lock}")
        ctx.set_state(lock_id, key="__lock__")
        return None

    elif operation == "__release_lock__":
        lock_id = input
        current_lock = ctx.get_state(key="__lock__")
        if current_lock is None:
            raise ValueError(f"Account {ctx.instance_id} is not locked")
        if current_lock != lock_id:
            raise ValueError(f"Lock ID mismatch for account {ctx.instance_id}")
        ctx.set_state(None, key="__lock__")
        return None

    elif operation == "deposit":
        current_lock = ctx.get_state(key="__lock__")
        if current_lock is None:
            raise ValueError(f"Account {ctx.instance_id} must be locked before deposit")

        amount = input.get("amount", 0)
        current_balance = ctx.get_state(key="balance") or 0
        new_balance = current_balance + amount
        ctx.set_state(new_balance, key="balance")
        return new_balance

    elif operation == "withdraw":
        current_lock = ctx.get_state(key="__lock__")
        if current_lock is None:
            raise ValueError(f"Account {ctx.instance_id} must be locked before withdraw")

        amount = input.get("amount", 0)
        current_balance = ctx.get_state(key="balance") or 0
        if current_balance < amount:
            raise ValueError("Insufficient funds")
        new_balance = current_balance - amount
        ctx.set_state(new_balance, key="balance")
        return new_balance

    elif operation == "get_balance":
        return ctx.get_state(key="balance") or 0


def transfer_money_orchestration(ctx: dt.OrchestrationContext, input: Any) -> Any:
    """Orchestration that safely transfers money between accounts using entity locking."""
    from_account = input["from_account"]
    to_account = input["to_account"]
    amount = input["amount"]

    # Lock both accounts to prevent race conditions during transfer
    with ctx.lock_entities(from_account, to_account):
        # First, withdraw from source account
        yield ctx.signal_entity(from_account, "withdraw", input={"amount": amount})

        # Then, deposit to destination account
        yield ctx.signal_entity(to_account, "deposit", input={"amount": amount})

    # Return confirmation that transfer is complete
    return {
        "transfer_completed": True,
        "from_account": from_account,
        "to_account": to_account,
        "amount": amount
    }


def batch_counter_update_orchestration(ctx: dt.OrchestrationContext, input: Any) -> Any:
    """Orchestration that safely updates multiple counters in a batch."""
    counter_ids = input.get("counter_ids", [])
    increment_value = input.get("increment_value", 1)

    # Lock all counters to ensure atomic batch operation
    with ctx.lock_entities(*counter_ids):
        results = []
        for counter_id in counter_ids:
            # Signal each counter to increment
            task = yield ctx.signal_entity(counter_id, "increment", input=increment_value)
            results.append(task)

    # After all operations are complete, get final values
    final_values = {}
    for counter_id in counter_ids:
        value_task = yield ctx.signal_entity(counter_id, "get")
        final_values[counter_id] = value_task

    return {
        "updated_counters": counter_ids,
        "increment_value": increment_value,
        "final_values": final_values
    }


if __name__ == "__main__":
    print("Entity Locking Example")
    print("======================")
    print()
    print("This example demonstrates entity locking patterns:")
    print("1. Counter entity with locking support")
    print("2. Bank account entity with locking for transfers")
    print("3. Transfer orchestration using entity locking")
    print("4. Batch counter update orchestration")
    print()
    print("Key concepts:")
    print("- Entities handle __acquire_lock__ and __release_lock__ operations")
    print("- Orchestrations use ctx.lock_entities() context manager")
    print("- Locks prevent race conditions during multi-entity operations")
    print("- Locks are automatically released even if exceptions occur")
    print()
    print("To use these patterns in your own code:")
    print("1. Implement lock handling in your entity functions")
    print("2. Use 'with ctx.lock_entities(*entity_ids):' in orchestrations")
    print("3. Perform all related entity operations within the lock context")
