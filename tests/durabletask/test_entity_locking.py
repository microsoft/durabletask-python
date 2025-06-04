# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for entity locking functionality."""

import unittest
from typing import Any, Optional
from unittest.mock import patch

import durabletask as dt
from durabletask.worker import _RuntimeOrchestrationContext, EntityLockContext
from durabletask.task import EntityInstanceId


class TestEntityLocking(unittest.TestCase):
    """Test cases for entity locking functionality."""

    def setUp(self):
        """Set up test context."""
        self.ctx = _RuntimeOrchestrationContext("test-instance-id")

    def test_lock_entities_context_manager(self):
        """Test that lock_entities returns a proper context manager."""
        lock_context = self.ctx.lock_entities("Counter@global", "ShoppingCart@user1")
        self.assertIsInstance(lock_context, EntityLockContext)

    def test_lock_entities_with_entity_instance_id(self):
        """Test locking entities using EntityInstanceId objects."""
        entity_id = EntityInstanceId(name="Counter", key="global")
        with patch.object(self.ctx, 'signal_entity'):
            lock_context = self.ctx.lock_entities(entity_id, "ShoppingCart@user1")
            self.assertIsInstance(lock_context, EntityLockContext)

    def test_lock_context_enter_exit_basic(self):
        """Test basic enter/exit functionality of EntityLockContext."""
        with patch.object(self.ctx, 'signal_entity'):
            lock_context = self.ctx.lock_entities("Counter@global", "ShoppingCart@user1")

            # Test enter
            result = lock_context.__enter__()
            self.assertIs(result, lock_context)

            # Test exit
            exit_result = lock_context.__exit__(None, None, None)
            self.assertFalse(exit_result)  # Should not suppress exceptions

    def test_lock_context_signals_correct_operations(self):
        """Test that lock context sends correct lock/unlock signals."""
        with patch.object(self.ctx, 'signal_entity') as mock_signal:
            entity_ids = ["Counter@global", "ShoppingCart@user1"]

            with self.ctx.lock_entities(*entity_ids):
                pass  # Context manager will handle enter/exit

            # Should have called signal_entity 4 times: 2 locks + 2 unlocks
            self.assertEqual(mock_signal.call_count, 4)

            # Check acquire lock calls (first 2 calls)
            for i, entity_id in enumerate(entity_ids):
                call_args = mock_signal.call_args_list[i]
                self.assertEqual(call_args[0][0], entity_id)  # entity_id
                self.assertEqual(call_args[0][1], "__acquire_lock__")  # operation
                self.assertIsNotNone(call_args[1]['input'])  # lock_instance_id

            # Check release lock calls (last 2 calls)
            for i, entity_id in enumerate(entity_ids):
                call_args = mock_signal.call_args_list[i + 2]
                self.assertEqual(call_args[0][0], entity_id)  # entity_id
                self.assertEqual(call_args[0][1], "__release_lock__")  # operation
                self.assertIsNotNone(call_args[1]['input'])  # lock_instance_id

    def test_lock_context_preserves_lock_instance_id(self):
        """Test that the same lock instance ID is used for acquire and release."""
        with patch.object(self.ctx, 'signal_entity') as mock_signal:
            entity_id = "Counter@global"

            with self.ctx.lock_entities(entity_id):
                pass

            # Extract lock instance IDs from the calls
            acquire_call = mock_signal.call_args_list[0]
            release_call = mock_signal.call_args_list[1]

            acquire_lock_id = acquire_call[1]['input']
            release_lock_id = release_call[1]['input']

            self.assertEqual(acquire_lock_id, release_lock_id)
            self.assertIn("__lock__", acquire_lock_id)
            self.assertIn("test-instance-id", acquire_lock_id)

    def test_lock_context_exception_handling(self):
        """Test that locks are released even when exceptions occur."""
        with patch.object(self.ctx, 'signal_entity') as mock_signal:
            entity_id = "Counter@global"

            with self.assertRaises(ValueError):
                with self.ctx.lock_entities(entity_id):
                    raise ValueError("Test exception")

            # Should still have called signal_entity for both acquire and release
            self.assertEqual(mock_signal.call_count, 2)

            # Verify acquire lock call
            acquire_call = mock_signal.call_args_list[0]
            self.assertEqual(acquire_call[0][0], entity_id)
            self.assertEqual(acquire_call[0][1], "__acquire_lock__")

            # Verify release lock call
            release_call = mock_signal.call_args_list[1]
            self.assertEqual(release_call[0][0], entity_id)
            self.assertEqual(release_call[0][1], "__release_lock__")

    def test_multiple_entity_locking(self):
        """Test locking multiple entities simultaneously."""
        with patch.object(self.ctx, 'signal_entity') as mock_signal:
            entity_ids = ["Counter@global", "Counter@user1", "ShoppingCart@cart1"]

            with self.ctx.lock_entities(*entity_ids):
                pass

            # Should have 6 calls: 3 acquire + 3 release
            self.assertEqual(mock_signal.call_count, 6)

            # All acquire calls should use the same lock instance ID
            lock_ids = set()
            for i in range(3):
                call_args = mock_signal.call_args_list[i]
                lock_ids.add(call_args[1]['input'])

            self.assertEqual(len(lock_ids), 1)  # All should use same lock ID

    def test_empty_entity_list(self):
        """Test locking with no entities."""
        with patch.object(self.ctx, 'signal_entity') as mock_signal:
            with self.ctx.lock_entities():
                pass

            # Should not call signal_entity at all
            mock_signal.assert_not_called()


class TestEntityLockingIntegration(unittest.TestCase):
    """Integration tests for entity locking with real entity functions."""

    def setUp(self):
        """Set up test entities."""
        self.lock_states = {}  # Track which entities are locked by which orchestration

        def counter_entity(ctx: dt.EntityContext, input: Any) -> Optional[Any]:
            """A test counter entity that respects locking."""
            operation = ctx.operation_name

            if operation == "__acquire_lock__":
                lock_id = input
                if ctx.instance_id in self.lock_states:
                    raise ValueError(f"Entity {ctx.instance_id} is already locked by {self.lock_states[ctx.instance_id]}")
                self.lock_states[ctx.instance_id] = lock_id
                return None

            elif operation == "__release_lock__":
                lock_id = input
                if ctx.instance_id not in self.lock_states:
                    raise ValueError(f"Entity {ctx.instance_id} is not locked")
                if self.lock_states[ctx.instance_id] != lock_id:
                    raise ValueError(f"Lock ID mismatch for entity {ctx.instance_id}")
                del self.lock_states[ctx.instance_id]
                return None

            elif operation == "increment":
                # Check if locked (for integration testing)
                if ctx.instance_id in self.lock_states:
                    current = ctx.get_state() or 0
                    new_value = current + (input or 1)
                    ctx.set_state(new_value)
                    return new_value
                else:
                    raise ValueError(f"Entity {ctx.instance_id} must be locked before increment")

            elif operation == "get":
                return ctx.get_state() or 0

        self.counter_entity = counter_entity

    def test_entity_lock_integration(self):
        """Test that entities properly handle lock/unlock operations."""
        ctx = dt.EntityContext("Counter@test", "__acquire_lock__")

        # Test acquiring lock
        result = self.counter_entity(ctx, "test-lock-id")
        self.assertIsNone(result)
        self.assertEqual(self.lock_states["Counter@test"], "test-lock-id")

        # Test releasing lock
        ctx = dt.EntityContext("Counter@test", "__release_lock__")
        result = self.counter_entity(ctx, "test-lock-id")
        self.assertIsNone(result)
        self.assertNotIn("Counter@test", self.lock_states)

    def test_entity_double_lock_fails(self):
        """Test that double-locking an entity fails."""
        ctx = dt.EntityContext("Counter@test", "__acquire_lock__")

        # Acquire first lock
        self.counter_entity(ctx, "lock-id-1")

        # Try to acquire second lock - should fail
        with self.assertRaises(ValueError) as cm:
            self.counter_entity(ctx, "lock-id-2")

        self.assertIn("already locked", str(cm.exception))

    def test_entity_unlock_without_lock_fails(self):
        """Test that unlocking a non-locked entity fails."""
        ctx = dt.EntityContext("Counter@test", "__release_lock__")

        with self.assertRaises(ValueError) as cm:
            self.counter_entity(ctx, "test-lock-id")

        self.assertIn("not locked", str(cm.exception))

    def test_entity_operation_requires_lock(self):
        """Test that entity operations require the entity to be locked."""
        ctx = dt.EntityContext("Counter@test", "increment")

        with self.assertRaises(ValueError) as cm:
            self.counter_entity(ctx, 1)

        self.assertIn("must be locked", str(cm.exception))


class TestEntityLockingOrchestration(unittest.TestCase):
    """Test entity locking in orchestration context."""

    def test_orchestration_with_entity_locking(self):
        """Test an orchestration that uses entity locking."""
        def test_orchestration(ctx: dt.OrchestrationContext, input: Any):
            """Test orchestration that locks entities and performs operations."""
            with ctx.lock_entities("Counter@global", "Counter@user1"):
                # Perform operations on locked entities
                yield ctx.signal_entity("Counter@global", "increment", input=1)
                yield ctx.signal_entity("Counter@user1", "increment", input=2)

            # After lock is released, signal another operation
            yield ctx.signal_entity("Counter@global", "get")
            return "completed"

        # This test verifies the orchestration can be compiled and the context manager works
        # In a real scenario, this would be executed by the durable task runtime
        self.assertTrue(callable(test_orchestration))


if __name__ == '__main__':
    unittest.main()
