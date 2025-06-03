# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import unittest
from datetime import datetime
from durabletask import task


class TestEntityTypes(unittest.TestCase):
    
    def test_entity_context_creation(self):
        """Test that EntityContext can be created with basic properties."""
        ctx = task.EntityContext("test-entity-1", "increment", is_new_entity=True)
        
        self.assertEqual(ctx.instance_id, "test-entity-1")
        self.assertEqual(ctx.operation_name, "increment")
        self.assertTrue(ctx.is_new_entity)
        self.assertIsNone(ctx.get_state())
    
    def test_entity_context_state_management(self):
        """Test that EntityContext can manage state."""
        ctx = task.EntityContext("test-entity-1", "increment")
        
        # Initially no state
        self.assertIsNone(ctx.get_state())
        
        # Set state
        test_state = {"count": 5}
        ctx.set_state(test_state)
        
        # Get state back
        self.assertEqual(ctx.get_state(), test_state)
    
    def test_entity_state_creation(self):
        """Test that EntityState can be created."""
        now = datetime.utcnow()
        state = task.EntityState(
            instance_id="test-entity-1",
            last_modified_time=now,
            backlog_queue_size=0,
            locked_by=None,
            serialized_state='{"count": 5}'
        )
        
        self.assertEqual(state.instance_id, "test-entity-1")
        self.assertEqual(state.last_modified_time, now)
        self.assertEqual(state.backlog_queue_size, 0)
        self.assertIsNone(state.locked_by)
        self.assertEqual(state.serialized_state, '{"count": 5}')
        self.assertTrue(state.exists)
    
    def test_entity_state_exists_property(self):
        """Test that EntityState.exists works correctly."""
        # Entity with state exists
        state_with_data = task.EntityState(
            instance_id="test-entity-1",
            last_modified_time=datetime.utcnow(),
            backlog_queue_size=0,
            locked_by=None,
            serialized_state='{"count": 5}'
        )
        self.assertTrue(state_with_data.exists)
        
        # Entity without state doesn't exist
        state_without_data = task.EntityState(
            instance_id="test-entity-2",
            last_modified_time=datetime.utcnow(),
            backlog_queue_size=0,
            locked_by=None,
            serialized_state=None
        )
        self.assertFalse(state_without_data.exists)
    
    def test_entity_query_creation(self):
        """Test that EntityQuery can be created with various parameters."""
        query = task.EntityQuery(
            instance_id_starts_with="test-",
            include_state=True,
            include_transient=False,
            page_size=10
        )
        
        self.assertEqual(query.instance_id_starts_with, "test-")
        self.assertTrue(query.include_state)
        self.assertFalse(query.include_transient)
        self.assertEqual(query.page_size, 10)
        self.assertIsNone(query.continuation_token)
    
    def test_entity_query_result_creation(self):
        """Test that EntityQueryResult can be created."""
        entities = [
            task.EntityState(
                instance_id="test-entity-1",
                last_modified_time=datetime.utcnow(),
                backlog_queue_size=0,
                locked_by=None,
                serialized_state='{"count": 5}'
            )
        ]
        
        result = task.EntityQueryResult(
            entities=entities,
            continuation_token="next-page-token"
        )
        
        self.assertEqual(len(result.entities), 1)
        self.assertEqual(result.entities[0].instance_id, "test-entity-1")
        self.assertEqual(result.continuation_token, "next-page-token")


if __name__ == '__main__':
    unittest.main()