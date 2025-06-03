# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import unittest
from datetime import datetime
from durabletask import task
from durabletask import worker as task_worker


class TestEntityTypes(unittest.TestCase):

    def test_entity_context_creation(self):
        """Test that EntityContext can be created with basic properties."""
        ctx = task.EntityContext("Counter@test-entity-1", "increment", is_new_entity=True)

        self.assertEqual(ctx.instance_id, "Counter@test-entity-1")
        self.assertEqual(ctx.operation_name, "increment")
        self.assertTrue(ctx.is_new_entity)
        self.assertIsNone(ctx.get_state())

    def test_entity_context_state_management(self):
        """Test that EntityContext can manage state."""
        ctx = task.EntityContext("Counter@test-entity-1", "increment")

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


class TestEntityWorkerIntegration(unittest.TestCase):

    def test_worker_entity_registration(self):
        """Test that entities can be registered with the worker."""
        worker = task_worker.TaskHubGrpcWorker()

        def counter_entity(ctx: task.EntityContext, input):
            if ctx.operation_name == "increment":
                current_count = ctx.get_state() or 0
                new_count = current_count + (input or 1)
                ctx.set_state(new_count)
                return new_count
            elif ctx.operation_name == "get":
                return ctx.get_state() or 0
            elif ctx.operation_name == "reset":
                ctx.set_state(0)
                return 0

        # Test registration
        entity_name = worker.add_entity(counter_entity)
        self.assertEqual(entity_name, "counter_entity")

        # Test that entity is in registry
        self.assertIsNotNone(worker._registry.get_entity("counter_entity"))

        # Test error for duplicate registration
        with self.assertRaises(ValueError):
            worker.add_entity(counter_entity)

    def test_entity_execution(self):
        """Test entity execution via the EntityExecutor."""
        from durabletask.worker import _Registry, _EntityExecutor
        import durabletask.internal.orchestrator_service_pb2 as pb
        import durabletask.internal.helpers as ph
        import logging

        # Create registry and register entity
        registry = _Registry()

        def counter_entity(ctx: task.EntityContext, input):
            if ctx.operation_name == "increment":
                current_count = ctx.get_state() or 0
                new_count = current_count + (input or 1)
                ctx.set_state(new_count)
                return new_count
            elif ctx.operation_name == "get":
                return ctx.get_state() or 0

        # Register the entity with a specific name
        registry.add_named_entity("Counter", counter_entity)

        # Create executor
        logger = logging.getLogger("test")
        executor = _EntityExecutor(registry, logger)

        # Create test request
        req = pb.EntityBatchRequest()
        req.instanceId = "Counter@test-key"  # Instance ID with entity type prefix matching registration
        req.entityState.CopyFrom(ph.get_string_value("0"))  # Initial state

        # Add increment operation
        operation = pb.OperationRequest()
        operation.operation = "increment"
        operation.input.CopyFrom(ph.get_string_value("5"))
        req.operations.append(operation)

        # Execute
        result = executor.execute(req)

        # Verify result
        self.assertEqual(len(result.results), 1)
        self.assertTrue(result.results[0].HasField("success"))
        self.assertEqual(result.results[0].success.result.value, "5")
        self.assertEqual(result.entityState.value, "5")

    def test_entity_instance_id(self):
        """Test that EntityInstanceId works correctly."""
        # Create from name and key
        entity_id = task.EntityInstanceId("Counter", "user1")
        self.assertEqual(entity_id.name, "Counter")
        self.assertEqual(entity_id.key, "user1")
        self.assertEqual(str(entity_id), "Counter@user1")

        # Parse from string
        parsed_id = task.EntityInstanceId.from_string("ShoppingCart@user2")
        self.assertEqual(parsed_id.name, "ShoppingCart")
        self.assertEqual(parsed_id.key, "user2")

        # Test invalid formats
        with self.assertRaises(ValueError):
            task.EntityInstanceId.from_string("invalid")
        
        with self.assertRaises(ValueError):
            task.EntityInstanceId.from_string("@")
        
        with self.assertRaises(ValueError):
            task.EntityInstanceId.from_string("name@")

    def test_entity_context_entity_id_property(self):
        """Test that EntityContext provides structured entity ID."""
        ctx = task.EntityContext("Counter@test-user", "increment")
        
        self.assertEqual(ctx.entity_id.name, "Counter")
        self.assertEqual(ctx.entity_id.key, "test-user")
        self.assertEqual(str(ctx.entity_id), "Counter@test-user")

    def test_entity_context_signal_entity(self):
        """Test that EntityContext can signal other entities."""
        ctx = task.EntityContext("Notification@system", "notify_user")
        
        # Signal using string
        ctx.signal_entity("Counter@user1", "increment", input=5)
        
        # Signal using EntityInstanceId
        counter_id = task.EntityInstanceId("Counter", "user2")
        ctx.signal_entity(counter_id, "increment", input=10)
        
        # Check signals were stored
        self.assertTrue(hasattr(ctx, '_signals'))
        self.assertEqual(len(ctx._signals), 2)
        
        self.assertEqual(ctx._signals[0]['entity_id'], "Counter@user1")
        self.assertEqual(ctx._signals[0]['operation_name'], "increment")
        self.assertEqual(ctx._signals[0]['input'], 5)
        
        self.assertEqual(ctx._signals[1]['entity_id'], "Counter@user2")
        self.assertEqual(ctx._signals[1]['operation_name'], "increment")
        self.assertEqual(ctx._signals[1]['input'], 10)

    def test_entity_context_start_orchestration(self):
        """Test that EntityContext can start orchestrations."""
        ctx = task.EntityContext("OrchestrationStarter@main", "start_workflow")
        
        # Start orchestration with custom instance ID
        instance_id = ctx.start_new_orchestration(
            "test_orchestrator", 
            input={"test": True}, 
            instance_id="custom-instance-123"
        )
        
        self.assertEqual(instance_id, "custom-instance-123")
        
        # Check orchestration was stored
        self.assertTrue(hasattr(ctx, '_orchestrations'))
        self.assertEqual(len(ctx._orchestrations), 1)
        
        orch = ctx._orchestrations[0]
        self.assertEqual(orch['name'], "test_orchestrator")
        self.assertEqual(orch['input'], {"test": True})
        self.assertEqual(orch['instance_id'], "custom-instance-123")

    def test_entity_operation_failed_exception(self):
        """Test EntityOperationFailedException."""
        entity_id = task.EntityInstanceId("Counter", "test")
        failure_details = task.FailureDetails("Test error", "ValueError", "stack trace")
        
        ex = task.EntityOperationFailedException(entity_id, "increment", failure_details)
        
        self.assertEqual(ex.entity_id, entity_id)
        self.assertEqual(ex.operation_name, "increment")
        self.assertEqual(ex.failure_details, failure_details)
        self.assertIn("increment", str(ex))
        self.assertIn("Counter@test", str(ex))

if __name__ == '__main__':
    unittest.main()