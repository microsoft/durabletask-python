# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Example demonstrating durable entities usage.

This example shows how to create and use durable entities with the Python SDK.
Entities are stateful objects that can maintain state across multiple operations.
"""

import durabletask.task as dt
from durabletask.worker import TaskHubGrpcWorker
import logging
from datetime import datetime


def counter_entity(ctx: dt.EntityContext, input) -> int:
    """A simple counter entity that can increment, decrement, get, and reset."""

    if ctx.operation_name == "increment":
        current_count = ctx.get_state() or 0
        increment_by = input or 1
        new_count = current_count + increment_by
        ctx.set_state(new_count)
        return new_count

    elif ctx.operation_name == "decrement":
        current_count = ctx.get_state() or 0
        decrement_by = input or 1
        new_count = current_count - decrement_by
        ctx.set_state(new_count)
        return new_count

    elif ctx.operation_name == "get":
        return ctx.get_state() or 0

    elif ctx.operation_name == "reset":
        ctx.set_state(0)
        return 0

    else:
        raise ValueError(f"Unknown operation: {ctx.operation_name}")


def shopping_cart_entity(ctx: dt.EntityContext, input):
    """A shopping cart entity that can add/remove items and calculate totals."""

    if ctx.operation_name == "add_item":
        cart = ctx.get_state() or {"items": []}
        cart["items"].append(input)
        ctx.set_state(cart)
        return len(cart["items"])

    elif ctx.operation_name == "remove_item":
        cart = ctx.get_state() or {"items": []}
        if input in cart["items"]:
            cart["items"].remove(input)
            ctx.set_state(cart)
        return len(cart["items"])

    elif ctx.operation_name == "get_items":
        cart = ctx.get_state() or {"items": []}
        return cart["items"]

    elif ctx.operation_name == "get_total":
        cart = ctx.get_state() or {"items": []}
        # Simple total calculation assuming each item has a 'price' field
        total = sum(item.get("price", 0) for item in cart["items"] if isinstance(item, dict))
        return total

    elif ctx.operation_name == "clear":
        ctx.set_state({"items": []})
        return 0

    else:
        raise ValueError(f"Unknown operation: {ctx.operation_name}")


def notification_entity(ctx: dt.EntityContext, input):
    """A notification entity that demonstrates entity-to-entity communication."""
    
    if ctx.operation_name == "notify_user":
        # Get the user ID and message from input
        user_id = input.get("user_id")
        message = input.get("message")
        
        # Get current notifications
        notifications = ctx.get_state() or {"notifications": []}
        
        # Add new notification
        notification = {
            "message": message,
            "timestamp": datetime.utcnow().isoformat(),
            "user_id": user_id
        }
        notifications["notifications"].append(notification)
        ctx.set_state(notifications)
        
        # Signal the user's counter to increment notification count
        if user_id:
            counter_entity_id = dt.EntityInstanceId("Counter", f"notifications-{user_id}")
            ctx.signal_entity(counter_entity_id, "increment", input=1)
        
        return len(notifications["notifications"])
    
    elif ctx.operation_name == "get_notifications":
        notifications = ctx.get_state() or {"notifications": []}
        return notifications["notifications"]
    
    elif ctx.operation_name == "clear":
        ctx.set_state({"notifications": []})
        return 0
    
    else:
        raise ValueError(f"Unknown operation: {ctx.operation_name}")


def orchestration_starter_entity(ctx: dt.EntityContext, input):
    """Entity that demonstrates starting orchestrations from entity operations."""
    
    if ctx.operation_name == "start_workflow":
        workflow_name = input.get("workflow_name", "entity_orchestrator")
        workflow_input = input.get("workflow_input")
        
        # Start a new orchestration
        instance_id = ctx.start_new_orchestration(workflow_name, input=workflow_input)
        
        # Update state to track started workflows
        state = ctx.get_state() or {"started_workflows": []}
        state["started_workflows"].append({
            "instance_id": instance_id,
            "workflow_name": workflow_name,
            "started_at": datetime.utcnow().isoformat()
        })
        ctx.set_state(state)
        
        return instance_id
    
    elif ctx.operation_name == "get_workflows":
        state = ctx.get_state() or {"started_workflows": []}
        return state["started_workflows"]
    
    else:
        raise ValueError(f"Unknown operation: {ctx.operation_name}")


def entity_orchestrator(ctx: dt.OrchestrationContext, input):
    """Orchestrator that demonstrates entity interactions."""

    # Using structured EntityInstanceId for better type safety
    counter_global = dt.EntityInstanceId("Counter", "global")
    counter_user1 = dt.EntityInstanceId("Counter", "user1")
    counter_user2 = dt.EntityInstanceId("Counter", "user2")
    cart_user1 = dt.EntityInstanceId("ShoppingCart", "user1")

    # Signal entities (fire-and-forget)
    yield ctx.signal_entity(counter_global, "increment", input=5)
    yield ctx.signal_entity(counter_user1, "increment", input=1)
    yield ctx.signal_entity(counter_user2, "increment", input=2)

    # Add items to shopping cart
    yield ctx.signal_entity(cart_user1, "add_item",
                          input={"name": "Apple", "price": 1.50})
    yield ctx.signal_entity(cart_user1, "add_item",
                          input={"name": "Banana", "price": 0.75})

    # Demonstrate notification system
    notification_entity_id = dt.EntityInstanceId("Notification", "system")
    yield ctx.signal_entity(notification_entity_id, "notify_user",
                          input={"user_id": "user1", "message": "Your cart has been updated!"})

    return "Entity operations completed"


def main():
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Create and configure the worker
    worker = TaskHubGrpcWorker()

    # Register entities - entities should be registered by their intended name
    # Since entity execution extracts the entity type from the instance ID (e.g., "Counter@key1")
    # we need to register them with the exact name that will be used in instance IDs
    worker._registry.add_named_entity("Counter", counter_entity)
    worker._registry.add_named_entity("ShoppingCart", shopping_cart_entity)
    worker._registry.add_named_entity("Notification", notification_entity)
    worker._registry.add_named_entity("OrchestrationStarter", orchestration_starter_entity)

    # Register orchestrator
    worker.add_orchestrator(entity_orchestrator)

    print("Enhanced entity worker example setup complete.")
    print("\nRegistered entities:")
    print("- Counter: supports increment, decrement, get, reset operations")
    print("- ShoppingCart: supports add_item, remove_item, get_items, get_total, clear operations")
    print("- Notification: supports notify_user, get_notifications, clear operations")
    print("- OrchestrationStarter: supports start_workflow, get_workflows operations")
    print("\nFeatures demonstrated:")
    print("- Entity-to-entity communication via signal_entity")
    print("- Starting orchestrations from entity operations")
    print("- Structured EntityInstanceId for type safety")
    print("- Complex entity state management")
    print("\nTo use entities, you would:")
    print("1. Start the worker: worker.start()")
    print("2. Use a client to signal entities or start orchestrations")
    print("3. Query entity state using client.get_entity()")

    # Example client usage (commented out since it requires a running sidecar)
    """
    # Create client
    client = TaskHubGrpcClient()

    # Start an orchestration that uses entities
    instance_id = client.schedule_new_orchestration(entity_orchestrator)
    print(f"Started orchestration: {instance_id}")

    # Signal entities directly using structured IDs
    counter_id = dt.EntityInstanceId("Counter", "test")
    client.signal_entity(counter_id, "increment", input=10)
    client.signal_entity(counter_id, "increment", input=5)

    # Query entity state
    counter_state = client.get_entity(counter_id, include_state=True)
    if counter_state:
        print(f"Counter state: {counter_state.serialized_state}")

    # Query entities
    query = dt.EntityQuery(instance_id_starts_with="Counter@", include_state=True)
    results = client.query_entities(query)
    print(f"Found {len(results.entities)} counter entities")

    # Test notification system
    notification_id = dt.EntityInstanceId("Notification", "system")
    client.signal_entity(notification_id, "notify_user", 
                        input={"user_id": "user1", "message": "Welcome to the system!"})

    # Test orchestration starter
    starter_id = dt.EntityInstanceId("OrchestrationStarter", "main")
    client.signal_entity(starter_id, "start_workflow", 
                        input={"workflow_name": "entity_orchestrator", "workflow_input": {"test": True}})
    """


if __name__ == "__main__":
    main()