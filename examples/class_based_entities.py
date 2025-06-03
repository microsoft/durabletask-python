# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Example demonstrating class-based durable entities using the EntityBase pattern.

This example shows how to create durable entities as classes, following patterns
similar to the .NET TaskEntity implementation. This provides better organization
and type safety compared to function-based entities.
"""

import durabletask as dt
import durabletask.task as task_types
from durabletask.worker import TaskHubGrpcWorker
import logging
from datetime import datetime
from typing import Optional, Dict, List


class CounterEntity(dt.EntityBase):
    """A counter entity implemented as a class with method-based operations."""
    
    def __init__(self):
        super().__init__()
        # Initialize default state
        self._state = 0
    
    def increment(self, value: Optional[int] = None) -> int:
        """Increment the counter by the specified value (default 1)."""
        increment_by = value or 1
        current = self.get_state() or 0
        new_value = current + increment_by
        self.set_state(new_value)
        return new_value
    
    def decrement(self, value: Optional[int] = None) -> int:
        """Decrement the counter by the specified value (default 1)."""
        decrement_by = value or 1
        current = self.get_state() or 0
        new_value = current - decrement_by
        self.set_state(new_value)
        return new_value
    
    def get(self) -> int:
        """Get the current counter value."""
        return self.get_state() or 0
    
    def reset(self) -> int:
        """Reset the counter to zero."""
        self.set_state(0)
        return 0
    
    def multiply(self, factor: int) -> int:
        """Multiply the counter by a factor."""
        current = self.get_state() or 0
        new_value = current * factor
        self.set_state(new_value)
        return new_value


class ShoppingCartEntity(dt.EntityBase):
    """A shopping cart entity with rich functionality."""
    
    def __init__(self):
        super().__init__()
        self._state = {"items": [], "discounts": []}
    
    def add_item(self, item: Dict) -> int:
        """Add an item to the shopping cart."""
        cart = self.get_state() or {"items": [], "discounts": []}
        
        # Validate item structure
        if not isinstance(item, dict) or "name" not in item or "price" not in item:
            raise ValueError("Item must have 'name' and 'price' fields")
        
        cart["items"].append({
            "name": item["name"],
            "price": float(item["price"]),
            "quantity": item.get("quantity", 1),
            "added_at": datetime.utcnow().isoformat()
        })
        
        self.set_state(cart)
        return len(cart["items"])
    
    def remove_item(self, item_name: str) -> int:
        """Remove an item from the shopping cart by name."""
        cart = self.get_state() or {"items": [], "discounts": []}
        
        # Remove first matching item
        for i, item in enumerate(cart["items"]):
            if item["name"] == item_name:
                cart["items"].pop(i)
                break
        
        self.set_state(cart)
        return len(cart["items"])
    
    def get_items(self) -> List[Dict]:
        """Get all items in the cart."""
        cart = self.get_state() or {"items": [], "discounts": []}
        return cart["items"]
    
    def get_total(self) -> float:
        """Calculate the total price of items in the cart."""
        cart = self.get_state() or {"items": [], "discounts": []}
        
        # Calculate subtotal
        subtotal = sum(
            item["price"] * item.get("quantity", 1) 
            for item in cart["items"]
        )
        
        # Apply discounts
        total_discount = sum(cart.get("discounts", []))
        
        return max(0.0, subtotal - total_discount)
    
    def apply_discount(self, discount_amount: float) -> float:
        """Apply a discount to the cart."""
        cart = self.get_state() or {"items": [], "discounts": []}
        cart.setdefault("discounts", []).append(discount_amount)
        self.set_state(cart)
        return self.get_total()
    
    def clear(self) -> int:
        """Clear all items from the cart."""
        self.set_state({"items": [], "discounts": []})
        return 0


class NotificationEntity(dt.EntityBase):
    """A notification entity that demonstrates entity-to-entity communication."""
    
    def __init__(self):
        super().__init__()
        self._state = {"notifications": [], "preferences": {}}
    
    def send_notification(self, data: Dict) -> str:
        """Send a notification and update related entities."""
        user_id = data.get("user_id")
        message = data.get("message")
        notification_type = data.get("type", "info")
        
        if not user_id or not message:
            raise ValueError("user_id and message are required")
        
        # Add notification to state
        notifications = self.get_state() or {"notifications": [], "preferences": {}}
        notification = {
            "id": f"notif-{len(notifications['notifications']) + 1}",
            "user_id": user_id,
            "message": message,
            "type": notification_type,
            "timestamp": datetime.utcnow().isoformat(),
            "read": False
        }
        
        notifications["notifications"].append(notification)
        self.set_state(notifications)
        
        # Signal user's notification counter
        counter_id = dt.EntityInstanceId("Counter", f"notifications-{user_id}")
        self.signal_entity(counter_id, "increment", input=1)
        
        return notification["id"]
    
    def mark_read(self, notification_id: str) -> bool:
        """Mark a notification as read."""
        notifications = self.get_state() or {"notifications": [], "preferences": {}}
        
        for notif in notifications["notifications"]:
            if notif["id"] == notification_id:
                notif["read"] = True
                self.set_state(notifications)
                return True
        
        return False
    
    def get_unread_count(self, user_id: str) -> int:
        """Get the count of unread notifications for a user."""
        notifications = self.get_state() or {"notifications": [], "preferences": {}}
        
        return sum(
            1 for notif in notifications["notifications"]
            if notif["user_id"] == user_id and not notif["read"]
        )
    
    def get_notifications(self, user_id: str) -> List[Dict]:
        """Get all notifications for a user."""
        notifications = self.get_state() or {"notifications": [], "preferences": {}}
        
        return [
            notif for notif in notifications["notifications"]
            if notif["user_id"] == user_id
        ]


class WorkflowManagerEntity(dt.EntityBase):
    """Entity that manages and starts orchestrations."""
    
    def __init__(self):
        super().__init__()
        self._state = {"workflows": [], "stats": {"started": 0, "completed": 0}}
    
    def start_workflow(self, workflow_data: Dict) -> str:
        """Start a new workflow orchestration."""
        workflow_name = workflow_data.get("name", "default_workflow")
        workflow_input = workflow_data.get("input", {})
        custom_instance_id = workflow_data.get("instance_id")
        
        # Start the orchestration
        instance_id = self.start_new_orchestration(
            workflow_name,
            input=workflow_input,
            instance_id=custom_instance_id
        )
        
        # Track the workflow
        state = self.get_state() or {"workflows": [], "stats": {"started": 0, "completed": 0}}
        workflow_record = {
            "instance_id": instance_id,
            "name": workflow_name,
            "started_at": datetime.utcnow().isoformat(),
            "status": "started",
            "input": workflow_input
        }
        
        state["workflows"].append(workflow_record)
        state["stats"]["started"] += 1
        self.set_state(state)
        
        return instance_id
    
    def mark_completed(self, instance_id: str) -> bool:
        """Mark a workflow as completed."""
        state = self.get_state() or {"workflows": [], "stats": {"started": 0, "completed": 0}}
        
        for workflow in state["workflows"]:
            if workflow["instance_id"] == instance_id:
                workflow["status"] = "completed"
                workflow["completed_at"] = datetime.utcnow().isoformat()
                state["stats"]["completed"] += 1
                self.set_state(state)
                return True
        
        return False
    
    def get_stats(self) -> Dict:
        """Get workflow statistics."""
        state = self.get_state() or {"workflows": [], "stats": {"started": 0, "completed": 0}}
        return state["stats"]
    
    def get_workflows(self) -> List[Dict]:
        """Get all managed workflows."""
        state = self.get_state() or {"workflows": [], "stats": {"started": 0, "completed": 0}}
        return state["workflows"]


def enhanced_orchestrator(ctx: task_types.OrchestrationContext, input):
    """Orchestrator that demonstrates class-based entity interactions."""
    
    # Create entity IDs
    counter_global = dt.EntityInstanceId("Counter", "global")
    counter_user1 = dt.EntityInstanceId("Counter", "user1")
    cart_user1 = dt.EntityInstanceId("ShoppingCart", "user1")
    notification_system = dt.EntityInstanceId("Notification", "system")
    workflow_manager = dt.EntityInstanceId("WorkflowManager", "main")
    
    # Increment counters
    yield ctx.signal_entity(counter_global, "increment", input=10)
    yield ctx.signal_entity(counter_user1, "increment", input=5)
    
    # Add items to shopping cart
    yield ctx.signal_entity(cart_user1, "add_item", input={
        "name": "Premium Coffee",
        "price": 12.99,
        "quantity": 2
    })
    yield ctx.signal_entity(cart_user1, "add_item", input={
        "name": "Organic Tea",
        "price": 8.50,
        "quantity": 1
    })
    
    # Apply a discount
    yield ctx.signal_entity(cart_user1, "apply_discount", input=5.0)
    
    # Send notifications
    yield ctx.signal_entity(notification_system, "send_notification", input={
        "user_id": "user1",
        "message": "Your cart has been updated with premium items!",
        "type": "cart_update"
    })
    
    # Start a sub-workflow
    yield ctx.signal_entity(workflow_manager, "start_workflow", input={
        "name": "process_order",
        "input": {"user_id": "user1", "cart_id": "cart_user1"}
    })
    
    return "Enhanced class-based entity operations completed"


def main():
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Create and configure the worker
    worker = TaskHubGrpcWorker()
    
    # Register class-based entities
    worker._registry.add_named_entity("Counter", CounterEntity)
    worker._registry.add_named_entity("ShoppingCart", ShoppingCartEntity)
    worker._registry.add_named_entity("Notification", NotificationEntity)
    worker._registry.add_named_entity("WorkflowManager", WorkflowManagerEntity)
    
    # Register orchestrator
    worker.add_orchestrator(enhanced_orchestrator)
    
    print("Class-based entity worker example setup complete.")
    print("\nRegistered class-based entities:")
    print("- Counter: increment, decrement, get, reset, multiply operations")
    print("- ShoppingCart: add_item, remove_item, get_items, get_total, apply_discount, clear operations")
    print("- Notification: send_notification, mark_read, get_unread_count, get_notifications operations")
    print("- WorkflowManager: start_workflow, mark_completed, get_stats, get_workflows operations")
    print("\nAdvanced features demonstrated:")
    print("- Class-based entity implementation with EntityBase")
    print("- Method-based operation dispatch")
    print("- Type hints and parameter validation")
    print("- Rich state management")
    print("- Entity-to-entity communication")
    print("- Orchestration management from entities")
    print("- Automatic context injection")
    
    # Example usage patterns
    print("\nExample usage patterns:")
    print("1. Create instances with default state")
    print("2. Use method names as operation names")
    print("3. Automatic parameter binding (context injection)")
    print("4. Type-safe entity operations")
    print("5. Rich business logic in entity methods")


if __name__ == "__main__":
    main()