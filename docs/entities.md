# Durable Entities Guide

This guide covers the comprehensive durable entities support in the Python SDK, bringing feature parity with other Durable Task SDKs.

## What are Durable Entities?

Durable entities are stateful objects that can maintain state across multiple operations. Each entity has a unique entity ID and can handle various operations that read and modify its state. Entities are accessed using the format `EntityType@EntityKey` (e.g., `Counter@user1`).

## Key Features

### Entity Functions (Basic Implementation)

Register entity functions that handle operations and maintain state:

```python
import durabletask as dt

def counter_entity(ctx: dt.EntityContext, input):
    if ctx.operation_name == "increment":
        current_count = ctx.get_state() or 0
        new_count = current_count + (input or 1)
        ctx.set_state(new_count)
        return new_count
    elif ctx.operation_name == "get":
        return ctx.get_state() or 0

# Register with worker
worker = TaskHubGrpcWorker()
worker._registry.add_named_entity("Counter", counter_entity)
```

### Class-Based Entities (Advanced Implementation)

For more complex entities, use the `EntityBase` class with method-based dispatch:

```python
import durabletask as dt

class CounterEntity(dt.EntityBase):
    def __init__(self):
        super().__init__()
        self._state = 0
    
    def increment(self, value: int = 1) -> int:
        """Increment the counter by the specified value."""
        current = self.get_state() or 0
        new_value = current + value
        self.set_state(new_value)
        return new_value
    
    def get(self) -> int:
        """Get the current counter value."""
        return self.get_state() or 0
    
    def reset(self) -> int:
        """Reset the counter to zero."""
        self.set_state(0)
        return 0

# Register class-based entity
worker._registry.add_named_entity("Counter", CounterEntity)
```

### Client Operations

Signal entities, query state, and manage entity storage:

```python
# Create client
client = TaskHubGrpcClient()

# Signal an entity using string ID
client.signal_entity("Counter@my-counter", "increment", input=5)

# Signal an entity using structured ID (recommended)
counter_id = dt.EntityInstanceId("Counter", "my-counter")
client.signal_entity(counter_id, "increment", input=5)

# Query entity state
entity_state = client.get_entity(counter_id, include_state=True)
if entity_state and entity_state.exists:
    print(f"Counter value: {entity_state.serialized_state}")

# Query multiple entities
query = dt.EntityQuery(instance_id_starts_with="Counter@", include_state=True)
results = client.query_entities(query)
print(f"Found {len(results.entities)} counter entities")

# Clean entity storage
removed, released, token = client.clean_entity_storage()
```

### Orchestration Integration

Signal entities from orchestrations:

```python
def my_orchestrator(ctx: dt.OrchestrationContext, input):
    # Signal entities (fire-and-forget)
    counter_id = dt.EntityInstanceId("Counter", "global")
    yield ctx.signal_entity(counter_id, "increment", input=5)
    
    cart_id = dt.EntityInstanceId("ShoppingCart", "user1")
    yield ctx.signal_entity(cart_id, "add_item", 
                           input={"name": "Apple", "price": 1.50})
    return "Entity operations completed"
```

### Entity-to-Entity Communication

Entities can signal other entities and start orchestrations:

```python
class NotificationEntity(dt.EntityBase):
    def send_notification(self, data):
        user_id = data["user_id"]
        message = data["message"]
        
        # Store notification
        notifications = self.get_state() or {"notifications": []}
        notifications["notifications"].append({
            "user_id": user_id,
            "message": message,
            "timestamp": datetime.utcnow().isoformat()
        })
        self.set_state(notifications)
        
        # Signal user's notification counter
        counter_id = dt.EntityInstanceId("Counter", f"notifications-{user_id}")
        self.signal_entity(counter_id, "increment", input=1)
        
        # Start a notification processing workflow
        workflow_id = self.start_new_orchestration(
            "process_notification",
            input={"user_id": user_id, "message": message}
        )
        
        return workflow_id
```

## Entity ID Structure

Use `EntityInstanceId` for type-safe entity references:

```python
# Create structured entity ID
entity_id = dt.EntityInstanceId("Counter", "user123")
print(entity_id.name)  # "Counter"
print(entity_id.key)   # "user123"
print(str(entity_id))  # "Counter@user123"

# Parse from string
parsed_id = dt.EntityInstanceId.from_string("ShoppingCart@cart1")
```

## Error Handling

Handle entity operation failures with specialized exceptions:

```python
try:
    client.signal_entity("NonExistent@entity", "operation")
except dt.EntityOperationFailedException as ex:
    print(f"Entity operation failed: {ex.failure_details.message}")
    print(f"Failed entity: {ex.entity_id}")
    print(f"Failed operation: {ex.operation_name}")
```

## Entity Context Features

The `EntityContext` provides rich functionality:

```python
def advanced_entity(ctx: dt.EntityContext, input):
    # Access entity information
    print(f"Entity ID: {ctx.instance_id}")
    print(f"Entity name: {ctx.entity_id.name}")
    print(f"Entity key: {ctx.entity_id.key}")
    print(f"Operation: {ctx.operation_name}")
    print(f"Is new: {ctx.is_new_entity}")
    
    # State management
    current_state = ctx.get_state()
    ctx.set_state({"updated": True, "input": input})
    
    # Signal other entities
    ctx.signal_entity("Logger@system", "log", 
                     input=f"Operation {ctx.operation_name} executed")
    
    # Start orchestrations
    workflow_id = ctx.start_new_orchestration("cleanup_workflow")
    
    return {"workflow_id": workflow_id}
```

## Best Practices

### 1. Use Structured Entity IDs

```python
# ✅ Good - Type-safe and clear
counter_id = dt.EntityInstanceId("Counter", "user123")
client.signal_entity(counter_id, "increment")

# ❌ Avoid - Error-prone string concatenation
client.signal_entity("Counter@user123", "increment")
```

### 2. Implement Rich Entity Classes

```python
# ✅ Good - Clear separation of concerns
class ShoppingCartEntity(dt.EntityBase):
    def add_item(self, item: dict) -> int:
        # Validation
        if not item.get("name") or not item.get("price"):
            raise ValueError("Item must have name and price")
        
        # Business logic
        cart = self.get_state() or {"items": []}
        cart["items"].append(item)
        self.set_state(cart)
        
        return len(cart["items"])
    
    def get_total(self) -> float:
        cart = self.get_state() or {"items": []}
        return sum(item["price"] for item in cart["items"])
```

### 3. Handle State Initialization

```python
class StatefulEntity(dt.EntityBase):
    def __init__(self):
        super().__init__()
        # Set default state structure
        self._state = {"initialized": True, "value": 0}
    
    def ensure_initialized(self):
        if not self.get_state():
            self.set_state({"initialized": True, "value": 0})
```

### 4. Use Type Hints

```python
from typing import Dict, List, Optional

class TypedEntity(dt.EntityBase):
    def process_order(self, order_data: Dict[str, any]) -> str:
        """Process an order and return order ID."""
        order_id = f"order-{len(self.get_orders())}"
        self.add_order(order_data)
        return order_id
    
    def get_orders(self) -> List[Dict]:
        """Get all orders."""
        state = self.get_state() or {"orders": []}
        return state["orders"]
```

## Examples

- **Basic entities**: See [`examples/durable_entities.py`](examples/durable_entities.py)
- **Class-based entities**: See [`examples/class_based_entities.py`](examples/class_based_entities.py)

## Comparison with .NET Implementation

This Python implementation provides feature parity with the .NET DurableTask SDK:

| Feature | .NET | Python | Status |
|---------|------|--------|--------|
| Function-based entities | ✅ | ✅ | Complete |
| Class-based entities | ✅ | ✅ | Complete |
| Method dispatch | ✅ | ✅ | Complete |
| Structured entity IDs | ✅ | ✅ | Complete |
| Entity-to-entity signals | ✅ | ✅ | Complete |
| Orchestration starting | ✅ | ✅ | Complete |
| State management | ✅ | ✅ | Complete |
| Error handling | ✅ | ✅ | Complete |
| Client operations | ✅ | ✅ | Complete |
| Entity locking | ✅ | ✅ | Complete |

The Python implementation follows the same patterns and provides equivalent functionality to ensure consistency across Durable Task SDKs.