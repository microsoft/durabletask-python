# Supported patterns

The following orchestration patterns are currently supported.

### Function chaining

An orchestration can chain a sequence of function calls using the following syntax:

```python
# simple activity function that returns a greeting
def hello(ctx: task.ActivityContext, name: str) -> str:
    return f'Hello {name}!'

# orchestrator function that sequences the activity calls
def sequence(ctx: task.OrchestrationContext, _):
    result1 = yield ctx.call_activity(hello, input='Tokyo')
    result2 = yield ctx.call_activity(hello, input='Seattle')
    result3 = yield ctx.call_activity(hello, input='London')

    return [result1, result2, result3]
```

See the full [function chaining example](../examples/activity_sequence.py).

### Fan-out/fan-in

An orchestration can fan-out a dynamic number of function calls in parallel and then fan-in the results using the following syntax:

```python
# activity function for getting the list of work items
def get_work_items(ctx: task.ActivityContext, _) -> List[str]:
    # ...

# activity function for processing a single work item
def process_work_item(ctx: task.ActivityContext, item: str) -> int:
    # ...

# orchestrator function that fans-out the work items and then fans-in the results
def orchestrator(ctx: task.OrchestrationContext, _):
    # the number of work-items is unknown in advance
    work_items = yield ctx.call_activity(get_work_items)

    # fan-out: schedule the work items in parallel and wait for all of them to complete
    tasks = [ctx.call_activity(process_work_item, input=item) for item in work_items]
    results = yield task.when_all(tasks)

    # fan-in: summarize and return the results
    return {'work_items': work_items, 'results': results, 'total': sum(results)}
```

See the full [fan-out sample](../examples/fanout_fanin.py).

### Human interaction and durable timers

An orchestration can wait for a user-defined event, such as a human approval event, before proceding to the next step. In addition, the orchestration can create a timer with an arbitrary duration that triggers some alternate action if the external event hasn't been received:

```python
def purchase_order_workflow(ctx: task.OrchestrationContext, order: Order):
    """Orchestrator function that represents a purchase order workflow"""
    # Orders under $1000 are auto-approved
    if order.Cost < 1000:
        return "Auto-approved"

    # Orders of $1000 or more require manager approval
    yield ctx.call_activity(send_approval_request, input=order)

    # Approvals must be received within 24 hours or they will be canceled.
    approval_event = ctx.wait_for_external_event("approval_received")
    timeout_event = ctx.create_timer(timedelta(hours=24))
    winner = yield task.when_any([approval_event, timeout_event])
    if winner == timeout_event:
        return "Canceled"

    # The order was approved
    yield ctx.call_activity(place_order, input=order)
    approval_details = approval_event.get_result()
    return f"Approved by '{approval_details.approver}'"
```

As an aside, you'll also notice that the example orchestration above works with custom business objects. Support for custom business objects includes support for custom classes, custom data classes, and named tuples. Serialization and deserialization of these objects is handled automatically by the SDK.

See the full [human interaction sample](../examples/human_interaction.py).

### Version-aware orchestrator

When utilizing orchestration versioning, it is possible for an orchestrator to remain backwards-compatible with orchestrations created using the previously defined version. For instance, consider an orchestration defined with the following signature:

```python
def my_orchestrator(ctx: task.OrchestrationContext, order: Order):
    """Dummy orchestrator function illustrating old logic"""
    yield ctx.call_activity(activity_one)
    yield ctx.call_activity(activity_two) 
    return "Success"
```

Assume that any orchestrations created using this orchestrator were versioned 1.0.0. If the signature of this method needs to be updated to call activity_three between the calls to activity_one and activity_two, ordinarily this would break any running orchestrations at the time of deployment. However, the following orchestrator will be able to process both orchestraions versioned 1.0.0 and 2.0.0 after the change:

```python
def my_orchestrator(ctx: task.OrchestrationContext, order: Order):
    """Version-aware dummy orchestrator capable of processing both old and new orchestrations"""
    yield ctx.call_activity(activity_one)
    if ctx.version > '1.0.0':
        yield ctx.call_activity(activity_three)
    yield ctx.call_activity(activity_two) 
```

Alternatively, if the orchestrator changes completely, the following syntax might be preferred:

```python
def my_orchestrator(ctx: task.OrchestrationContext, order: Order):
    if ctx.version == '1.0.0':
        yield ctx.call_activity(activity_one)
        yield ctx.call_activity(activity_two)
        return "Success
    yield ctx.call_activity(activity_one)
    yield ctx.call_activity(activity_three)
    yield ctx.call_activity(activity_two) 
    return "Success"        
```

See the full [version-aware orchestrator sample](../examples/version_aware_orchestrator.py)