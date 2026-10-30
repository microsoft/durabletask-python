# Supported patterns

The following orchestration patterns are currently supported.

Generator and native coroutine orchestrators share these durable patterns.
See the [async orchestrator contract](async-orchestrators.md) for supported
asyncio/AnyIO composition, durable clocks/timeouts, and dispatch-local cleanup.

## Function chaining

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

## Cleanup in orchestrator finally blocks

The SDK closes an orchestrator generator at the end of every dispatch, including
when it is waiting for durable work. Closing injects `GeneratorExit`, running
active `finally` blocks and context-manager exits. Replay creates a fresh
generator, so this cleanup may run many times before the orchestration completes.

Use these hooks to release dispatch-local resources, not as orchestration-completion
or exactly-once compensation hooks. Do not release resources that an activity
still needs: that activity may still be running when the dispatch ends.

The response is built before this forced teardown. Durable actions, timer
cancellation, custom-status changes, and `continue_as_new()` issued only during
teardown do not change that response. Keep workflow-level cleanup in normal
orchestration control flow, as a yielded activity:

```python
from io import StringIO
from typing import Any, Generator
from durabletask import task


def orchestrator(
    ctx: task.OrchestrationContext, value: str,
) -> Generator[task.Task[Any], Any, Any]:
    scratch = StringIO(value)
    try:
        result = yield ctx.call_activity("process", input=scratch.getvalue())
    finally:
        scratch.close()  # Local buffer cleanup runs at each dispatch boundary.

    yield ctx.call_activity("cleanup_workflow", input=value)
    return result
```

`ctx.is_replaying` is normally false after new events are processed. Neither
`not ctx.is_replaying` nor replay-safe logging makes teardown completion-only.
Yielding during forced close raises `RuntimeError: generator ignored GeneratorExit`;
the SDK logs a warning, but the generator remains open and may retain resources.
A `finally` reached through normal execution, rather than forced close, can
still yield durable work.

Garbage collection could already trigger finalization; completion-only timing
was never guaranteed. Explicit dispatch teardown does not guarantee cleanup
across process failure.

Unexpected non-cancellation `BaseException` exits from orchestration execution,
result serialization, or forced generator teardown fail the orchestration with the exception's type,
message, and stack trace. An existing orchestration failure is retained if
teardown also fails. Outstanding acquired entity locks are released with the
failed response; durable work and status changes made only by teardown are not
committed.

The gRPC worker does not turn `asyncio.CancelledError`, `GeneratorExit`,
`SystemExit`, or `KeyboardInterrupt` into orchestration failures. This also applies
to nested exception groups containing these signals on Python 3.11 and later.
The existing warning-only handling of direct `asyncio.CancelledError` during
forced teardown is unchanged. Other cancellation or process-control exits
propagate after the worker attempts to abandon the accepted dispatch.
Native coroutine authoring distinguishes a cancelled local root task: an
unhandled local cancellation becomes `TaskCancelledError` and fails the
orchestration instead of repeatedly abandoning replay. It does not terminate
the durable instance; see the [async cancellation contract](async-orchestrators.md#failures-timeouts-and-cancellation).

If a completion or abandonment RPC raises, its server-side outcome may be
unknown. The worker logs the uncertainty and closes the dispatch's originating
work-item stream so the scheduler can recover ownership. It does not retry an
uncertain RPC or send a conflicting completion or abandonment. Other work owned
by that stream can also be redelivered. This is not an exactly-once execution
guarantee or a bounded recovery-time guarantee. Cancelling an asyncio await of a
still-running executor thread does not abandon that thread's dispatch.

## Fan-out/fan-in

An orchestration can fan-out a dynamic number of function calls in parallel and then fan-in the
results using the following syntax:

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

Tasks can be shared between `when_all` and `when_any` groups without scheduling
the activity again. `when_all` preserves input order and every duplicate position:
`when_all([work, work])` returns the result twice, whether `work` is pending or
already complete. If a child fails, `when_all` waits for every child before raising
the first observed failure.

`when_any` returns its winning direct child, including when that child is itself
a composite. It stops observing losing children but does not cancel them or
remove other groups' subscriptions. Completion notifications follow group
subscription order, so shared nested races can select a composite rather than
the leaf task that triggered it.

> [!WARNING]
> Corrected shared-task notifications can change race winners during replay of
> existing instances. Drain affected instances on the previous SDK or isolate
> new instances in a separate task hub before upgrading. See the
> [core changelog](../CHANGELOG.md) for migration guidance.

A canceled task still raises `TaskCancelledError` when its result is read.
If collecting `when_all` results throws, that group does not notify its own
parents; do not catch the error and reuse the group. Other groups observing
the same child are still notified. A single completion callback error is
re-raised unchanged; multiple callback errors are collected in one exception
whose `errors` tuple retains each original exception.

## Human interaction and durable timers

An orchestration can wait for a user-defined event, such as a human approval event, before proceeding
to the next step. In addition, the orchestration can create a timer with an arbitrary duration that
triggers some alternate action if the external event hasn't been received:

```python
def purchase_order_workflow(ctx: task.OrchestrationContext, order: Order):
    """Orchestrator function that represents a purchase order workflow"""
    # Orders under $1000 are auto-approved
    if order.Cost < 1000:
        return "Auto-approved"

    # Orders of $1000 or more require manager approval
    yield ctx.call_activity(send_approval_request, input=order)

    # Approvals must be received within 24 hours or they will be cancelled.
    # Passing ``data_type`` reconstructs the event payload as an ``Approval``.
    approval_event = ctx.wait_for_external_event("approval_received", data_type=Approval)
    timeout_event = ctx.create_timer(timedelta(hours=24))
    winner = yield task.when_any([approval_event, timeout_event])
    if winner == timeout_event:
        return "Cancelled"

    # The order was approved
    yield ctx.call_activity(place_order, input=order)
    approval_details = approval_event.get_result()
    return f"Approved by '{approval_details.approver}'"
```

As an aside, you'll also notice that the example orchestration above works with custom business
objects. Custom classes, data classes, and named tuples are serialized to plain JSON automatically.
To reconstruct the original type on the receiving side, supply the type — for example via the
`data_type` argument to `wait_for_external_event` (shown above), the `return_type` argument to
`call_activity` / `call_sub_orchestrator` / `call_entity`, or by annotating the consuming function's
input parameter. Without a type, the payload is returned as plain JSON (a `dict` or `list`).

See the full [human interaction sample](../examples/human_interaction.py).

## Version-aware orchestrator

When utilizing orchestration versioning, it is possible for an orchestrator to remain
backwards-compatible with orchestrations created using the previously defined version. For instance,
consider an orchestration defined with the following signature:

```python
def my_orchestrator(ctx: task.OrchestrationContext, order: Order):
    """Dummy orchestrator function illustrating old logic"""
    yield ctx.call_activity(activity_one)
    yield ctx.call_activity(activity_two)
    return "Success"
```

Assume that any orchestrations created using this orchestrator were versioned 1.0.0. If the
signature of this method needs to be updated to call activity_three between the calls to
activity_one and activity_two, ordinarily this would break any running orchestrations at the time of
deployment. However, the following orchestrator will be able to process both orchestrations versioned
1.0.0 and 2.0.0 after the change:

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

## Work item filtering

When running multiple workers against the same task hub, each worker can declare which work items it
handles. The backend then dispatches only the matching orchestrations, activities, and entities,
avoiding unnecessary round-trips. Filtering is opt-in and supports both auto-generated and explicit
filter sets.

The simplest approach auto-generates filters from the worker's registry:

```python
with DurableTaskSchedulerWorker(...) as w:
    w.add_orchestrator(greeting_orchestrator)
    w.add_activity(greet)
    w.use_work_item_filters()  # auto-generate from registry
    w.start()
```

For more control you can provide explicit filters, including version constraints:

```python
from durabletask.worker import (
    WorkItemFilters,
    OrchestrationWorkItemFilter,
    ActivityWorkItemFilter,
)

w.use_work_item_filters(WorkItemFilters(
    orchestrations=[
        OrchestrationWorkItemFilter(
            name="greeting_orchestrator",
            versions=["2.0.0"],
        ),
    ],
    activities=[
        ActivityWorkItemFilter(name="greet"),
    ],
))
```

See the full [work item filtering sample](../examples/work_item_filtering.py).

## Large payload externalization

When orchestrations work with very large inputs, outputs, or event data, the payloads can exceed
gRPC message size limits. The large payload externalization pattern transparently offloads these
payloads to Azure Blob Storage and replaces them with compact reference tokens in the gRPC messages.

No changes are required in orchestrator or activity code. Simply install the optional dependency and
configure a payload store on the worker and client:

```python
from durabletask.extensions.azure_blob_payloads import BlobPayloadStore, BlobPayloadStoreOptions
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker

# Configure the blob payload store
store = BlobPayloadStore(BlobPayloadStoreOptions(
    connection_string="DefaultEndpointsProtocol=https;...",
))

# Pass the store to both worker and client
with DurableTaskSchedulerWorker(
    host_address=endpoint, secure_channel=secure_channel,
    taskhub=taskhub_name, token_credential=credential,
    payload_store=store,
) as w:
    w.add_orchestrator(my_orchestrator)
    w.add_activity(process_large_data)
    w.start()

    c = DurableTaskSchedulerClient(
        host_address=endpoint, secure_channel=secure_channel,
        taskhub=taskhub_name, token_credential=credential,
        payload_store=store,
    )

    # This large input is automatically externalized to blob storage
    large_input = "x" * 1_000_000  # 1 MB string
    instance_id = c.schedule_new_orchestration(my_orchestrator, input=large_input)
    state = c.wait_for_orchestration_completion(instance_id, timeout=60)
```

In this example, any payload exceeding the threshold (default 256 KiB) is compressed and uploaded to
the configured Azure Blob container. When the worker or client reads the message, it downloads and
decompresses the payload automatically.

See the full [large payload example](../examples/large_payload/) and [feature
documentation](./features.md#large-payload-externalization) for configuration options and details.
