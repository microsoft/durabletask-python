# Async orchestrators

All three packages accept native `async def` orchestrators alongside existing
synchronous generator orchestrators. Both styles use the same history, action,
retry, payload, and entity engine. Register the function normally with
`TaskHubGrpcWorker`, `DurableTaskSchedulerWorker`, or the Functions
`orchestration_trigger` decorator. Functions supports both native two-argument
and compatibility single-argument orchestrators.

```python
import asyncio

from durabletask import task


async def workflow(ctx: task.OrchestrationContext, values: list[int]) -> list[int]:
    async def branch(value: int) -> int:
        first = await ctx.call_activity("increment", input=value, return_type=int)
        await asyncio.sleep(1)
        return await ctx.call_activity("increment", input=first, return_type=int)

    return await asyncio.gather(*(branch(value) for value in values))
```

Each branch resumes as its own durable operation completes, including when
multiple completions arrive in one dispatch. It does not wait for an entire
batch before scheduling its next operation.

## Supported authoring contract

Await activities, timers, external events, sub-orchestrations, durable
`task.when_all` / `task.when_any`, and entity calls/locks where supported by the
provider. Functions compatibility operations such as `context.task_all()`
remain awaitable. Generator and coroutine parents/children may call one another
through durable sub-orchestrations.

The dispatch-local loop supports FIFO ready callbacks, local futures and tasks,
`asyncio.create_task`, `asyncio.gather`, and `asyncio.sleep`. On Python 3.11+,
it supports `asyncio.TaskGroup` and `asyncio.timeout` / `timeout_at`.
`asyncio.wait_for`, AnyIO's asyncio task groups, cancel scopes, shielded cleanup,
and `fail_after` use the same task/future scheduling and durable timers.
AnyIO support is bounded to these paths, not all backends or APIs.

Create branches from ordered inputs. Ready work follows FIFO scheduling;
history completions are delivered in history order, draining ready work after
each completion. Task-group cancellation wakeups are ordered by local task
creation, not set/object-hash iteration. Local tasks are dispatch resources,
not new durable instances.

`loop.time()` uses seconds from a fixed UTC epoch and the current orchestration
clock, never machine monotonic time. `loop.call_at` and `call_later` schedule
durable timers for future finite deadlines. Timer conversion rounds up when
necessary so a durable timer does not precede its floating-point deadline.
Core/Functions long timers reuse the existing chunking; Azure Managed retains
its native long timers. Past deadlines are ready callbacks. `sleep(0)` yields
to ready coroutines without a durable timer. Clock advancement and timers
require history events, not elapsed time in the worker.

> [!WARNING]
> This is not a general-purpose asyncio event loop. External network/storage
> I/O, threads, executors, subprocesses, custom/eager task factories, and
> cross-thread callbacks are outside the orchestrator contract. Perform
> nondeterministic work in ordinary activities. Detected unsupported operations
> raise errors, but the SDK cannot detect every unsafe operation, especially
> synchronous side effects or already-completed foreign awaitables.

`asyncio.wait` and `as_completed` are **not supported deterministic composition
APIs**. `wait` returns unordered sets; `as_completed` converts inputs to a set
and can create tasks in hash-dependent order. Pre-completed/tied tasks and
differences between Python implementations can change ordering even when a
particular example executes successfully. Do not iterate sets to schedule
durable actions. Use ordered `gather`, task groups, or durable `when_any` and
explicitly handle the selected durable task. Other asyncio APIs are not
implicitly supported because they happen to execute.

## Failures, timeouts, and cancellation

Durable failures raise the existing SDK exceptions at the await, preserving
failure details, typed results, and retry policies. `task.when_all` still waits
for every durable child before surfacing failure. In contrast,
`asyncio.gather` with default options reports the first failure without
cancelling siblings. Retain and await siblings if their results matter.
`return_exceptions=True` collects their outcomes in input order.

Task groups cancel and drain local sibling tasks on failure and use their
library's normal exception grouping rules. AnyIO uses level cancellation:
awaited cleanup in a cancelled scope may require
`with anyio.CancelScope(shield=True)`. Python-version-specific exception
grouping and cancel-count behavior remain those of asyncio/AnyIO.

Cancelling a local task or timing out its wait detaches that waiter; it does
**not** cancel the durable activity, sub-orchestration, entity operation, or
external-event subscription it was waiting on. Another waiter can still await
the same durable task. Keep the durable task if you intend to await it again.
Use explicit SDK cancellation for cancellable durable timers/event waits.
Cancelling a timer callback removes an uncommitted timer when possible, but
cannot undo a timer already committed to history. Neither local nor durable
cancellation implies external side effects were undone.

An unhandled local root-coroutine cancellation fails the orchestration with
`TaskCancelledError`; it does not request scheduler termination or repeatedly
abandon the same deterministic replay. Use the client to terminate the durable
instance. Worker/dispatch cancellation and process-control exceptions continue
to follow the existing worker recovery policy.

## Dispatch teardown is not workflow cancellation

Every dispatch reconstructs the coroutine and its local tasks from history.
After the response and entity-release obligations are snapshotted, teardown
cancels remaining local tasks, drains cooperative cleanup, closes tracked async
generators, and disposes the loop. This also happens on suspension, termination,
continue-as-new, history failure, and version rejection.

Nested `finally` blocks and async context-manager exits may run at **every
dispatch boundary**, before workflow completion. Local cleanup can await ready
work such as `sleep(0)`; it must not wait for new durable or external work.
Scheduling/awaiting durable work during async teardown raises an actionable
error. Teardown must cooperate with cancellation; a non-quiescent cleanup
queue is bounded and reported, not allowed to run indefinitely.

Ordinary teardown failures (including `CancelledError`) are logged without
changing the decided response. Unexpected non-cancellation `BaseException`
exits retain the existing terminal-failure/recovery policy; an existing primary
failure is preserved. Teardown status/actions do not replace the response.
Held entity-release obligations are captured before teardown for terminal
payload/failure handling.

Keep workflow cleanup that must be durable in **normal orchestration control
flow**, before return or continue-as-new, not solely in a dispatch finalizer.
No cleanup is guaranteed after process crash or forced termination.
Async-generator *helpers* are closed; async-generator *orchestrators*
(`async def` containing `yield`) are rejected.

## Replay and framework adapters

Ordinary async helpers rerun during replay. Their return values are not
automatically checkpointed. There are no local/virtual activities and no
automatic recording or interception of arbitrary await expressions.
`is_replaying` is not a substitute for recording results. Activities remain
at-least-once work: side effects require appropriate idempotency/deduplication.

Pydantic AI's real `Agent.run` / parallel-tool AnyIO path is exercised with a
deterministic `TestModel` in
[`test_pydantic_ai_orchestrator.py`](../tests/durabletask/test_pydantic_ai_orchestrator.py).
The orchestrator directly awaits the agent; no adapter event loop, trampoline,
or per-batch durable join is needed. The proof adapter still registers ordinary
model/tool activities, encodes/decodes framework payloads, supplies replay-stable
IDs and a deadline clock, and explicitly canonicalizes volatile message
timestamps. These are **adapter responsibilities**, not SDK-wide interception.
This proof is not a complete production Pydantic adapter or compatibility
promise for arbitrary agents, model providers, tools, or framework versions.
No paid model calls are needed.

Changing an in-flight generator to equivalent coroutine code is safe **only
if its durable history contract is preserved**: action IDs/order, names,
payloads, retries, timer deadlines/chunks, event consumption, and entity
release order. Introducing concurrency, a different join, or a new timer may
change that contract. Test recorded histories and version/drain affected
instances; basic sequential parity tests are not blanket migration safety.

The Functions `orchestrator_generator_wrapper` helper supports direct durable
awaits for mock-based unit tests; it does not simulate the asyncio scheduler.
Use the real replay executor/provider or the in-memory backend to test task
groups, gather, and durable timeouts.

## Coordinated delivery

This feature is one draft PR/release unit across all packages. Do not ship a
limited intermediate contract. Release the core package first; then use a
dedicated coordinated release PR to update provider dependency minimums,
versions, and release notes together. Feature development does not change
package versions or dependency minimums.
