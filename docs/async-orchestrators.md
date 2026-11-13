# Async orchestrators

All three packages accept native `async def` orchestrators alongside existing
synchronous generator orchestrators. Both styles use the same history, action,
retry, payload, and entity engine. Register the function normally with
`TaskHubGrpcWorker`, `DurableTaskSchedulerWorker`, or the Functions
`orchestration_trigger` decorator. Async orchestrators use the same
two-argument `(ctx, input)` shape in all three packages.

```python
import asyncio
from datetime import timedelta

from durabletask import task


async def workflow(ctx: task.AsyncOrchestrationContext, values: list[int]) -> list[int]:
    async def branch(value: int) -> int:
        first = await ctx.call_activity("increment", input=value, return_type=int)
        await ctx.create_timer(timedelta(seconds=1))
        return await ctx.call_activity("increment", input=first, return_type=int)

    async with asyncio.TaskGroup() as group:
        branches = [group.create_task(branch(value)) for value in values]
    return [branch.result() for branch in branches]
```

Each branch resumes as its own durable operation completes, including when
multiple completions arrive in one dispatch. It does not wait for an entire
batch before scheduling its next operation.

> [!WARNING]
> Python scopes are not persisted workflow resources. `finally` blocks and async
> context-manager exits can run when an ordinary dispatch suspends, not only
> when the workflow finishes. Keep durable compensation in normal orchestration
> control flow; see [dispatch teardown](#dispatch-teardown-is-not-workflow-cancellation).

## Motivation and scope

Generator orchestrators cannot call `async def` helpers or use ordinary
asyncio composition, so async-only libraries need adapters or rewrites before
they can participate in a durable workflow
([#294](https://github.com/microsoft/durabletask-python/issues/294)).

Goals:

- Let orchestrators be written with `async def` and `await`, sharing the
existing replay engine, history format, and provider integrations.
- Support ordinary deterministic asyncio composition, such as
`asyncio.gather`, `asyncio.TaskGroup`, `asyncio.timeout`, and
`asyncio.wait_for`, with durable timers as the clock.
- Allow async helpers and libraries to compose durable operations, as long as
they use only supported asyncio APIs.
- Keep generator orchestrators and their recorded histories unchanged.

Non-goals:

- A general-purpose asyncio event loop. Real network, storage, or subprocess
I/O, threads, and executors remain activity work.
- Set-based composition such as `asyncio.wait` and `asyncio.as_completed`,
whose ordering is not replay-stable.
- Automatic recording of arbitrary `await` results or local activities.

Alternatives considered:

- **A minimal await-to-generator driver without an event loop.** Driving the
coroutine directly would support sequential awaits, but not asyncio tasks,
task groups, timeouts, or libraries that call `asyncio.get_running_loop()`.
- **Combinator-only APIs.** Extending `task.when_all` / `task.when_any` keeps the
generator model but still excludes async-only code and standard asyncio
composition.

A deterministic, dispatch-local event loop supports the standard composition
primitives while deriving its clock and task ordering from durable history,
so replays reconstruct the same local scheduling decisions.

## Cold operations and reusable handles

An `async def` orchestrator automatically receives an
`AsyncOrchestrationContext`. Its awaited durable operations return cold,
single-use native coroutines.
Generator orchestrators still receive `OrchestrationContext` and keep their
eager durable task handles. Both contexts use the same replay engine.

One-way commands `ctx.send_event(...)`, `ctx.signal_entity(...)`, and
`ctx.continue_as_new(...)` remain synchronous and eager: call them **without
`await`**. These commands take effect when invoked and return `None`.

| Expression | Meaning |
|---|---|
| `ctx.call_activity(...)` | Create a single-use native coroutine; schedule when it starts. |
| `ctx.as_task_context().call_activity(...)` | Schedule eagerly and return a durable handle. |
| `handle.wait()` | Create a native coroutine borrowing an existing handle. |
| `ctx.wait_for_external_event(...)` | Withdraw an owned event wait on cancellation or timeout. |
| `ctx.create_timer(...)` | Own a timer, cancelling its pending local work on cancellation. |

Use these cold coroutines with `asyncio.create_task`, `TaskGroup.create_task`,
and ordered `gather`. Constructing a coroutine does not validate the operation,
allocate action IDs, serialize input, or consume buffered events. Cancelling its local task before
its first step starts no durable operation. Operation inputs are not copied:
mutations made before the coroutine's first step are observed, and relative
timer delays start from that execution point. Keep or copy inputs accordingly.
Dropping an operation without awaiting it or wrapping it in a task schedules
nothing, and Python reports that the coroutine was never awaited.

Cold coroutines are single-use, like ordinary Python coroutines. For an
explicitly started operation with multiple independent waiters, retain its
durable handle:

```python
operation = ctx.as_task_context().call_activity("quote", input="order-1", return_type=int)
first = asyncio.create_task(operation.wait())
second = asyncio.create_task(operation.wait())
first.cancel()
value = await second
```

Cancelling either borrower does not cancel the underlying operation. Direct
`await operation` remains supported, but `create_task(operation)` is not valid:
an awaitable durable handle is not a coroutine. Do not retry that expression
assuming the eager operation was rolled back.

To identify the winner of a race, check whether an operation has completed, or
read its result later, use eager handles with `task.when_any`, or wrap cold
operations in `asyncio` tasks:

```python
eager = ctx.as_task_context()
approval = eager.wait_for_external_event("approval", data_type=bool)
deadline = eager.create_timer(timedelta(hours=1))
winner = await task.when_any([approval, deadline])  # The identical handle wins.
if winner is approval:
    deadline.cancel()
    approved = approval.get_result()
else:
    report = asyncio.create_task(ctx.call_activity("report", return_type=str))
    finished = report.done()  # Completion check without waiting.
    summary = await report  # Result, or the operation's failure.
```

The losing handle stays inspectable through `is_complete` and, once complete,
`get_result()`.

The worker classifies each orchestrator once, when it is registered:

1. A callable that is itself a coroutine function (including partials, bound
methods, and callable instances whose `__call__` is `async def`) is async.
2. A callable that is itself a generator function is a generator orchestrator.
3. Otherwise, a `functools.wraps` wrapper is classified by the function it
wraps, so a synchronous decorator around an async orchestrator remains async.
4. Any other callable is synchronous.

Async orchestrators receive an `AsyncOrchestrationContext` and must return a
coroutine; synchronous orchestrators receive `OrchestrationContext`. An
unmarked synchronous wrapper returning a coroutine is rejected because it would
have received the generator context. Cyclic `__wrapped__` chains are rejected at
registration. Existing registration naming requirements still apply.

## Azure Functions

Functions async orchestrators use the two-argument shape and receive
`task.AsyncOrchestrationContext`, as in the other packages. The second argument
is the orchestration input. Single-argument `async def` orchestrators are not
supported: the `orchestration_trigger` decorator raises `TypeError` for them.
The single-argument `df.DurableOrchestrationContext` remains available for
generator orchestrators.

Durable HTTP requests use `df.call_http`, which accepts the async context and
returns a cold coroutine:

```python
import asyncio

import azure.durable_functions as df
from durabletask import task

app = df.DFApp()


@app.orchestration_trigger(context_name="context")
async def greetings(
        ctx: task.AsyncOrchestrationContext, names: list[str]) -> dict[str, object]:
    messages = await asyncio.gather(*(
        ctx.call_activity("say_hello", input=name, return_type=str)
        for name in names
    ))
    response = await df.call_http(ctx, "GET", "https://example.com/status")
    return {"greetings": messages, "status": response.status_code}
```

Compose branches with `asyncio.gather` or `asyncio.TaskGroup`, and use
`ctx.as_task_context()` handles with `task.when_any` for races, as described
above.

## Supported authoring contract

Await activities, timers, external events, sub-orchestrations, durable
operations via eager `task.when_all` / `task.when_any`, and entity calls/locks
where supported by the provider. Generator and coroutine parents/children may call
one another through durable sub-orchestrations.

For sub-orchestration results, pass `return_type=Result` to reconstruct a custom
result type. Unlike activity calls, a child orchestrator's return annotation is
not used for result reconstruction, even when passing the child function rather
than its name. Without an explicit `return_type`, the async API returns `Any`;
with the default JSON converter, a plain JSON object remains a dictionary.

The dispatch-local loop supports FIFO ready callbacks, local futures and tasks,
`asyncio.create_task`, `asyncio.gather`, `asyncio.sleep`, `asyncio.TaskGroup`,
and `asyncio.timeout` / `timeout_at`.
`asyncio.wait_for` uses the same task/future scheduling and durable timers.
Third-party async libraries are not part of this compatibility contract, even
when individual code paths happen to use supported asyncio primitives.

Create branches from ordered inputs. Ready work follows FIFO scheduling;
history completions are delivered in history order, draining ready work after
each completion. Task-group cancellation wakeups are ordered by local task
creation, not set/object-hash iteration. Local tasks are dispatch resources,
not new durable instances. Create them with `asyncio.create_task` or a task
group; do not construct `asyncio.Task` directly, because such tasks are not
tracked by the dispatch and are not cancelled or cleaned up with it.

`loop.time()` uses seconds from a fixed UTC epoch and the current orchestration
clock, never machine monotonic time. `loop.call_at` and `call_later` schedule
durable timers for future finite deadlines. Timer conversion rounds up when
necessary so a durable timer does not precede its floating-point deadline.
Core/Functions long timers reuse the existing chunking; Azure Managed retains
its native long timers. Past deadlines are ready callbacks. `sleep(0)` yields
to ready coroutines without a durable timer. Delays below the resolution of
`loop.time()` (about 238 nanoseconds at current dates) are effectively zero.
Clock advancement and timers require history events, not elapsed time in the
worker.

> [!WARNING]
> This is not a general-purpose asyncio event loop. External network/storage
> I/O, threads, executors, subprocesses, custom/eager task factories, and
> cross-thread callbacks are outside the orchestrator contract. Perform
> nondeterministic work in ordinary activities. Detected unsupported operations
> raise errors, but the SDK cannot detect every unsafe operation, especially
> synchronous side effects or already-completed foreign awaitables.

Other asyncio APIs are not implicitly supported because they happen to execute.
Do not iterate sets to schedule durable actions.

> [!WARNING]
> `asyncio.wait` and `asyncio.as_completed` are **not supported** in async
> orchestrators. They are not rejected at run time, but `wait` returns
> unordered sets and `as_completed` converts its inputs to a set and can create
> tasks in hash-dependent order. Pre-completed or tied tasks and differences
> between Python implementations can change durable action order on replay,
> even when a particular example executes successfully. Use ordered `gather`,
> task groups, or durable `when_any` and explicitly handle the selected durable
> task.

## Failures, timeouts, and cancellation

Durable failures raise the existing SDK exceptions at the await, preserving
failure details, typed results, and retry policies. `task.when_all` still waits
for every durable child before surfacing failure. In contrast,
`asyncio.gather` with default options reports the first failure without
cancelling siblings. Retain and await siblings if their results matter.
`return_exceptions=True` collects their outcomes in input order.
Unobserved local background-task failures are reported during dispatch cleanup;
ordinary exceptions are logged, not silently discarded or automatically joined.
Discarded local-task results and observed-failure tracebacks are released during
replay rather than retained until dispatch cleanup.

Task groups cancel and drain local sibling tasks on failure and use asyncio's
normal exception grouping rules. Python-version-specific exception grouping
and cancel-count behavior remain those of asyncio.

Cancelling a borrower of an existing durable handle, including `handle.wait()`,
detaches that waiter; it does **not** cancel the activity, sub-orchestration,
entity operation, or external-event subscription. Another borrower can still
await the same handle. Keep the handle if you intend to await it again.
Use explicit SDK cancellation when abandoning a cancellable shared timer/event
wait and deliberately affecting every borrower.

The async context owns its private timer and event registrations. For an event
with a deadline, use the owned receive instead of timing out a borrowed handle:

```python
try:
    approval = await ctx.wait_for_external_event(
        "approval", timeout=timedelta(seconds=30), data_type=bool)
except TimeoutError:
    approval = await ctx.wait_for_external_event("approval", data_type=bool)
```

The timed-out registration is withdrawn before the replacement is registered,
so it cannot consume the next approval. The receive also cancels its own
deadline timer when the event wins. A nonpositive timeout polls an already
buffered event and otherwise raises `TimeoutError` without creating a timer.
This ownership does not extend to an
activity, sub-orchestration, or entity call: cancellation cannot stop already
issued remote work. A task-group sibling failure can therefore leave remote
operations running. Retain and join their handles when completion is required
before compensation or releasing application-level capacity.

An entity lock request can still be granted after its local waiter is cancelled.
Abandoning that waiter can leave locks held until the orchestration ends and
block subsequent critical sections and some entity operations. If a timeout is
needed, retain an acquisition task and shield it from the timeout; later await
it and exit the returned lock to release it. Do not start another critical
section while acquisition is pending.

Cancelling a timer callback removes an uncommitted timer when possible, but
cannot undo a timer already committed to history. Neither local nor durable
cancellation implies external side effects were undone.

An unhandled local root-coroutine cancellation fails the orchestration with
`TaskCancelledError`; it does not request scheduler termination or repeatedly
abandon the same deterministic replay. Use the client to terminate the durable
instance. Worker/dispatch cancellation and process-control exceptions continue
to follow the existing worker recovery policy.

## Dispatch teardown is not workflow cancellation

> [!WARNING]
> Dispatch teardown is not workflow cancellation. Nested `finally` blocks and
> async context-manager exits may run at **every dispatch boundary**, including
> dispatches that simply suspend while waiting for durable work, long before
> the workflow completes.

Every dispatch reconstructs the coroutine and its local tasks from history.
After the response and entity-release obligations are snapshotted, teardown
cancels remaining local tasks, drains cooperative cleanup, closes tracked async
generators, and disposes the loop. This includes dispatches that suspend,
continue as new, encounter a history failure, or reject a version.

Local cleanup can await ready work such as `sleep(0)`; it must not wait for new
durable or external work.
Scheduling/awaiting durable work during async teardown raises an actionable
error. Teardown must cooperate with cancellation.

> [!WARNING]
> Async teardown is capped at about 10,000 event-loop steps per cleanup phase
> (more for dispatches with many tasks or async generators). Cleanup that is
> still running when the cap is reached is truncated: remaining callbacks are
> dropped, unfinished coroutines are closed, and a warning is logged. The
> response decided before teardown is unchanged. Each async-generator sweep's
> budget is fixed at sweep entry; tasks or generators created during that sweep
> cannot increase it. The cap bounds cooperative scheduling steps, not
> wall-clock time or code that never yields.

Ordinary teardown failures (including `CancelledError`) are logged without
changing the decided response or skipping other cooperative finalizers.
Unexpected non-cancellation `BaseException`
exits retain the existing terminal-failure/recovery policy; an existing primary
failure is preserved. Teardown status/actions do not replace the response.
Held entity-release obligations are captured before teardown for terminal
payload/failure handling. Exiting an entity critical section during teardown
does not schedule additional release messages, and a `SystemExit` or
`KeyboardInterrupt` raised by any finalizer takes precedence over other
cleanup failures.

Keep workflow cleanup that must be durable in **normal orchestration control
flow**, before return or continue-as-new, not solely in a dispatch finalizer.
No cleanup is guaranteed after process crash or forced termination.
Async-generator *helpers* are closed; async-generator *orchestrators*
(`async def` containing `yield`) are rejected. The dispatch owns every async
generator it starts until teardown, including generators first iterated by
another generator's cleanup. To run a generator's cleanup earlier, close it
explicitly with `await gen.aclose()` or iterate it inside
`contextlib.aclosing(...)`.

## Replay and framework adapters

> [!WARNING]
> Recorded histories are coupled to the Python minor version. Python 3.11 and
> 3.12+ schedule coroutine operands of `asyncio.wait_for()` differently, which
> can change durable action order inside concurrent branches, and the async
> context does not normalize arbitrary standard-library scheduling. Keep the
> Python minor version (and dependencies) fixed for in-flight instances; drain
> or version affected instances before upgrading. See
> [#337](https://github.com/microsoft/durabletask-python/issues/337).

Ordinary async helpers rerun during replay. Their return values are not
automatically checkpointed. There are no local/virtual activities and no
automatic recording or interception of arbitrary await expressions.
`is_replaying` is not a substitute for recording results. Activities remain
at-least-once work: side effects require appropriate idempotency/deduplication.

Successive orchestration dispatches and their activities can run on different
workers. Replay reconstructs local tasks and futures from ordered durable
history; it does not rely on task identities or memory retained by a previous
worker. Activity completion order is the order recorded in history, not worker
placement.

Framework adapters can compose supported durable operations using async
helpers. Adapters remain responsible for registering ordinary model/tool
activities, encoding/decoding framework payloads, and supplying replay-stable
IDs, clocks, and message metadata. These responsibilities are not handled by
SDK-wide interception. Libraries that require unsupported asyncio APIs or
nondeterministic operations need an appropriate adapter or activity boundary.

Changing an in-flight generator to equivalent coroutine code is safe **only
if its durable history contract is preserved**: action IDs/order, names,
payloads, retries, timer deadlines/chunks, event consumption, and entity
release order. Introducing concurrency, a different join, or a new timer may
change that contract. Test recorded histories and version/drain affected
instances; basic sequential parity tests are not blanket migration safety.
Switching from eager handles to cold operations can also change action order:
construction order is not execution order.

Supporting several Python versions does not guarantee recorded-history
portability between them. Pin the runtime and dependencies for active
instances and validate histories before upgrades. All workers processing an
instance must use that pinned environment; see the Python version warning
above.

The Functions `orchestrator_generator_wrapper` helper drives generator
orchestrators only. Use the real replay executor/provider or the in-memory
backend to test async orchestrators, including task groups, gather, and durable
timeouts.
