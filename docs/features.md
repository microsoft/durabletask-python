# Feature overview

The following features are currently supported:

### Orchestrations

Orchestrators are implemented using ordinary Python functions that take an `OrchestrationContext` as their first parameter. The `OrchestrationContext` provides APIs for starting child orchestrations, scheduling activities, and waiting for external events, among other things. Orchestrations are fault-tolerant and durable, meaning that they can automatically recover from failures and rebuild their local execution state. Orchestrator functions must be deterministic, meaning that they must always produce the same output given the same input.

#### Orchestration versioning

Orchestrations may be assigned a version when they are first created. If an orchestration is given a version, it will continually be checked during its lifecycle to ensure that it remains compatible with the underlying orchestrator code. If the orchestrator code is updated while an orchestration is running, rules can be set that will define the behavior - whether the orchestration should fail, abandon for reprocessing at a later time, or attempt to run anyway. For more information, see [The provided examples](./supported-patterns.md). For more information about versioning in the context of Durable Functions, see [Orchestration versioning in Durable Functions](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-orchestration-versioning) (Note that concepts specific to Azure Functions, such as host.json settings, do not apply to this SDK).

##### Orchestration versioning options

Both the Durable worker and durable client have versioning configuration available. Because versioning checks are handled by the worker, the only information the client needs is a default_version, taken in its constructor, to use as the version for new orchestrations unless otherwise specified. The worker takes a VersioningOptions object with a `default_version` for new sub-orchestrations, a `version` used by the worker for orchestration version comparisons, and two more options giving control over versioning behavior in case of match failures, a `VersionMatchStrategy` and `VersionFailureStrategy`.

**VersionMatchStrategy**

| VersionMatchStrategy.NONE | VersionMatchStrategy.STRICT | VersionMatchStrategy.CURRENT_OR_OLDER |
|-|-|-|
| Do not compare orchestration versions | Only allow orchestrations with the same version as the worker | Allow orchestrations with the same or older version as the worker |

**VersionFailureStrategy**

| VersionFailureStrategy.REJECT | VersionFailureStrategy.FAIL |
|-|-|
| Abandon execution of the orchestrator, but allow it to be reprocessed later | Fail the orchestration |

**Strategy examples**

Scenario 1: You are implementing versioning for the first time in your worker. You want to have a default version for new orchestrations, but do not care about comparing versions with currently running ones. Choose VersionMatchStrategy.NONE, and VersionFailureStrategy does not matter.

Scenario 2: You are updating an orchestrator's code, and you do not want old orchestrations to continue to be processed on the new code. Bump the default version and the worker version, set VersionMatchStrategy.STRICT and VersionFailureStrategy.FAIL.

Scenario 3: You are updating an orchestrator's code, and you have ensured the code is version-aware so that it remains backward-compatible with existing orchestrations. Bump the default version and the worker version, and set VersionMatchStrategy.CURRENT_OR_OLDER and VersionFailureStrategy.FAIL.

Scenario 4: You are performing a high-availability deployment, and your orchestrator code contains breaking changes making it not backward-compatible. Bump the default version and the worker version, and set VersionFailureStrategy.REJECT and VersionMatchStrategy.STRICT. Ensure that at least a few of the previous version of workers remain available to continue processing the older orchestrations - eventually, all older orchestrations _should_ land on the correct workers for processing. Once all remaining old orchestrations have been processed, shut down the remaining old workers.

### Activities

Activities are implemented using ordinary Python functions that take an `ActivityContext` as their first parameter. Activity functions are scheduled by orchestrations and have at-least-once execution guarantees, meaning that they will be executed at least once but may be executed multiple times in the event of a transient failure. Activity functions are where the real "work" of any orchestration is done.

### Durable timers

Orchestrations can schedule durable timers using the `create_timer` API. These timers are durable, meaning that they will survive orchestrator restarts and will fire even if the orchestrator is not actively in memory. Durable timers can be of any duration, from milliseconds to months.

### Sub-orchestrations

Orchestrations can start child orchestrations using the `call_sub_orchestrator` API. Child orchestrations are useful for encapsulating complex logic and for breaking up large orchestrations into smaller, more manageable pieces. Sub-orchestrations can also be versioned in a similar manner to their parent orchestrations, however, they do not inherit the parent orchestrator's version. Instead, they will use the default_version defined in the current worker's VersioningOptions unless otherwise specified during `call_sub_orchestrator`.

### Entities

#### Concepts

Durable Entities provide a way to model small, stateful objects within your orchestration workflows. Each entity has a unique identity and maintains its own state, which is persisted durably. Entities can be interacted with by sending them operations (messages) that mutate or query their state. These operations are processed sequentially, ensuring consistency. Examples of uses for durable entities include counters, accumulators, or any other operation which requires state to persist across orchestrations.

Entities can be invoked from durable clients directly, or from durable orchestrators. They support features like automatic state persistence, concurrency control, and can be locked for exclusive access during critical operations.

Entities are accessed by a unique ID, implemented here as EntityInstanceId. This ID is comprised of two parts, an entity name referring to the function or class that defines the behavior of the entity, and a key which is any string defined in your code. Each entity instance, represented by a distinct EntityInstanceId, has its own state.

#### Syntax

##### Defining Entities

Entities can be defined using either function-based or class-based syntax.

```python
# Funtion-based entity
def counter(ctx: entities.EntityContext, input: int):
    state = ctx.get_state(int, 0)
    if ctx.operation == "add":
        state += input
        ctx.set_state(state)
    elif operation == "get":
        return state

# Class-based entity
class Counter(entities.DurableEntity):
    def __init__(self):
        self.set_state(0)

    def add(self, amount: int):
        self.set_state(self.get_state(int, 0) + amount)

    def get(self):
        return self.get_state(int, 0)
```

> Note that the object properties of class-based entities may not be preserved across invocations. Use the derived get_state and set_state methods to access the persisted entity data. 

##### Invoking entities

Entities are invoked using the `signal_entity` or `call_entity` APIs. The Durable Client only allows `signal_entity`: 

```python
c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True,
                                taskhub=taskhub_name, token_credential=None)
entity_id = entities.EntityInstanceId("my_entity_function", "myEntityId")
c.signal_entity(entity_id, "do_nothing")
```

Whereas orchestrators can choose to use `signal_entity` or `call_entity`:

```python
# Signal an entity (fire-and-forget)
entity_id = entities.EntityInstanceId("my_entity_function", "myEntityId")
ctx.signal_entity(entity_id, operation_name="add", input=5)

# Call an entity (wait for result)
entity_id = entities.EntityInstanceId("my_entity_function", "myEntityId")
result = yield ctx.call_entity(entity_id, operation_name="get")
```

##### Entity actions

Entities can perform actions such signaling other entities or starting new orchestrations

- `ctx.signal_entity(entity_id, operation, input)`
- `ctx.schedule_new_orchestration(orchestrator_name, input)`

##### Locking and concurrency

Because entites can be accessed from multiple running orchestrations at the same time, entities may also be locked by a single orchestrator ensuring exclusive access during the duration of the lock (also known as a critical section). Think semaphores:

```python
with (yield ctx.lock_entities([entity_id_1, entity_id_2]):
        # Perform entity call operations that require exclusive access
        ...
```

Note that locked entities may not be signalled, and every call to a locked entity must return a result before another call to the same entity may be made from within the critical section. For more details and advanced usage, see the examples and API documentation.

##### Deleting entities

Entites are represented as orchestration instances in your Task Hub, and their state is persisted in the Task Hub as well. When using the Durable Task Scheduler as your durability provider, the backend will automatically clean up entities when their state is empty, this is effectively the "delete" operation to save space in the Task Hub. In the DTS Dashboard, "delete entity" simply signals the entity with the "delete" operation. In this SDK, we provide a default implementation for the "delete" operation to clear the state when using class-based entities, which end users are free to override as needed. Users must implement "delete" manually for function-based entities.

### External events

Orchestrations can wait for external events using the `wait_for_external_event` API. External events are useful for implementing human interaction patterns, such as waiting for a user to approve an order before continuing.

### Continue-as-new

Orchestrations can be continued as new using the `continue_as_new` API. This API allows an orchestration to restart itself from scratch, optionally with a new input.

### Suspend, resume, and terminate

Orchestrations can be suspended using the `suspend_orchestration` client API and will remain suspended until resumed using the `resume_orchestration` client API. A suspended orchestration will stop processing new events, but will continue to buffer any that happen to arrive until resumed, ensuring that no data is lost. An orchestration can also be terminated using the `terminate_orchestration` client API. Terminated orchestrations will stop processing new events and will discard any buffered events.

### Retry policies

Orchestrations can specify retry policies for activities and sub-orchestrations. These policies control how many times and how frequently an activity or sub-orchestration will be retried in the event of a transient error.

### Replay-safe logging

Orchestrator functions replay their history each time they are resumed,
which can cause duplicate log messages. The `create_replay_safe_logger`
method on `OrchestrationContext` returns a `ReplaySafeLogger` that wraps
a standard `logging.Logger` and automatically suppresses output while
the orchestrator is replaying. `ReplaySafeLogger` extends Python's
`logging.LoggerAdapter`, which is the idiomatic way to add context or
modify behavior on an existing logger.

```python
import logging

logger = logging.getLogger("my_orchestrator")

def my_orchestrator(ctx: task.OrchestrationContext, input):
    replay_logger = ctx.create_replay_safe_logger(logger)
    replay_logger.info("Starting orchestration %s", ctx.instance_id)
    result = yield ctx.call_activity(my_activity, input=input)
    replay_logger.info("Activity returned: %s", result)
    return result
```

> [!NOTE]
> Unlike the .NET SDK, where `CreateReplaySafeLogger` accepts a
> category name string and internally creates the logger via
> `ILoggerFactory`, the Python SDK requires you to pass an existing
> `logging.Logger` instance. This is because Python's
> `logging.getLogger(name)` already serves as the global factory and
> is the standard way to obtain loggers.

The replay-safe logger supports all standard log levels: `debug`,
`info`, `warning`, `error`, `critical`, and `exception`, as well as
the generic `log(level, msg)` method. It also exposes `isEnabledFor`
which returns `False` during replay so callers can skip expensive
message formatting.

> [!TIP]
> Create the replay-safe logger once at the start of your orchestrator
> and reuse it throughout the function.

### Large payload externalization

Orchestration inputs, outputs, and event data are transmitted through
gRPC messages. When these payloads become very large they can exceed
gRPC message size limits or degrade performance. Large payload
externalization solves this by transparently offloading oversized
payloads to an external store (such as Azure Blob Storage) and
replacing them with compact reference tokens in the gRPC messages.

This feature is **opt-in** and requires installing an optional
dependency:

```bash
pip install durabletask[azure-blob-payloads]
```

#### How it works

1. When the worker or client sends a payload that exceeds the
   configured threshold (default 900 KB), the payload is
   compressed (GZip, enabled by default) and uploaded to the
   external store.
2. The original payload in the gRPC message is replaced with a
   compact token (e.g. `blob:v1:<container>:<blobName>`).
3. When the worker or client receives a message containing a token,
   it downloads and decompresses the original payload automatically.

This process is fully transparent to orchestrator and activity code —
no changes are needed in your workflow logic.

#### Configuring the blob payload store

The built-in `BlobPayloadStore` uses Azure Blob Storage. Create a
store instance and pass it to both the worker and client:

```python
from durabletask.extensions.azure_blob_payloads import BlobPayloadStore, BlobPayloadStoreOptions

store = BlobPayloadStore(BlobPayloadStoreOptions(
    connection_string="DefaultEndpointsProtocol=https;...",
    container_name="durabletask-payloads",  # default
    threshold_bytes=900_000,                # default (900 KB)
    max_stored_payload_bytes=10_485_760,    # default (10 MB)
    enable_compression=True,                # default
))
```

Then pass the store to the worker and client:

```python
with DurableTaskSchedulerWorker(
    host_address=endpoint,
    secure_channel=secure_channel,
    taskhub=taskhub_name,
    token_credential=credential,
    payload_store=store,
) as w:
    # ... register orchestrators and activities ...
    w.start()

    c = DurableTaskSchedulerClient(
        host_address=endpoint,
        secure_channel=secure_channel,
        taskhub=taskhub_name,
        token_credential=credential,
        payload_store=store,
    )
```

You can also authenticate using `account_url` and a
`TokenCredential` instead of a connection string:

```python
from azure.identity import DefaultAzureCredential

store = BlobPayloadStore(BlobPayloadStoreOptions(
    account_url="https://<account>.blob.core.windows.net",
    credential=DefaultAzureCredential(),
))
```

#### Configuration options

| Option | Default | Description |
|---|---|---|
| `threshold_bytes` | 900,000 (900 KB) | Payloads larger than this are externalized |
| `max_stored_payload_bytes` | 10,485,760 (10 MB) | Maximum size for externalized payloads |
| `enable_compression` | `True` | GZip-compress payloads before uploading |
| `container_name` | `"durabletask-payloads"` | Azure Blob container name |
| `connection_string` | `None` | Azure Storage connection string |
| `account_url` | `None` | Azure Storage account URL (use with `credential`) |
| `credential` | `None` | `TokenCredential` for token-based auth |

#### Cross-SDK compatibility

The blob token format (`blob:v1:<container>:<blobName>`) is
compatible with the .NET Durable Task SDK, enabling
interoperability between Python and .NET workers sharing the same
task hub and storage account. Note that message serialization strategies
may differ for complex objects and custom types.

#### Custom payload stores

You can implement a custom payload store by subclassing
`PayloadStore` from `durabletask.payload` and implementing
the `upload`, `upload_async`, `download`, `download_async`, and
`is_known_token` methods:

```python
from typing import Optional

from durabletask.payload import PayloadStore, LargePayloadStorageOptions


class MyPayloadStore(PayloadStore):

    def __init__(self, options: LargePayloadStorageOptions):
        self._options = options

    @property
    def options(self) -> LargePayloadStorageOptions:
        return self._options

    def upload(self, data: bytes, *, instance_id: Optional[str] = None) -> str:
        # Store data and return a unique token string
        ...

    async def upload_async(self, data: bytes, *, instance_id: Optional[str] = None) -> str:
        ...

    def download(self, token: str) -> bytes:
        # Retrieve data by token
        ...

    async def download_async(self, token: str) -> bytes:
        ...

    def is_known_token(self, value: str) -> bool:
        # Return True if the value looks like a token from this store
        ...
```

See the [large payload example](../examples/large_payload/) for a
complete working sample.

### Logging configuration

Both the TaskHubGrpcWorker and TaskHubGrpcClient (as well as DurableTaskSchedulerWorker and DurableTaskSchedulerClient for durabletask-azuremanaged) accept a log_handler and log_formatter object from `logging`. These can be used to customize verbosity, output location, and format of logs emitted by these sources.

For example, to output logs to a file called `worker.log` at level `DEBUG`, the following syntax might apply:

```python
log_handler = logging.FileHandler('durable.log', encoding='utf-8')
log_handler.setLevel(logging.DEBUG)

with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=secure_channel,
                                taskhub=taskhub_name, token_credential=credential, log_handler=log_handler) as w:
```

> [!NOTE]
> The worker and client output many logs at the `DEBUG` level that will be useful when understanding orchestration flow and diagnosing issues with Durable applications. Before submitting issues, please attempt a repro of the issue with debug logging enabled.

### Work item filtering

By default a worker receives **all** work items from the backend,
regardless of which orchestrations, activities, or entities are
registered. Work item filtering lets you explicitly tell the backend
which work items a worker can handle so that only matching items are
dispatched. This is useful when running multiple specialized workers
against the same task hub.

Work item filtering is **opt-in**. Call `use_work_item_filters()` on
the worker before starting it.

#### Auto-generated filters

Calling `use_work_item_filters()` with no arguments builds filters
automatically from the worker's registry at start time:

```python
with DurableTaskSchedulerWorker(...) as w:
    w.add_orchestrator(my_orchestrator)
    w.add_activity(my_activity)
    w.use_work_item_filters()  # auto-generate from registry
    w.start()
```

When versioning is configured with `VersionMatchStrategy.STRICT`,
the worker's version is included in every filter so the backend
only dispatches work items that match that exact version.

#### Explicit filters

Pass a `WorkItemFilters` instance for fine-grained control:

```python
from durabletask.worker import (
    WorkItemFilters,
    OrchestrationWorkItemFilter,
    ActivityWorkItemFilter,
    EntityWorkItemFilter,
)

w.use_work_item_filters(WorkItemFilters(
    orchestrations=[
        OrchestrationWorkItemFilter(name="my_orch", versions=["2.0.0"]),
    ],
    activities=[
        ActivityWorkItemFilter(name="my_activity"),
    ],
    entities=[
        EntityWorkItemFilter(name="my_entity"),
    ],
))
```

#### Clearing filters

Pass `None` to clear any previously configured filters and return
to the default behaviour of processing all work items:

```python
w.use_work_item_filters(None)
```

See the full
[work item filtering sample](../examples/work_item_filtering.py).
