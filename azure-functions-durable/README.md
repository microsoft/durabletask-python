# Azure Functions Durable (Python) — 2.x

`azure-functions-durable` is the Python SDK provider for
[Durable Azure Functions](https://learn.microsoft.com/azure/azure-functions/durable/),
built on top of the [`durabletask`](https://pypi.org/project/durabletask/) SDK.

> [!NOTE]
> 2.x is a ground-up rewrite of the Durable Functions Python SDK on top of the
> `durabletask` runtime. It is currently a preview (beta) release; APIs may
> change before the stable 2.0.0.

## Requirements

- Python 3.13+
- The decorator-based Azure Functions programming model (`DFApp` / `Blueprint`)

## Installation

```bash
pip install azure-functions-durable
```

## Overview

Author orchestrations, activities, and entities as Azure Functions and let the
Durable Task runtime handle scheduling, checkpointing, and replay. Both
durabletask-native two-argument functions (`def orchestrator(ctx, input)`) and
v1-style single-argument functions (`def orchestrator(context)`) are supported,
along with class-based entities and a compatibility layer over the v1 API.

Key capabilities include durable orchestrations and sub-orchestrations, durable
timers, external events, durable entities, retries, versioning, durable HTTP
calls (`context.call_http(...)`), recurring scheduled tasks, and history export.

## Large payloads

Configure a `durabletask.payload.PayloadStore` once at app startup to store large
serialized payloads outside orchestration history. For Azure Blob Storage, install
the optional dependencies:

```bash
pip install azure-functions-durable "durabletask[azure-blob-payloads]" aiohttp
```

In your Function app, configure the root `DFApp` before any invocations:

```python
import os

import azure.durable_functions as df
from durabletask.extensions.azure_blob_payloads import (
    BlobPayloadStore,
    BlobPayloadStoreOptions,
)

app = df.DFApp()
app.configure_large_payloads(
    payload_store=BlobPayloadStore(BlobPayloadStoreOptions(
        connection_string=os.environ["PAYLOAD_STORAGE_CONNECTION_STRING"],
        container_name="durable-payloads",
        threshold_bytes=256 * 1024,
    ))
)
```

Set `PAYLOAD_STORAGE_CONNECTION_STRING` in your Function app settings (or in
`local.settings.json` for local development). The store automatically uploads
serialized payloads above the threshold and downloads their contents when the
SDK consumes them. Orchestration and activity inputs and outputs, custom status,
external events, and entity inputs, results, and state use the configured store.
Sub-orchestrations and continue-as-new use it as well. The default maximum stored
payload size is 10 MiB; `max_stored_payload_bytes` can configure this limit.

Configuration applies to both synchronous and asynchronous durable clients and
all registered blueprints, including blueprints imported before configuration.
There is one store per Python worker process. Registering the same store object
again is allowed; registering a different object raises `ValueError`. Configure
every scaled-out worker with access to the same backing storage and retain that
access across deployments. Keep the store open for the process lifetime.

> [!WARNING]
> Keep payload blobs for as long as any retained orchestration history or entity
> state references them, including histories needed for replay. Purging an
> orchestration does not delete its payload blobs; manage retention separately.

This is SDK-managed storage, separate from the Azure Storage backend's automatic
large-message handling. Without configuration, the SDK keeps payloads inline.
Use the configured Python clients to retrieve hydrated payloads. Host management
HTTP endpoints and other consumers that do not use this configuration can expose
reference strings instead. Applications exchanging externalized payloads must
agree on the store and reference encoding; Functions references are JSON strings.

> [!WARNING]
> Storage failures can fail durable invocations, including orchestrations.
> Storage transport retries are separate from durable activity retry policies.
> This SDK does not add an activity retry policy or guarantee that the Functions
> host abandons and redelivers a work item after a storage failure. A transient
> storage error can therefore become a terminal orchestration failure.

Registered orchestration and entity handlers await the store's async methods
before and after execution. Orchestrators remain synchronous generators, and
orchestration replay and entity code run on execution threads with their
invocation logging context preserved. Their payload downloads and uploads do
not occupy those threads, and serialization does not access storage during
replay. Custom stores must implement genuinely nonblocking async methods to
benefit from this behavior.

For orchestration and entity execution, the SDK reuses the Functions runtime's
thread pool when the runtime exposes it; otherwise it uses a process-wide SDK
pool. Both honor `PYTHON_THREADPOOL_THREAD_COUNT`.

Activities retain their synchronous or asynchronous calling convention.
Synchronous activities use synchronous storage inside the host-managed execution
thread and remain directly callable without `await`; async activities await
async storage. Direct calls to decorated activities return ordinary Python values
without accessing payload storage; transport processing applies only to host
binding invocations. Binding converters perform no storage I/O. Synchronous functions
still receive the synchronous durable client, and synchronous client APIs use
synchronous storage. Direct `Orchestrator.handle()` and `Orchestrator.create()`
adapters also remain synchronous.

Both client history APIs hydrate entity operation inputs and results, including
values nested in the host's entity protocol envelopes. During orchestration
replay, nested entity results are hydrated, but historical nested request inputs
are not downloaded because replay only needs their correlation metadata.
Historical scheduled activity inputs are also not downloaded during replay;
explicit history retrieval continues to hydrate those inputs.

> [!WARNING]
> With payload storage configured, whole payload strings recognized by the
> store's `is_known_token()` are reserved references, not literal application
> data. For `BlobPayloadStore`, this includes strings of the form
> `blob:v1:<container>:<blobName>`, with nonempty container and blob names.
> Functions recognizes both raw and JSON-quoted references. A matching string
> is treated as already externalized on output and downloaded on input, even
> below the size threshold. Missing or inaccessible references raise errors;
> they do not fall back to literal strings.

To pass a reference as application data for later retrieval, wrap it in an
object, for example `{"reference": "blob:v1:container:blob"}`. Reference detection
does not recursively inspect strings inside application JSON objects. The
wrapper preserves the literal reference whether the object stays inline or is
itself externalized. Keep the wrapper whenever passing that value across a
durable payload boundary; passing its string field alone opts back into reference
interpretation. Custom payload stores define their own reserved token syntax.

> [!WARNING]
> Recognized references are trusted transport inputs, not authorization
> boundaries. `BlobPayloadStore` reads from the container named in the token
> using its configured credentials; `container_name` selects the upload
> container and does not restrict downloads. An account-wide connection string
> can therefore allow reads outside that container. Use least-privilege
> credentials scoped to the intended payload storage, and explicitly decide
> whether external callers may supply references. Reject untrusted references
> or validate their allowed storage locations before passing them into durable
> APIs; token recognition alone does not authorize a read.

## Unit testing entities

Use `execute_entity()` to run one entity operation in-process without a
Functions host or Durable Task backend. It supports v1-style entity functions,
durabletask-native entity functions, and `DurableEntity` subclasses:

```python
from azure.durable_functions.testing import execute_entity
from durabletask.entities import DurableEntity


class Counter(DurableEntity):
    def add(self, amount: int) -> int:
        value = self.get_state(int, 0) + amount
        self.set_state(value)
        return value


outcome = execute_entity(Counter, "add", input=2, state=3)

assert outcome.get_result() == 5
assert outcome.get_state() == 5
assert outcome.actions == ()
```

For an `entity_trigger`-decorated function, pass the exposed entity function:

```python
import azure.durable_functions as df
from azure.durable_functions.testing import execute_entity


app = df.DFApp()


@app.entity_trigger(context_name="context")
def counter(context: df.DurableEntityContext) -> None:
    value = context.get_state(initializer=lambda: 0)
    value += context.get_input()
    context.set_state(value)
    context.set_result(value)


entity_function = counter.build().get_user_function().entity_function
outcome = execute_entity(entity_function, "add", input=2, state=3)

assert outcome.get_result() == 5
assert outcome.get_state() == 5
```

The returned `EntityTestResult` provides `get_result()` and `get_state()`
methods plus typed signal or orchestration-start actions scheduled by the
operation. Pass `expected_type` when reconstructing a custom payload:

```python
assert outcome.get_state(expected_type=CounterState) == CounterState(value=5)
```

## Links

- [2.x samples](samples/)
- [Migration guide from 1.x](MIGRATION_GUIDE.md)
- [Changelog](CHANGELOG.md)
- [Durable Functions documentation](https://learn.microsoft.com/azure/azure-functions/durable/)
- [`durabletask` on PyPI](https://pypi.org/project/durabletask/)
- [Azure Functions Durable 1.x source](https://github.com/Azure/azure-functions-durable-python)
- [Azure Functions Python library](https://github.com/Azure/azure-functions-python-library)
- [Azure Functions Python worker](https://github.com/Azure/azure-functions-python-worker)
- [Repository](https://github.com/microsoft/durabletask-python)

## License

Licensed under the [MIT License](LICENSE).
