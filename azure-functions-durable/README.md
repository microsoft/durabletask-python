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
pip install "azure-functions-durable>=2.0.0b1,<3"
```

For deployments, also constrain `azure-functions-durable` in the Function
App's `requirements.txt`:

```text
azure-functions-durable>=2.0.0b1,<3
```

Future major versions may include broad, replay-breaking changes. An automatic
upgrade during deployment can cause existing orchestrations to fail
unexpectedly. Review the [migration guide](MIGRATION_GUIDE.md) and test replay
before upgrading. A major-version constraint does not pin the `durabletask`
dependency or guarantee replay compatibility; lock the complete dependency
graph for reproducible deployments.

After the first durable-client, orchestrator, or entity invocation,
each Python worker checks accessible `requirements.txt` files in the
background and writes a host worker log warning when the declared constraints
permit a later major version. The check follows local `-r` and `-c` files and
applicable environment markers. It is best-effort and may miss uncommon version
ranges; missing or unsupported requirements are treated as unknown. A
lockfile's presence does not indicate whether deployment uses it, so the warning
describes the risk of installing directly from the requirements. The check does
not read dependency files during app startup or wait for the check before
returning from an invocation.

## Overview

Author orchestrations, activities, and entities as Azure Functions and let the
Durable Task runtime handle scheduling, checkpointing, and replay. Both
durabletask-native two-argument functions (`def orchestrator(ctx, input)`) and
v1-style single-argument functions (`def orchestrator(context)`) are supported,
along with class-based entities and a compatibility layer over the v1 API.

Key capabilities include durable orchestrations and sub-orchestrations, durable
timers, external events, durable entities, retries, versioning, durable HTTP
calls (`context.call_http(...)`), recurring scheduled tasks, and history export.

`DurableFunctionsClient` and `SyncDurableFunctionsClient` inherit
`OrchestrationQuery.task_hub_names` for filtering orchestration queries by one
or more task hubs. Filtering support depends on the backend.

For both Functions clients, `get_all_orchestration_states()` requests at most
1000 instances per page to support Azure Table Storage. An explicit
`OrchestrationQuery.max_instance_count` below 1000 keeps that page size.
The method follows continuation tokens and returns all matching instances;
the page size is not a total result limit. The `get_status_all()` compatibility
method uses the same limit. Core SDK and Durable Task Scheduler clients keep
their existing page sizes.

`EntityInstanceId` and the compatibility `EntityId` are immutable and hashable,
so they can be used as dictionary keys or set elements. Construct a new ID
instead of assigning or deleting attributes on an existing ID.

## JSON payload typing and legacy compatibility

The provider uses the shared `azure-functions` JSON codec. Its default loose
mode writes custom objects with `to_json` and reconstructs legacy envelopes,
including nested objects, with `from_json`. Classes are resolved only from
already-loaded modules. Where available, destination types are passed to the
codec, such as a native orchestration input annotation, the v1-style
`orchestration_trigger` `input_type` option, or an explicit `expected_type` on
an API that accepts one. Loose mode can still reconstruct a different class
from the one requested; it logs a warning rather than rejecting the mismatch.

Setting `AZURE_FUNCTIONS_DURABLE_STRICT_TYPING=1` opts into strict mode; the
provider does not enable it automatically. To reconstruct a top-level
custom-object envelope, strict mode requires a matching destination type with
both `to_json` and `from_json` methods. Nested legacy envelopes in otherwise
plain JSON remain dictionaries, and strict serialization does not support
custom objects nested in plain containers.
For a v1-style orchestration input, `get_input(expected_type=...)` alone is too
late for the initial decode: declare `input_type` on `orchestration_trigger`
when strict mode must reconstruct a top-level custom-object input.

The Functions worker does not pass activity parameter annotations to the
activity trigger converter. Consequently, strict mode rejects a top-level
custom-object activity input even when the activity has an annotation; this
also applies to externalized payloads. Plain JSON activity inputs still decode
without a destination type.

The shared codec emits one-time `DeprecationWarning` advisories in loose mode
for decoding without a destination type and for reconstructing custom objects.
These compatibility paths remain supported.

> [!WARNING]
> Changing typing modes can change values returned during orchestration replay.
> Validate existing payloads and drain affected orchestrations before switching.

## Durable HTTP failure retries

Opt in to durable retries for the **initial** `context.call_http()` request with
`HttpRetryOptions`. It uses the core activity retry engine and durable timers,
not an in-process HTTP retry loop:

```python
from datetime import timedelta

import azure.durable_functions as df


def orchestrator(context: df.DurableOrchestrationContext):
    response = yield context.call_http(
        "GET",
        "https://example.com/data",
        retry_options=df.HttpRetryOptions(
            first_retry_interval=timedelta(seconds=2),
            max_number_of_attempts=4,
            backoff_coefficient=2,
            max_retry_interval=timedelta(seconds=30),
            retry_timeout=timedelta(minutes=2),
            status_codes_to_retry=[429, 503],
        ),
    )
    return response.content
```

Durations use `timedelta`, matching `RetryPolicy`, rather than the deprecated
millisecond-based `RetryOptions`. The attempt limit includes the initial
attempt. Backoff defaults to 1 (constant delay), the maximum retry interval
defaults to six days, and the retry timeout defaults to unbounded. Set
`max_retry_interval=None` to remove the interval cap.

With a policy, an omitted, `None`, or empty `status_codes_to_retry` selects
**all non-2xx responses**, including unfollowed redirects, matching .NET
`EnsureSuccessStatusCode` semantics, not just 4xx/5xx. A nonempty list selects
exactly those statuses: selecting 202 retries it instead of starting polling.
Excluded statuses return the full `DurableHttpResponse` unchanged. Selected
responses are closed without reading their bodies and become activity
failures; exhaustion fails `call_http` with `durabletask.task.TaskFailedError`
rather than returning the last selected response. Transport and other activity
failures also use the policy regardless of the status list.

The policy and status list are snapshotted when the request is scheduled.
Omitting `retry_options` preserves the existing request payload and behavior.
Automatic 202/Location polling remains separate: subsequent GET polls inherit
neither the failure retry policy nor its status classification. Poll HTTP
errors return normally, and a poll activity/transport failure fails the call
without these retries. `Retry-After` controls polling delays, not failure retry
delays.

`retry_timeout` uses replay-safe orchestration time from the initial activity
scheduling. It stops retrying when a failure is handled at or beyond the
timeout. It does **not** cancel or time-limit an in-flight HTTP request, and
the next retry delay may cross the timeout. Unlike the JavaScript core engine,
Python does not reject a retry solely because its next delay would exceed the
remaining timeout.

> [!WARNING]
> Repeating requests can duplicate side effects, including when a server
> processes a request but its response is lost. Enable retries only for safe
> operations or use server-enforced idempotency keys. Adding or changing retry
> options changes durable scheduling and failure handling. Keep policies
> deterministic; version new caller orchestrations or drain affected in-flight
> instances on the old code before changing their policy.

This feature requires the corrected core replay-safe retry-timeout behavior in
[#326](https://github.com/microsoft/durabletask-python/pull/326). Release the core
fix before publishing this provider feature, then coordinate the provider
version and core dependency minimum in a dedicated release PR. This feature
does not itself change package versions or dependency minimums.

## Unicode JSON serialization

By default, durable payloads preserve the shared `azure-functions` codec's
ASCII-escaped JSON representation. With `azure-functions>=2.4.0b1`, opt in to
literal Unicode JSON on the root `DFApp` at startup:

```python
import azure.durable_functions as df

app = df.DFApp()
app.configure_serialization(ensure_ascii=False)
```

The shared `df_dumps(..., ensure_ascii=...)` API was released in
[`azure-functions` 2.4.0b1](https://github.com/Azure/azure-functions-python-library/releases/tag/2.4.0b1),
which is the minimum required shared SDK version. Dependency installation
selects a compatible release automatically. If an older SDK is manually
installed despite that requirement, configuring the opt-in raises `RuntimeError`
rather than silently ignoring it. Leaving the option unset, or explicitly
choosing `ensure_ascii=True`, preserves the existing JSON encoding.

There is one encoding policy per Python worker process. It applies to both
synchronous and asynchronous durable clients, orchestration and entity workers,
activity binding outputs, and all registered blueprints, including handlers
created before configuration. Configure every scaled-out worker consistently
before invocations. Repeating the same setting is allowed; a conflicting setting
or changing the policy after the first durable JSON serialization raises
`ValueError`.

Literal Unicode reduces escaping overhead for non-Latin text. For example,
a JSON object containing 100,000 Cyrillic characters occupies approximately
200,012 UTF-8 bytes rather than 600,012 ASCII-escaped bytes. Actual reduction
depends on the payload. The shared codec preserves surrogate-code-unit and
ASCII DEL escapes, custom-object envelopes, and strict/loose typing semantics.
This option does not normalize Unicode or change the decoder; existing escaped
payloads remain readable.

> [!WARNING]
> Enabling or disabling this option changes serialized bytes for non-ASCII
> text. Review applications deriving hashes, signatures, IDs, or replay
> decisions from serialized JSON before rolling out or rolling back a change.
> Do not change the setting inside orchestrator code or during invocations.

The policy applies before large-payload externalization, so uploads use the
selected JSON encoding and size thresholds use its serialized UTF-8 bytes.
Reduced escaping does not eliminate aggregate request limits or replace
externalization. Protocol envelopes, reference tokens, management responses, and
JSON serialized explicitly by application code are not reformatted by this
setting.

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
> `blob:v1:<container>:<blobName>` and `blob:v2:<fullBlobUrl>`.
> New uploads use v2; both formats default to reads within the configured
> Storage endpoint and container.
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
> Older Python SDK versions cannot resolve v2 references. Upgrade all readers
> before enabling v2 writers; see the
> [core compatibility guidance](../docs/features.md#cross-sdk-compatibility).
> New compressed uploads also set `Content-Encoding: gzip`; older Python
> readers may decode them twice. Existing headerless blobs retain their
> original compression-setting requirement.

`BlobPayloadStore` uses its configured credentials and defaults to reads within
the configured account endpoint and container. Explicit account-wide and
additional-account scopes are available; see
[Blob payload access scopes](../docs/features.md#blob-payload-access-scopes).
References outside the configured scopes fail before Storage access.
Legacy v2 query parameters and fragments are ignored; only the store's configured
credentials are used. This location check does not provide per-instance
authorization within the payload container. Use
least-privilege credentials scoped to the intended payload storage, and
explicitly decide whether external callers may supply references.

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
