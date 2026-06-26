# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

ADDED

- Added a pluggable `DataConverter` (`durabletask.serialization`) accepted by
  `TaskHubGrpcWorker`, `TaskHubGrpcClient`, and `AsyncTaskHubGrpcClient` via a
  `data_converter` argument. Every payload boundary (inputs, outputs, events,
  custom status, entity state) routes through it, so one converter controls how
  Python values become JSON and how they are reconstructed. The default
  `JsonDataConverter` preserves existing behavior, so a custom converter (for
  example one backed by pydantic) is fully opt-in.
- Custom objects can participate in serialization by exposing a `to_json()`
  method and a `from_json(value)` classmethod. Both are honored recursively, so
  nested custom objects round-trip through their own hooks.
- Payloads are reconstructed into a caller-supplied type — dataclasses
  (including nested fields), `from_json()`-capable types, and `enum.Enum`
  members, recursing through `list`, `dict`, `tuple`, and `Optional`/`Union`
  hints. The type comes from a function's annotations, from an explicit
  `return_type` on `call_activity` / `call_sub_orchestrator` / `call_entity`
  (or `data_type` on `wait_for_external_event`), or from the typed accessors
  `get_input()` / `get_output()` / `get_custom_status()` on
  `client.OrchestrationState` and `EntityMetadata.get_typed_state(...)`. It is
  never inferred from the payload. Which annotated types are eligible is decided
  by the converter via the overridable `DataConverter.can_reconstruct(...)`; a
  custom converter can override it to recognize its own types (for example
  `pydantic.BaseModel` subclasses).

CHANGED

- Custom objects (dataclasses, `SimpleNamespace`, namedtuples) are now
  serialized as plain JSON. Decoding such a payload *without* a type hint now
  yields a plain `dict` (previously a `SimpleNamespace`; a namedtuple now
  round-trips as a JSON array). To get the original type back, supply a type via
  one of the mechanisms above. Payloads produced by older SDK versions still
  deserialize — including into a `SimpleNamespace` when no type is supplied — so
  in-flight orchestrations continue to replay across an upgrade.
- JSON serialization failures now raise a `TypeError` that chains the original
  error (`__cause__`) and names the offending type.
- `EntityContext.get_state()` / `DurableEntity.get_state()` now return a freshly
  reconstructed value on every call rather than a reference to a single cached
  object, so mutating the returned value in place no longer affects persisted
  state — write it back with `set_state()`. State is also serialized eagerly at
  `set_state()` time, so a non-serializable value fails inside the operation
  (which rolls back) instead of after the batch has run.

FIXED

- Falsy entity states (`0`, `""`, `[]`, `{}`) are no longer dropped when an
  entity batch is persisted. Previously a falsy current state was treated as
  "no state" and written as `None`, effectively deleting it; only an actual
  `None` state now clears the persisted entity state.

DEPRECATED

- `durabletask.internal.shared.to_json` and `durabletask.internal.shared.from_json`
  are deprecated and now emit a `DeprecationWarning`. Use a
  `durabletask.serialization.DataConverter` (for example the default
  `JsonDataConverter`) instead. The functions continue to work for backwards
  compatibility.

BREAKING CHANGES (no runtime impact for typical users)

Most of these are type-level only: because the package ships `py.typed`,
consumers running strict type checkers (pyright/mypy) — or subclassing the
public abstract types — may need to update their code. The constructor change
below also affects callers who *directly* construct the named classes, which is
uncommon since they are normally handed to you by the SDK.

- `OrchestrationContext.call_activity`, `call_sub_orchestrator`, `call_entity`,
  and `wait_for_external_event` gained new keyword-only parameters
  (`return_type` / `data_type`). Subclasses overriding these methods should add
  the parameter to match the base signature.
- `EntityContext` and `EntityMetadata` (and its `from_entity_metadata` /
  `from_entity_response` factories) now require a `data_converter` argument.
  These objects are normally constructed by the SDK — you receive an
  `EntityContext` in an entity function and an `EntityMetadata` from the client —
  so this only affects code that constructs them directly.

## v1.6.0

ADDED

- Added overridable activity-dispatch hooks `_on_activity_execution_started`
  and `_on_activity_execution_completed` on `TaskHubGrpcWorker`, invoked
  immediately before each activity runs and in a `finally` after it completes
  or fails. Subclasses can override these to observe in-flight activity
  execution (for example, to track the number of activities currently
  running).
- Added `durabletask.extensions.history_export` for exporting the event history of
  terminal orchestrations to an external destination. Includes
  `ExportHistoryClient`, a per-job `ExportHistoryJobClient` returned by
  `get_job_client(...)`, and `list_jobs(...)` for enumerating jobs by status
  or last-modified window. Ships with a bundled `AzureBlobHistoryExportWriter`
  (installed with `pip install durabletask[history-export-azure]`) and a
  `HistoryWriter` protocol for plugging in custom destinations. Supports both
  `ExportMode.BATCH` (export a window and complete) and `ExportMode.CONTINUOUS`
  (tail terminal instances indefinitely until stopped via `delete_job`).
  Exported blobs are self-describing: each blob carries an explicit
  `schema_version`, the orchestration's `OrchestrationState` metadata, and
  the full ordered event list. Blob names are a lowercase-hex SHA-256 of
  ``{last_updated_at}|{instance_id}`` with the format extension appended
  (matches the .NET `ExportInstanceHistoryActivity` naming scheme), so
  re-exporting an instance after a later terminal update lands at a new
  blob path rather than overwriting the previous one, and instance IDs
  that differ only by `/` no longer collide. Each exported blob also
  carries `{"instance_id": <id>}` as destination-side metadata (the Azure
  writer persists this as Azure Blob metadata) so consumers can scan a
  container without parsing each blob body. The export workflow retries each instance up
  to 3 times with exponential backoff (15s/30s/60s), retries failed batches
  up to 3 times, caps in-flight exports via `max_parallel_exports`
  (default 32), continues-as-new every 5 page cycles to bound orchestrator
  history while preserving cumulative totals across continue-as-new segments,
  and re-fetches entity state at the top of every page loop so
  external delete or mark-failed signals stop the orchestrator cleanly.
  Empty-page BATCH checkpoints no longer reset the persisted resume cursor,
  and duplicate `mark_failed` signals are now idempotent no-ops when a job
  is already failed to reduce transition-noise logs.
  `delete_job` actively tears the job down: it clears the entity state,
  terminates the driving orchestrator, waits briefly for it to settle, and
  purges its orchestration history so a re-created job with the same ID
  starts from a clean slate. Per-instance exports refuse to write a blob
  when the target instance has been purged or has re-entered a non-terminal
  state, surfacing the skipped instance as a per-batch failure.
  Job state lives in a durable entity with an explicit state-transition
  matrix (ACTIVE / COMPLETED / FAILED); invalid transitions raise
  `ExportJobInvalidTransitionError`. Persisted entity state uses a
  versioned, schema-stable JSON shape (`STATE_SCHEMA_VERSION`) with no
  embedded Python type metadata. Each export job's driving orchestrator
  uses a deterministic instance ID (`export-job-{job_id}`, exposed via
  `orchestrator_instance_id_for(...)`) so callers can correlate a job ID
  with its orchestrator for logging, monitoring, and restart.

## v1.5.0

BREAKING CHANGES (type-level only — no runtime impact for typical users)

These changes do not alter runtime behavior for clients or activity/orchestrator
authors, but because the package ships `py.typed`, consumers running strict type
checkers (pyright/mypy) against their own code — or subclassing the public
abstract types — may see new type-check errors and need to update their
overrides:

- `OrchestrationContext.create_timer` now returns the specific `TimerTask` type
  (was `CancellableTask`)
  ([#93](https://github.com/microsoft/durabletask-python/issues/93)).
- `OrchestrationContext.wait_for_external_event` now returns `CancellableTask[Any]`
  (was a bare `CancellableTask`).
- `WhenAnyTask` is now generic; `when_any(tasks: Sequence[Task[T]])` returns
  `WhenAnyTask[T]` for better static inference of the completing child task
  ([#94](https://github.com/microsoft/durabletask-python/issues/94)).
  `CompositeTask.on_child_completed` now takes `Task[Any]`.
- `TaskHubGrpcWorker.add_activity` / `add_entity` (and the internal registry
  methods) now require `Activity[Any, Any]` / `Entity[Any, Any]` instead of the
  bare `Activity` / `Entity` aliases.
- `OrchestrationContext.call_entity` / `signal_entity` `input` parameter widened
  from `TInput | None` to `Any` (Liskov-safe for callers; subclass overrides
  using the old narrower type will be flagged).
- gRPC client interceptors now use the public `grpc.ClientCallDetails` /
  `grpc.aio.ClientCallDetails` types instead of private internal namedtuples;
  custom interceptor subclasses should retype their override parameters.
- These changes also broadly improve generic type-safety hints throughout the
  SDK ([#92](https://github.com/microsoft/durabletask-python/issues/92)).

ADDED

- Added context-manager support (`__enter__` / `__exit__`) to
  `TaskHubGrpcClient` so it can be used with `with` statements, mirroring the
  existing `AsyncTaskHubGrpcClient` async-context-manager support and the
  `TaskHubGrpcWorker` pattern. `DurableTaskSchedulerClient` inherits this
  behavior automatically. `__exit__` delegates to `close()`, so the
  resiliency-aware teardown (in-flight recreate thread join, retired-channel
  timer cancellation, and SDK-owned channel cleanup) runs unchanged through the
  new `with` path.
- Added `ReplaySafeLogger` and `OrchestrationContext.create_replay_safe_logger()`
  for suppressing duplicate log messages during orchestrator replay
- Added `GrpcChannelOptions` and `GrpcRetryPolicyOptions` for configuring
  gRPC transport behavior, including message-size limits, keepalive settings,
  and channel-level retry policy service configuration.
- Added optional `channel` and `channel_options` parameters to
  `TaskHubGrpcClient`, `AsyncTaskHubGrpcClient`, and `TaskHubGrpcWorker` to
  support pre-configured channel passthrough and low-level gRPC channel
  customization.
- Added `GrpcWorkerResiliencyOptions` and `GrpcClientResiliencyOptions`, plus
  `resiliency_options` constructor parameters on `TaskHubGrpcClient`,
  `AsyncTaskHubGrpcClient`, and `TaskHubGrpcWorker`, to configure hello
  deadlines, silent-disconnect detection, reconnect backoff, and channel
  recreation thresholds for SDK-managed gRPC connections.
- Added `get_orchestration_history()` and `list_instance_ids()` to the sync
  and async gRPC clients.
- Added in-memory backend support for `StreamInstanceHistory` and
  `ListInstanceIds` so local orchestration tests can retrieve history and page
  terminal instance IDs by completion window.

CHANGED

- `when_any` now copies its input into a new list (`WhenAnyTask(list(tasks))`).
  Previously the task aliased the caller's list, so mutating it after
  construction was visible inside the task; that side effect no longer occurs.

FIXED

- Fixed `EntityInstanceId.__lt__` infinite recursion when compared against a
  non-`EntityInstanceId` operand. It now returns `NotImplemented`, so mixed-type
  comparisons raise `TypeError` cleanly instead of recursing.
- Improved `TaskHubGrpcWorker` recovery from stale or disconnected gRPC streams
  so configured hello timeouts apply on fresh connections, received work resets
  failure tracking, SDK-owned channels are refreshed and cleaned up safely, and
  caller-owned channels are never recreated or closed during reconnects.
- Fixed `TaskHubGrpcWorker` so in-flight and queued work item completions keep
  draining across graceful gRPC stream resets and worker shutdown before the
  worker retires an SDK-owned channel.
- Improved sync and async gRPC clients so repeated transport failures recreate
  SDK-owned channels, while long-poll deadlines, successful replies, and
  application-level RPC errors do not trigger unnecessary channel replacement.
- Fixed `TaskHubGrpcClient.close()` so explicit sync client shutdown now closes
  any previously retired SDK-owned gRPC channels immediately instead of waiting
  for the delayed cleanup timer.

## v1.4.0

ADDED

- Added large payload externalization support for automatically
  offloading oversized orchestration payloads to Azure Blob Storage.
  Install with `pip install durabletask[azure-blob-payloads]`.
  Pass a `BlobPayloadStore` to the worker and client via the
  `payload_store` parameter.
- Added `durabletask.extensions.azure_blob_payloads` extension
  package with `BlobPayloadStore` and `BlobPayloadStoreOptions`
- Added `PayloadStore` abstract base class in
  `durabletask.payload` for custom storage backends
- Added `durabletask.testing` module with `InMemoryOrchestrationBackend` for testing orchestrations
  without a sidecar process
- Added `AsyncTaskHubGrpcClient` for asyncio-based applications using `grpc.aio`
- Added `DefaultAsyncClientInterceptorImpl` for async gRPC metadata interceptors
- Added `get_async_grpc_channel` helper for creating async gRPC channels
- Added orchestration restart client support
- Added batch client actions for purge and query operations across orchestrations and entities
- Added worker work item filtering support
- Added new `work_item_filtering` sample
- Improved distributed tracing support with full span coverage for orchestrations, activities,
  sub-orchestrations, timers, and events

CHANGED

- Improved timer scheduling behavior for orchestrator timers

FIXED:

- Fix unbound variable in entity V1 processing
- Fixed `compute_next_delay` returning `None` when `max_retry_interval` is not set
- Fixed multiple entity-related bugs across ID parsing and failure handling

## v1.3.0

ADDED

- Allow entities with custom names

CHANGED

- Allow task.fail() to be called with Exceptions
- Update type-hinting for Task return sub-types
- Add/update type-hinting for various worker methods

## v1.2.0

ADDED:

- Added new_uuid method to orchestration clients allowing generation of replay-safe UUIDs.
- Added ProtoTaskHubSidecarServiceStub class to allow passing self-generated stubs to worker
- Added support for new event types needed for specific durable backend setups:
  - orchestratorCompleted
  - eventSent
  - eventRaised modified to support entity events

CHANGED:

- Added py.typed marker file to durabletask module
- Updated type hinting on EntityInstanceId.parse() to reflect behavior
- Entity operations now use UUIDs generated with new_uuid

FIXED:

- Mismatched parameter names in call_entity/signal_entity from interface

## v1.1.0

ADDED:

- Allow retrieving entity metadata from the client, with or without state

## v1.0.0

ADDED:

- Allow calling sub-orchestrators by name
- Abandon workitems if unhandled exception occurs in client

CHANGED:

- Improve execution logging
- Supported Python versions are now 3.10- 3.14. Python 3.9 is end of life and has been removed.

FIXED:

- Reduce exposure of Entity context internally

## v0.5.0

- Added support for Durable Entities

## v0.4.1

- Fixed an issue where orchestrations would still throw non-determinism errors even when versioning
  logic should have prevented it

## v0.4.0

- Added support for orchestration and activity tags
- Added support for orchestration versioning and versioning logic in the worker

## v0.3.0

ADDED

- Added `ConcurrencyOptions` class for fine-grained concurrency control with separate limits for
  activities and orchestrations. The thread pool worker count can also be configured.

FIXED

- Fixed an issue where a worker could not recover after its connection was interrupted or severed

## v0.2.1

ADDED

- Added `set_custom_status` orchestrator API
  ([#31](https://github.com/microsoft/durabletask-python/pull/31)) - contributed by
  [@famarting](https://github.com/famarting)
- Added `purge_orchestration` client API
  ([#34](https://github.com/microsoft/durabletask-python/pull/34)) - contributed by
  [@famarting](https://github.com/famarting)
- Added new `durabletask-azuremanaged` package for use with the [Durable Task
  Scheduler](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler)
  - by [@RyanLettieri](https://github.com/RyanLettieri)

CHANGED

- Protos are compiled with gRPC 1.62.3 / protobuf 3.25.X instead of the latest release. This ensures
  compatibility with a wider range of grpcio versions for better compatibility with other packages /
  libraries ([#36](https://github.com/microsoft/durabletask-python/pull/36)) - by
  [@berndverst](https://github.com/berndverst)
- Http and grpc protocols and their secure variants are stripped from the host name parameter if
  provided. Secure mode is enabled if the protocol provided is https or grpcs
  ([#38](https://github.com/microsoft/durabletask-python/pull/38)) - by
  [@berndverst](https://github.com/berndverst)
- Improve ProtoGen by downloading proto file directly instead of using submodule
  ([#39](https://github.com/microsoft/durabletask-python/pull/39) - by
  [@berndverst](https://github.com/berndverst)

CHANGED

- Updated `durabletask-protobuf` submodule reference to latest

## v0.1.1a1

ADDED

- Add recursive flag in terminate_orchestration to support cascade terminate
  ([#27](https://github.com/microsoft/durabletask-python/pull/27)) - contributed by
  [@shivamkm07](https://github.com/shivamkm07)

## v0.1.0

ADDED

- Retry policies for activities and sub-orchestrations
  ([#11](https://github.com/microsoft/durabletask-python/pull/11)) - contributed by
  [@DeepanshuA](https://github.com/DeepanshuA)

FIXED

- Fix try/except in orchestrator functions not being handled correctly
  ([#21](https://github.com/microsoft/durabletask-python/pull/21)) - by
  [@cgillum](https://github.com/cgillum)
- Updated `durabletask-protobuf` submodule reference to latest distributed tracing commit - by
  [@cgillum](https://github.com/cgillum)

## v0.1.0a5

ADDED

- Adds support for secure channels ([#18](https://github.com/microsoft/durabletask-python/pull/18))
  - contributed by [@elena-kolevska](https://github.com/elena-kolevska)

FIXED

- Fix zero argument values sent to activities as None
  ([#13](https://github.com/microsoft/durabletask-python/pull/13)) - contributed by
  [@DeepanshuA](https://github.com/DeepanshuA)

## v0.1.0a3

ADDED

- Add gRPC metadata option ([#16](https://github.com/microsoft/durabletask-python/pull/16)) -
  contributed by [@DeepanshuA](https://github.com/DeepanshuA)

CHANGED

- Removed Python 3.7 support due to EOL
  ([#14](https://github.com/microsoft/durabletask-python/pull/14)) - contributed by
  [@berndverst](https://github.com/berndverst)

## v0.1.0a2

ADDED

- Continue-as-new ([#9](https://github.com/microsoft/durabletask-python/pull/9))
- Support for Python 3.7+ ([#10](https://github.com/microsoft/durabletask-python/pull/10)) -
  contributed by [@DeepanshuA](https://github.com/DeepanshuA)

## v0.1.0a1

Initial release, which includes the following features:

- Orchestrations and activities
- Durable timers
- Sub-orchestrations
- Suspend, resume, and terminate client operations
