# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

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
- Improved distributed tracing support with full span coverage for orchestrations, activities,
  sub-orchestrations, timers, and events

CHANGED

- Refactored `TaskHubGrpcClient` to share request-building and validation logic
  with `AsyncTaskHubGrpcClient` via module-level helper functions

FIXED:

- Fix unbound variable in entity V1 processing

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

### New (v0.3.0)

- Added `ConcurrencyOptions` class for fine-grained concurrency control with separate limits for
  activities and orchestrations. The thread pool worker count can also be configured.

### Fixed (v0.3.0)

- Fixed an issue where a worker could not recover after its connection was interrupted or severed

## v0.2.1

### New (v0.2.1)

- Added `set_custom_status` orchestrator API
  ([#31](https://github.com/microsoft/durabletask-python/pull/31)) - contributed by
  [@famarting](https://github.com/famarting)
- Added `purge_orchestration` client API
  ([#34](https://github.com/microsoft/durabletask-python/pull/34)) - contributed by
  [@famarting](https://github.com/famarting)
- Added new `durabletask-azuremanaged` package for use with the [Durable Task
  Scheduler](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler)
  - by [@RyanLettieri](https://github.com/RyanLettieri)

### Changes (v0.2.1)

- Protos are compiled with gRPC 1.62.3 / protobuf 3.25.X instead of the latest release. This ensures
  compatibility with a wider range of grpcio versions for better compatibility with other packages /
  libraries ([#36](https://github.com/microsoft/durabletask-python/pull/36)) - by
  [@berndverst](https://github.com/berndverst)
- Http and grpc protocols and their secure variants are stripped from the host name parameter if
  provided. Secure mode is enabled if the protocol provided is https or grpcs
  ([#38](https://github.com/microsoft/durabletask-python/pull/38) - by
  [@berndverst)(https://github.com/berndverst)
- Improve ProtoGen by downloading proto file directly instead of using submodule
  ([#39](https://github.com/microsoft/durabletask-python/pull/39) - by
  [@berndverst](https://github.com/berndverst)

### Updates (v0.2.1)

- Updated `durabletask-protobuf` submodule reference to latest

## v0.1.1a1

### New (v0.1.1a1)

- Add recursive flag in terminate_orchestration to support cascade terminate
  ([#27](https://github.com/microsoft/durabletask-python/pull/27)) - contributed by
  [@shivamkm07](https://github.com/shivamkm07)

## v0.1.0

### New (v0.1.0)

- Retry policies for activities and sub-orchestrations
  ([#11](https://github.com/microsoft/durabletask-python/pull/11)) - contributed by
  [@DeepanshuA](https://github.com/DeepanshuA)

### Fixed (v0.1.0)

- Fix try/except in orchestrator functions not being handled correctly
  ([#21](https://github.com/microsoft/durabletask-python/pull/21)) - by
  [@cgillum](https://github.com/cgillum)
- Updated `durabletask-protobuf` submodule reference to latest distributed tracing commit - by
  [@cgillum](https://github.com/cgillum)

## v0.1.0a5

### New (v0.1.0a5)

- Adds support for secure channels ([#18](https://github.com/microsoft/durabletask-python/pull/18))
  - contributed by [@elena-kolevska](https://github.com/elena-kolevska)

### Fixed (v0.1.0a5)

- Fix zero argument values sent to activities as None
  ([#13](https://github.com/microsoft/durabletask-python/pull/13)) - contributed by
  [@DeepanshuA](https://github.com/DeepanshuA)

## v0.1.0a3

### New (v0.1.0a3)

- Add gRPC metadata option ([#16](https://github.com/microsoft/durabletask-python/pull/16)) -
  contributed by [@DeepanshuA](https://github.com/DeepanshuA)

### Changes (v0.1.0a3)

- Removed Python 3.7 support due to EOL
  ([#14](https://github.com/microsoft/durabletask-python/pull/14)) - contributed by
  [@berndverst](https://github.com/berndverst)

## v0.1.0a2

### New (v0.1.0a2)

- Continue-as-new ([#9](https://github.com/microsoft/durabletask-python/pull/9))
- Support for Python 3.7+ ([#10](https://github.com/microsoft/durabletask-python/pull/10)) -
  contributed by [@DeepanshuA](https://github.com/DeepanshuA)

## v0.1.0a1

Initial release, which includes the following features:

- Orchestrations and activities
- Durable timers
- Sub-orchestrations
- Suspend, resume, and terminate client operations
