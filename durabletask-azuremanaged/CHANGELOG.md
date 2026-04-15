# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

- Added optional `interceptors`, `channel`, and `channel_options` parameters to
  `DurableTaskSchedulerClient`, `AsyncDurableTaskSchedulerClient`, and
  `DurableTaskSchedulerWorker` to allow combining custom gRPC interceptors with
  DTS defaults and to support pre-configured/customized gRPC channels.
- Added `workerid` gRPC metadata on Durable Task Scheduler worker calls for
  improved worker identity and observability.
- Improved sync access token refresh concurrency handling to avoid duplicate
  refresh operations under concurrent access.

## v1.4.0

- Updates base dependency to durabletask v1.4.0
  - Includes restart support, batch actions, work item filtering, timer improvements,
    distributed tracing improvements, and entity bug fixes
- Added `AsyncDurableTaskSchedulerClient` for async/await usage with `grpc.aio`
- Added `DTSAsyncDefaultClientInterceptorImpl` async gRPC interceptor for DTS authentication
- Added `payload_store` parameter to `DurableTaskSchedulerWorker`,
  `DurableTaskSchedulerClient`, and `AsyncDurableTaskSchedulerClient`
  for large payload externalization support
- Added `azure-blob-payloads` optional dependency that installs
  `durabletask[azure-blob-payloads]` — install with
  `pip install durabletask.azuremanaged[azure-blob-payloads]`
- Improved worker timer handling to align with durabletask timer updates

## v1.3.0

- Updates base dependency to durabletask v1.3.0
  - See durabletask changelog for more details

## v1.2.0

- Updates base dependency to durabletask v1.2.0
  - See durabletask changelog for more details

## v1.1.0

CHANGED:

- Updates base dependency to durabletask v1.1.0
  - See durabletask changelog for more details

## v1.0.0

CHANGED:

- Supported Python versions are now 3.10- 3.14. Python 3.9 is end of life and has been removed.
- Updates base dependency to durabletask v1.0.0
  - See durabletask changelog for more details
- Allow logging configuration for DurableTaskSchedulerClient

## v0.4.0

- Updates base dependency to durabletask v0.5.0
  - Adds support for Durable Entities

## v0.3.1

- Updates base dependency to durabletask v0.4.1
  - Fixed an issue where orchestrations would still throw non-determinism errors even when versioning logic should have prevented it

## v0.3.0

- Updates base dependency to durabletask v0.4.0
  - Added support for orchestration and activity tags
  - Added support for orchestration versioning and versioning logic in the worker
