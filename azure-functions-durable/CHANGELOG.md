# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed

- `DurableFunctionsClient` is now an async client. Its orchestration and entity
  management methods (e.g. `schedule_new_orchestration`, `get_orchestration_state`,
  `wait_for_orchestration_completion`) are now coroutines and must be awaited.
  This aligns the client with the async API surface of the V1
  `DurableOrchestrationClient`.

## v0.1.0

- Initial implementation
