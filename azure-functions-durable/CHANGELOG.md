# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- `DurableFunctionsClient.rewind_orchestration(...)` (inherited from durabletask)
  rewinds a failed orchestration to its last known good state. The deprecated v1
  `rewind(...)` method now delegates to it instead of raising
  `NotImplementedError`.

- `DFApp.configure_scheduled_tasks()` opts an app in to durabletask scheduled
  tasks by registering the schedule entity and operation orchestrator. Once
  enabled, schedules are managed from a client via
  `durabletask.scheduled.ScheduledTaskClient`. Scheduled tasks are not
  registered unless this method is called.
- `DFApp.configure_history_export()` opts an app in to durabletask history
  export by registering the export-job entity, driving orchestrator, and
  activities. Once enabled, export jobs are driven from a client via
  `durabletask.extensions.history_export.ExportHistoryClient`; supply the
  activities' runtime dependencies with `history_export.bind_context(...)`.
  The instance-enumeration activity uses a Functions-specific implementation
  based on `QueryInstances` because the Durable Functions host extension does
  not implement the `ListInstanceIds` gRPC call the core activity relies on.
- `DurableOrchestrationContext.call_http(...)` for making durable HTTP calls
  from orchestrators, restoring the v1 API. The request is executed by a
  built-in activity and, when the endpoint responds with `202 Accepted` and a
  `Location` header, is automatically polled to completion (honoring
  `Retry-After`). `ManagedIdentityTokenSource` can be supplied to attach a
  Managed Identity bearer token to the request. `DurableHttpRequest` and
  `DurableHttpResponse` are exported from `azure.durable_functions`.

- The `orchestration_trigger` decorator now accepts an `input_type` argument
  (v1 parity). When set, a v1-style `context.get_input()` decodes the input to
  that type; a call-site `expected_type` on `get_input` takes precedence.
- One-argument (Azure Functions / v1-style) entity functions
  (``def entity(context):``) are now supported. The worker detects the entity's
  shape and, for single-argument functions, delivers a functional
  `DurableEntityContext` that wraps the durabletask `EntityContext` and exposes
  the v1 entity API: `entity_name`, `entity_key`, `operation_name`,
  `get_input()`, `get_state()` (with `initializer`), `set_state()`,
  `set_result()`, and `destruct_on_exit()`. The operation result is taken from
  `set_result(...)`, falling back to the function's return value.
  durabletask-native two-argument entity functions and class-based
  (`DurableEntity`) entities continue to work unchanged.
- One-argument (Azure Functions / v1-style) orchestrator functions
  (``def orchestrator(context):``) are now supported. The worker detects the
  orchestrator's arity and, for single-argument functions, delivers a
  functional `DurableOrchestrationContext` that wraps the durabletask
  `OrchestrationContext` and exposes the v1 context API: `get_input()`,
  `call_activity`/`call_activity_with_retry`,
  `call_sub_orchestrator`/`call_sub_orchestrator_with_retry`, `create_timer`,
  `wait_for_external_event`, `continue_as_new`, `set_custom_status`,
  `task_all`/`task_any`, `call_entity`/`signal_entity`, and `new_uuid`/`new_guid`.
  Two-argument (durabletask-native) orchestrators continue to work unchanged.
  `DurableOrchestrationContext.call_http` schedules a durable HTTP call (see the
  durable-HTTP entry above).
- `DurableOrchestrationContext` also exposes `custom_status` (reflecting the
  value set via `set_custom_status`), `will_continue_as_new` (True once
  `continue_as_new` has been called), `parent_instance_id`, and
  `function_context`. Only `histories` raises `NotImplementedError`, because
  durabletask does not surface that information on the orchestration context.

- Backwards-compatible, deprecated aliases on `DurableFunctionsClient` for the
  v1 `DurableOrchestrationClient` method names: `start_new`, `get_status`,
  `get_status_all`, `get_status_by`, `raise_event`, `terminate`,
  `purge_instance_history`, `purge_instance_history_by`, `suspend`, `resume`,
  `restart`, `read_entity_state`, `get_client_response_links`, and
  `wait_for_completion_or_create_check_status_response`. Each delegates to the
  corresponding durabletask method and emits a `DeprecationWarning`; new code
  should use the durabletask names (e.g. `schedule_new_orchestration`,
  `get_orchestration_state`).
- `DurableFunctionsClient.signal_entity` now also accepts the v1
  `operation_input` keyword (alias for `input`); `task_hub_name` and
  `connection_name` are accepted for compatibility and ignored.
- Deprecated v1 compatibility aliases are now exported from
  `azure.durable_functions`: `DurableOrchestrationClient` (alias for
  `DurableFunctionsClient`), `DurableOrchestrationContext`, `DurableEntityContext`,
  `EntityId`, `ManagedIdentityTokenSource`, `TokenSource`, `Entity`, and
  `OrchestrationRuntimeStatus`.
- v1-compatible return-type wrappers `DurableOrchestrationStatus`,
  `PurgeHistoryResult`, and `EntityStateResponse` (exported from
  `azure.durable_functions`). The deprecated client methods now return these:
  `get_status`/`get_status_all`/`get_status_by` return
  `DurableOrchestrationStatus` (wrapping durabletask `OrchestrationState`, with
  v1 attributes like `runtime_status`, `output`, `input_`, `custom_status`, and
  a falsy value for missing instances); `purge_instance_history`/`_by` return
  `PurgeHistoryResult` (with `instances_deleted`); and `read_entity_state`
  returns `EntityStateResponse` (with `entity_exists`/`entity_state`).
- `RetryOptions`, a deprecated shim that maps the v1 millisecond-based
  constructor onto durabletask `RetryPolicy` (which uses `timedelta`).
  `RetryPolicy` is now also exported from `azure.durable_functions`.

### Changed

- `DurableFunctionsClient` is now an async client. Its orchestration and entity
  management methods (e.g. `schedule_new_orchestration`, `get_orchestration_state`,
  `wait_for_orchestration_completion`) are now coroutines and must be awaited.
  This aligns the client with the async API surface of the V1
  `DurableOrchestrationClient`.
- `create_http_management_payload` now accepts either the durabletask
  `(request, instance_id)` signature or the v1 `(instance_id)` signature for
  backwards compatibility.
- `HttpManagementPayload` now subclasses `dict`, so it is directly
  JSON-serializable via `json.dumps(payload)` and supports mapping-style access
  (`payload["statusQueryGetUri"]`, iteration, `in`, `keys()`/`items()`/`values()`)
  so v1 code that treated the payload as a `dict` keeps working.

### Fixed

- Registering a `Blueprint` into a `DFApp` (via `register_functions` /
  `register_blueprint`) no longer raises a duplicate-function-name error for the
  reserved built-in durable-HTTP functions. Both the app and every blueprint
  auto-register those built-ins, so the app now de-duplicates them during
  registration (leaving the blueprint itself unmodified). This restores the
  standard Azure Functions blueprint authoring pattern.
- `durable_client_input` now injects a rich `DurableFunctionsClient` into the
  decorated function's client parameter (the binding's JSON string is converted
  to a client object). Previously the client parameter received the raw string.
- `DurableFunctionsClient` now applies the host-provided
  `maxGrpcMessageSizeInBytes` to the gRPC channel's send/receive message limits
  (when provided), allowing large orchestration payloads to be retrieved. When
  the host does not supply a value, the gRPC library defaults are left in place.
- `DurableOrchestrationContext.current_utc_datetime` is now timezone-aware
  (UTC), matching v1, so comparisons against timezone-aware datetimes (e.g. a
  parsed scheduled-start time) no longer raise.
- `DurableOrchestrationStatus.to_json()` now emits orchestration payloads
  (`output`, `input`, `customStatus`) as their raw JSON representation instead
  of reconstructed Python objects, so the result is always JSON-serializable
  even when payloads are custom types.
- Restored v1 members that were missing on the compatibility types, avoiding
  `AttributeError`/`TypeError` for existing code that used them:
  - `create_http_management_payload(...)` now returns a `dict`-based
    `HttpManagementPayload`, so `json.dumps(payload)` works directly again.
  - `RetryOptions.to_json()` returns the v1
    `firstRetryIntervalInMilliseconds`/`maxNumberOfAttempts` dictionary, and the
    `first_retry_interval_in_milliseconds` / `max_number_of_attempts` getters
    remain available.
  - `DurableOrchestrationStatus.from_json(...)` reconstructs a status from its
    `to_json()` representation (or the equivalent v1 JSON schema).
  - `PurgeHistoryResult.from_json(...)` reconstructs a result from its v1 JSON
    representation.
  - `DurableOrchestrationContext.version` returns the orchestration instance
    version (or `None`).

## v0.1.0

- Initial implementation
