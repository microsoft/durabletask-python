# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

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
  `DurableOrchestrationContext.call_http` raises `NotImplementedError` pending a
  durabletask durable-HTTP implementation.
- `DurableOrchestrationContext` also exposes `custom_status` (reflecting the
  value set via `set_custom_status`) and `will_continue_as_new` (True once
  `continue_as_new` has been called). `parent_instance_id`, `function_context`,
  and `histories` raise `NotImplementedError` because durabletask does not
  surface that information on the orchestration context.

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
- `DurableFunctionsClient.rewind` is present as a deprecated stub that raises
  `NotImplementedError`, pending a durabletask rewind implementation.
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
- `DurableOrchestrationContext.call_http` is present as a stub that raises
  `NotImplementedError`, documenting the durable-HTTP gap. `TokenSource` /
  `ManagedIdentityTokenSource` remain constructible but only apply to
  `call_http`, which is not yet supported.
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
- `HttpManagementPayload` now supports mapping-style access
  (`payload["statusQueryGetUri"]`, iteration, `in`, `keys()`/`items()`/`values()`)
  so v1 code that treated the payload as a `dict` keeps working.


## v0.1.0

- Initial implementation
