# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

ADDED

- Added `SyncDurableFunctionsClient` and
  `DFApp.durable_client_input_sync()` for synchronous durable-client functions.
  Both synchronous and asynchronous Functions durable clients can now use the
  scheduled-tasks and history-export client APIs without an async-to-sync bridge.

## 2.0.0b1

First preview (beta) release of `azure-functions-durable` 2.x — a ground-up
rewrite of the Azure Durable Functions Python SDK built on top of the
[`durabletask`](https://pypi.org/project/durabletask/) SDK. This is a preview
release; APIs may change before the stable 2.0.0.

### Why the rewrite

The 1.x SDK implemented the Durable Functions out-of-process programming model
directly, with its own orchestration action/state model and a JSON protocol
tailored to the classic Durable Functions host extension. 2.x instead builds on
the `durabletask` Python SDK — the same gRPC-based runtime that powers the
Durable Task Scheduler (DTS) and the modern Durable Functions host. Building on
durabletask means:

- a single orchestration/entity execution core, serialization pipeline, and
  retry/versioning implementation shared with the broader durabletask
  ecosystem, instead of a Functions-specific reimplementation;
- Functions users can adopt durabletask-native APIs and patterns directly,
  while existing v1 code keeps working through the compatibility layer; and
- less protocol drift between the Python worker and the durable backend.

### Underlying packages

- [`durabletask`](https://pypi.org/project/durabletask/) — orchestration and
  entity client/worker, executed over gRPC.
- [`azure-functions`](https://pypi.org/project/azure-functions/) — the
  decorator/binding programming model (`DFApp` / `Blueprint`).
- [`azure-identity`](https://pypi.org/project/azure-identity/) — Managed
  Identity token acquisition for durable HTTP calls.

### Breaking changes (from `azure-functions-durable` 1.x)

- **Python 3.13+ is now required** (1.x supported 3.10+). On Functions Python
  workers older than 3.13, worker dependencies are not isolated from your app's
  dependencies, which causes a `grpc` version conflict with the durable runtime
  at load time. Python 3.13 enables worker dependency isolation, avoiding the
  collision.
- **The classic (v1) programming model has been dropped.** Only the
  decorator-based application model (`DFApp` / `Blueprint`, the Python v2
  programming model) is supported; the `function.json`-based model is not.
- **The OpenAI Agents integration has been removed.** The
  `azure.durable_functions.openai_agents` package (durable OpenAI Agents SDK
  orchestration) is not part of 2.x.
- **Runtime target.** 2.x speaks the durabletask gRPC protocol used by the
  Durable Task Scheduler and the modern Durable Functions host, rather than the
  classic Durable Functions extension protocol.
- **Primary client and context APIs use durabletask names.** The main surface
  is now durabletask's (e.g. `schedule_new_orchestration`,
  `get_orchestration_state`, `wait_for_orchestration_completion`). The v1
  `DurableOrchestrationClient` method names remain available as deprecated
  aliases that emit `DeprecationWarning` (see [Deprecated](#deprecated)).

### Added

New capabilities beyond the v1 surface, most inherited from `durabletask`:

- **durabletask-native authoring.** Two-argument orchestrator, entity, and
  activity functions (`def orchestrator(ctx, input)`, `def entity(ctx, input)`,
  `def activity(ctx, input)`) and class-based entities (`DurableEntity`) are
first-class, alongside the supported v1-style single-argument functions. For
activities, `activity_trigger` adapts a two-argument `(ctx, input)` function
to the host's single-input calling convention automatically (the context is a
placeholder object; accessing context attributes raises `NotImplementedError`).
- **`DurableFunctionsClient.rewind_orchestration(...)`** rewinds a failed
  orchestration to its last known good state (inherited from durabletask).
- **`DFApp.configure_scheduled_tasks()`** opts an app in to durabletask
  scheduled (recurring) tasks by registering the schedule entity and operation
  orchestrator. Schedules are then managed from a client via
  `durabletask.scheduled.ScheduledTaskClient`. Scheduled tasks are not
  registered unless this method is called.
- **`DFApp.configure_history_export(writer=...)`** opts an app in to durabletask
  history export by registering the export-job entity, driving orchestrator, and
  activities. Supply the `HistoryWriter` here; the activities resolve their
  durabletask client per invocation from a `durable_client_input` binding, so
  the export activities run correctly across a scaled-out, multi-worker
  deployment (each invocation resolves its own client). This is a correctness
  property, not a large-export throughput guarantee. Export jobs are
  driven from a client via
  `durabletask.extensions.history_export.ExportHistoryClient`. Continuous export
  (`ExportMode.CONTINUOUS`) is not supported on Azure Functions: the
  Functions-registered export entity rejects it at job creation (the job ends
  `FAILED` with an explanatory reason). Continuous tailing needs the host's
  `ListInstanceIds` gRPC call, which the Durable Functions host extension does
  not implement; the instance-enumeration activity uses a Functions-specific
  implementation based on `QueryInstances` for the same reason. This is an
  experimental beta feature intended for bounded, low-volume batch-export
  windows: the `QueryInstances`-based enumeration re-scans and re-sorts the
  terminal-instance population for each batch, so it is not yet suited to
  production-scale history export. Efficient large exports depend on a
  host-side completed-time paging API that the host extension does not yet
  provide.
- **`DurableOrchestrationContext.call_http(...)`** makes durable HTTP calls from
  orchestrators, restoring the v1 API. The request is executed by a built-in
  activity and, when the endpoint responds with `202 Accepted` and a `Location`
  header, is automatically polled to completion (honoring `Retry-After`).
  `ManagedIdentityTokenSource` can be supplied to attach a Managed Identity
  bearer token to the request. `DurableHttpRequest` and `DurableHttpResponse`
  are exported from `azure.durable_functions`.
- **`orchestration_trigger(..., input_type=...)`** decodes a v1-style
  `context.get_input()` to the declared type; a call-site `expected_type` on
  `get_input` takes precedence.

### Compatibility with v1

To ease migration, 2.x ships a compatibility layer over the durabletask
surface:

- **v1-style single-argument functions** (`def orchestrator(context)`,
  `def entity(context)`) are supported. The worker detects the function shape
  and, for single-argument functions, delivers a functional
  `DurableOrchestrationContext` / `DurableEntityContext` that wraps the
  durabletask context and exposes the v1 API — for orchestrations:
  `get_input`, `call_activity`/`call_activity_with_retry`,
  `call_sub_orchestrator`/`call_sub_orchestrator_with_retry`, `create_timer`,
  `wait_for_external_event`, `continue_as_new`, `set_custom_status`,
  `task_all`/`task_any`, `call_entity`/`signal_entity`, `new_uuid`/`new_guid`,
  `custom_status`, `will_continue_as_new`, `parent_instance_id`, and
  `function_context`; and for entities: `entity_name`, `entity_key`,
  `operation_name`, `get_input`, `get_state` (with `initializer`), `set_state`,
  `set_result`, and `destruct_on_exit`. The operation result is taken from
  `set_result(...)`, falling back to the function's return value.
- **v1 return-type wrappers** `DurableOrchestrationStatus`,
  `PurgeHistoryResult`, and `EntityStateResponse` are returned by the deprecated
  client methods and exported from `azure.durable_functions`.
- **`HttpManagementPayload`** subclasses `dict`, so it is directly
  JSON-serializable via `json.dumps(payload)` and supports mapping-style access,
  matching v1 usage.
- **`create_http_management_payload`** accepts either the durabletask
  `(request, instance_id)` or the v1 `(instance_id)` signature.

### Deprecated

These v1 names are retained as shims that delegate to their durabletask
equivalents and emit `DeprecationWarning`; prefer the durabletask names in new
code:

- `DurableOrchestrationClient` (alias for `DurableFunctionsClient`) and its
  method names: `start_new`, `get_status`, `get_status_all`, `get_status_by`,
  `raise_event`, `terminate`, `purge_instance_history`,
  `purge_instance_history_by`, `suspend`, `resume`, `restart`,
  `read_entity_state`, `get_client_response_links`, and
  `wait_for_completion_or_create_check_status_response`.
- `rewind(...)` — delegates to `rewind_orchestration(...)`.
- `signal_entity(..., operation_input=...)` — `operation_input` is an alias for
  `input`; `task_hub_name` / `connection_name` are accepted and ignored.
- `RetryOptions` — maps the v1 millisecond-based constructor onto durabletask
  `RetryPolicy` (which uses `timedelta`). `RetryPolicy` is also exported from
  `azure.durable_functions`.
- Compatibility aliases exported from `azure.durable_functions`:
  `DurableOrchestrationContext`, `DurableEntityContext`, `EntityId`,
  `ManagedIdentityTokenSource`, `TokenSource`, `Entity`, and
  `OrchestrationRuntimeStatus`.

### Known limitations

- Orchestration history is not exposed on the context;
  `DurableOrchestrationContext.histories` raises `NotImplementedError`. Use the
  client's `get_orchestration_history(...)` instead.
- The client status methods accept the v1 `show_history` /
  `show_history_output` flags for signature compatibility but ignore them, so
  the returned status has no `historyEvents`. Use
  `get_orchestration_history(...)` to retrieve history.
- Distributed tracing is not yet wired up. The Durable Functions host delivers
  the parent trace context and emits the orchestration/activity spans itself,
  so orchestrator user-code spans in the Python worker are not yet correlated
  to it, and durabletask's own span emission is intentionally left disabled to
  avoid duplicating the host's spans.
