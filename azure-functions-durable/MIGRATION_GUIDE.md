# Migrate from Azure Functions Durable 1.x to 2.x

This guide explains how to move an application from
`azure-functions-durable` 1.x to 2.x. Version 2.x is a ground-up rewrite built
on the [`durabletask`](https://pypi.org/project/durabletask/) SDK and its gRPC
runtime.

> [!WARNING]
> `azure-functions-durable` 2.x is currently in preview. Its APIs may change
> before the stable 2.0.0 release.

## Choose a migration path

Version 2.x includes compatibility shims for most 1.x APIs used by
decorator-based apps. This supports a staged migration:

1. **Compatibility upgrade:** update the runtime and dependencies while keeping
   existing one-argument orchestrators, entities, and client calls. These APIs
   continue to work but emit `DeprecationWarning` where a native replacement
   exists.
2. **Native API migration:** move to durabletask-native function signatures,
   client methods, retry policies, and entity types.

Use the compatibility upgrade first when minimizing application changes is more
important than adopting the new API immediately.

## Requirements and breaking changes

Before upgrading, account for these 2.x requirements:

- **Python 3.13 or later is required.**
- **Only the Azure Functions Python V2 programming model is supported.** Apps
  must use `DFApp` or `Blueprint` decorators. The classic programming model with
  one directory and `function.json` per function is not supported.
- **A modern Durable Functions host is required.** Local sample apps use the
  preview extension bundle shown below.
- **The OpenAI Agents integration was removed.** The
  `azure.durable_functions.openai_agents` package is not available in 2.x.
- **The primary APIs now come from `durabletask`.** Compatibility aliases are
  available to stage the migration, but new code should use the native names.

If the app still uses the classic Python programming model, migrate it to the
[decorator-based model](https://learn.microsoft.com/azure/azure-functions/functions-reference-python)
before upgrading this package.

## Prepare running orchestrations

Orchestrator code must remain deterministic and replay-compatible for existing
instances. Before changing function names, activity schedules, branching logic,
or serialized inputs:

- allow existing instances to finish; or
- deploy the migrated app with a new task hub and route new instances there.

Test replay behavior with representative orchestration histories in a
non-production environment before updating a task hub that contains running
instances.

## Perform a compatibility upgrade

### 1. Update the Python runtime

Update local development, CI, and the deployed Function App to Python 3.13 or
later.

### 2. Update dependencies

Replace the 1.x package constraint in `requirements.txt`:

```text
azure-functions>=2.3.0b2
azure-functions-durable>=2.0.0b1,<3
```

### 3. Update the extension bundle

Use the preview extension bundle that contains the required Durable Task gRPC
runtime:

```json
{
  "version": "2.0",
  "extensionBundle": {
    "id": "Microsoft.Azure.Functions.ExtensionBundle.Preview",
    "version": "[4.*, 5.0.0)"
  },
  "extensions": {
    "durableTask": {
      "storageProvider": {
        "type": "AzureStorage"
      }
    }
  }
}
```

### 4. Keep supported 1.x code temporarily

A decorator-based 1.x orchestrator and client starter can run through the 2.x
compatibility layer without changing their function shape:

```python
import azure.functions as func
import azure.durable_functions as df

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.orchestration_trigger(context_name="context")
def hello(context: df.DurableOrchestrationContext):
    name = context.get_input()
    return (yield context.call_activity("say_hello", name))


@app.route(route="start", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start(
        req: func.HttpRequest,
        client: df.DurableFunctionsClient) -> func.HttpResponse:
    instance_id = await client.start_new("hello", client_input="Tokyo")
    return client.create_check_status_response(req, instance_id)
```

The single-argument orchestrator receives the compatibility
`DurableOrchestrationContext`, and `start_new` delegates to the native client.
Use this state to validate the infrastructure upgrade before changing
application APIs.

## Migrate to native orchestration APIs

Native orchestrators accept the durabletask context and input as separate
arguments:

```python
from typing import Any

import azure.functions as func
import azure.durable_functions as df
from durabletask import task

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.orchestration_trigger(context_name="context")
def hello(ctx: task.OrchestrationContext, name: Any):
    return (yield ctx.call_activity("say_hello", input=name))


@app.route(route="start", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start(
            req: func.HttpRequest,
            client: df.DurableFunctionsClient) -> func.HttpResponse:
    instance_id = await client.schedule_new_orchestration("hello", input="Tokyo")
    return client.create_check_status_response(req, instance_id)
```

Activity functions can keep their existing single-input signature:

```python
@app.activity_trigger(input_name="name")
def say_hello(name: str) -> str:
    return f"Hello {name}!"
```

### Orchestration API mapping

| 1.x API | Native 2.x API |
| --- | --- |
| `context.get_input()` | The orchestrator's second argument |
| `context.call_activity(name, input_)` | `ctx.call_activity(name, input=input_)` |
| `context.call_activity_with_retry(...)` | `ctx.call_activity(..., retry_policy=policy)` |
| `context.call_sub_orchestrator(name, input_)` | `ctx.call_sub_orchestrator(name, input=input_)` |
| `call_sub_orchestrator_with_retry` | `ctx.call_sub_orchestrator(..., retry_policy=...)` |
| `context.task_all(tasks)` | `task.when_all(tasks)` |
| `context.task_any(tasks)` | `task.when_any(tasks)` |
| `context.new_guid()` | `ctx.new_uuid()` |
| `df.EntityId(name, key)` | `entities.EntityInstanceId(name, key)` |

Methods including `create_timer`, `wait_for_external_event`,
`continue_as_new`, `set_custom_status`, `call_entity`, and `signal_entity`
retain their names. Activity, sub-orchestration, and entity calls use keyword
`input` instead of the 1.x `input_` or `operationInput` names.
`continue_as_new` uses `new_input`.

### Retry policy mapping

`RetryOptions` accepted millisecond values. Native `RetryPolicy` uses
`timedelta`:

```python
from datetime import timedelta

from durabletask import task

policy = task.RetryPolicy(
    first_retry_interval=timedelta(seconds=1),
    max_number_of_attempts=3,
    backoff_coefficient=2.0,
)

result = yield ctx.call_activity(
    "call_service",
    input=request,
    retry_policy=policy,
)
```

## Migrate client APIs

`durable_client_input` supplies `DurableFunctionsClient` to coroutine functions
and `SyncDurableFunctionsClient` to synchronous functions. Async client calls
must be awaited; the corresponding sync calls must not be awaited.

| 1.x client API | Native 2.x client API |
| --- | --- |
| `start_new` | `schedule_new_orchestration` |
| `get_status` | `get_orchestration_state` |
| `get_status_all` | `get_all_orchestration_states` |
| `get_status_by` | `get_all_orchestration_states` with `OrchestrationQuery` |
| `raise_event` | `raise_orchestration_event` |
| `terminate` | `terminate_orchestration` |
| `suspend` | `suspend_orchestration` |
| `resume` | `resume_orchestration` |
| `restart` | `restart_orchestration` |
| `rewind` | `rewind_orchestration` |
| `purge_instance_history` | `purge_orchestration` |
| `purge_instance_history_by` | `purge_orchestrations_by` |
| `read_entity_state` | `get_entity` |
| `get_client_response_links` | `create_http_management_payload` |

Native `restart_orchestration` reuses the current instance ID by default. Pass
`restart_with_new_instance_id=True` to preserve the 1.x `restart` default.

Native status APIs return durabletask models rather than the compatibility
wrappers `DurableOrchestrationStatus`, `PurgeHistoryResult`, and
`EntityStateResponse`. Update code that serializes or checks those return
values. Use `get_orchestration_history` when history events are required.

## Migrate durable entities

One-argument entity functions remain available through
`DurableEntityContext`. Native code can use a two-argument entity function or a
`DurableEntity` class. A class exposes each method as an entity operation:

```python
from typing import Any

import azure.durable_functions as df
from durabletask.entities import DurableEntity


@app.entity_trigger(context_name="context")
class Counter(DurableEntity):
    def add(self, input_: Any = None) -> int:
        value = self.get_state(int, 0) + (input_ or 0)
        self.set_state(value)
        return value

    def get(self, input_: Any = None) -> int:
        return self.get_state(int, 0)
```

Replace `df.EntityId` with `durabletask.entities.EntityInstanceId` at call
sites. For function-based entities, return the operation result directly
instead of calling `context.set_result`.

## Review compatibility limitations

- `DurableOrchestrationContext.histories` is unavailable. Retrieve history with
  `client.get_orchestration_history(instance_id)`.
- `get_status(show_history=True)` cannot reproduce the v1 top-level `Reason`
  and `Details` fields for failed activities or sub-orchestrations because the
  shared gRPC history protocol represents only structured `FailureDetails`.
  Read `FailureDetails` when present.
- Distributed tracing is not yet wired through the Python provider.
- Continuous history export is not supported by Azure Functions.
- Unusual callable signatures can be misclassified by the compatibility layer.
  Keep compatibility orchestrators and entities to exactly one positional
  argument, and native functions to exactly two.

See the [changelog](CHANGELOG.md) for the complete compatibility surface and
known limitations.

## Validate and deploy

Before production rollout:

1. Run unit tests against Python 3.13 or later.
2. Run the app locally with Azurite and Azure Functions Core Tools.
3. Start new orchestration, sub-orchestration, entity, retry, timer, and
   external-event scenarios used by the app.
4. Verify status queries and HTTP management URLs.
5. Test existing orchestration histories or drain existing instances.
6. Deploy to a staging slot or non-production task hub before production.

The [2.x samples](samples/) provide runnable native examples for common
orchestration patterns.
