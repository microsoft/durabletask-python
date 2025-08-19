# Feature overview

The following features are currently supported:

### Orchestrations

Orchestrators are implemented using ordinary Python functions that take an `OrchestrationContext` as their first parameter. The `OrchestrationContext` provides APIs for starting child orchestrations, scheduling activities, and waiting for external events, among other things. Orchestrations are fault-tolerant and durable, meaning that they can automatically recover from failures and rebuild their local execution state. Orchestrator functions must be deterministic, meaning that they must always produce the same output given the same input.

#### Orchestration versioning

Orchestrations may be assigned a version when they are first created. If an orchestration is given a version, it will continually be checked during its lifecycle to ensure that it remains compatible with the underlying orchestrator code. If the orchestrator code is updated while an orchestration is running, rules can be set that will define the behavior - whether the orchestration should fail, abandon for reprocessing at a later time, or attempt to run anyway. For more information, see [Orchestration versioning in Durable Functions](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-orchestration-versioning) (Note that concepts specific to Azure Functions, such as host.json settings, do not apply) and [The provided examples](./supported-patterns.md)

### Activities

Activities are implemented using ordinary Python functions that take an `ActivityContext` as their first parameter. Activity functions are scheduled by orchestrations and have at-least-once execution guarantees, meaning that they will be executed at least once but may be executed multiple times in the event of a transient failure. Activity functions are where the real "work" of any orchestration is done.

### Durable timers

Orchestrations can schedule durable timers using the `create_timer` API. These timers are durable, meaning that they will survive orchestrator restarts and will fire even if the orchestrator is not actively in memory. Durable timers can be of any duration, from milliseconds to months.

### Sub-orchestrations

Orchestrations can start child orchestrations using the `call_sub_orchestrator` API. Child orchestrations are useful for encapsulating complex logic and for breaking up large orchestrations into smaller, more manageable pieces.

### External events

Orchestrations can wait for external events using the `wait_for_external_event` API. External events are useful for implementing human interaction patterns, such as waiting for a user to approve an order before continuing.

### Continue-as-new

Orchestrations can be continued as new using the `continue_as_new` API. This API allows an orchestration to restart itself from scratch, optionally with a new input.

### Suspend, resume, and terminate

Orchestrations can be suspended using the `suspend_orchestration` client API and will remain suspended until resumed using the `resume_orchestration` client API. A suspended orchestration will stop processing new events, but will continue to buffer any that happen to arrive until resumed, ensuring that no data is lost. An orchestration can also be terminated using the `terminate_orchestration` client API. Terminated orchestrations will stop processing new events and will discard any buffered events.

### Retry policies

Orchestrations can specify retry policies for activities and sub-orchestrations. These policies control how many times and how frequently an activity or sub-orchestration will be retried in the event of a transient error.