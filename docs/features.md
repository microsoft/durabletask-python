# Feature overview

The following features are currently supported:

### Orchestrations

Orchestrators are implemented using ordinary Python functions that take an `OrchestrationContext` as their first parameter. The `OrchestrationContext` provides APIs for starting child orchestrations, scheduling activities, and waiting for external events, among other things. Orchestrations are fault-tolerant and durable, meaning that they can automatically recover from failures and rebuild their local execution state. Orchestrator functions must be deterministic, meaning that they must always produce the same output given the same input.

#### Orchestration versioning

Orchestrations may be assigned a version when they are first created. If an orchestration is given a version, it will continually be checked during its lifecycle to ensure that it remains compatible with the underlying orchestrator code. If the orchestrator code is updated while an orchestration is running, rules can be set that will define the behavior - whether the orchestration should fail, abandon for reprocessing at a later time, or attempt to run anyway. For more information, see [The provided examples](./supported-patterns.md). For more information about versioning in the context of Durable Functions, see [Orchestration versioning in Durable Functions](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-orchestration-versioning) (Note that concepts specific to Azure Functions, such as host.json settings, do not apply to this SDK).

##### Orchestration versioning options

Both the Durable worker and durable client have versioning configuration available. Because versioning checks are handled by the worker, the only information the client needs is a default_version, taken in its constructor, to use as the version for new orchestrations unless otherwise specified. The worker takes a VersioningOptions object with a `default_version` for new sub-orchestrations, a `version` used by the worker for orchestration version comparisons, and two more options giving control over versioning behavior in case of match failures, a `VersionMatchStrategy` and `VersionFailureStrategy`.

**VersionMatchStrategy**

| VersionMatchStrategy.NONE | VersionMatchStrategy.STRICT | VersionMatchStrategy.CURRENT_OR_OLDER |
|-|-|-|
| Do not compare orchestration versions | Only allow orchestrations with the same version as the worker | Allow orchestrations with the same or older version as the worker |

**VersionFailureStrategy**

| VersionFailureStrategy.REJECT | VersionFailureStrategy.FAIL |
|-|-|
| Abandon execution of the orchestrator, but allow it to be reprocessed later | Fail the orchestration |

**Strategy examples**

Scenario 1: You are implementing versioning for the first time in your worker. You want to have a default version for new orchestrations, but do not care about comparing versions with currently running ones. Choose VersionMatchStrategy.NONE, and VersionFailureStrategy does not matter.

Scenario 2: You are updating an orchestrator's code, and you do not want old orchestrations to continue to be processed on the new code. Bump the default version and the worker version, set VersionMatchStrategy.STRICT and VersionFailureStrategy.FAIL.

Scenario 3: You are updating an orchestrator's code, and you have ensured the code is version-aware so that it remains backward-compatible with existing orchestrations. Bump the default version and the worker version, and set VersionMatchStrategy.CURRENT_OR_OLDER and VersionFailureStrategy.FAIL.

Scenario 4: You are performing a high-availability deployment, and your orchestrator code contains breaking changes making it not backward-compatible. Bump the default version and the worker version, and set VersionFailureStrategy.REJECT and VersionMatchStrategy.STRICT. Ensure that at least a few of the previous version of workers remain available to continue processing the older orchestrations - eventually, all older orchestrations _should_ land on the correct workers for processing. Once all remaining old orchestrations have been processed, shut down the remaining old workers.

### Activities

Activities are implemented using ordinary Python functions that take an `ActivityContext` as their first parameter. Activity functions are scheduled by orchestrations and have at-least-once execution guarantees, meaning that they will be executed at least once but may be executed multiple times in the event of a transient failure. Activity functions are where the real "work" of any orchestration is done.

### Durable timers

Orchestrations can schedule durable timers using the `create_timer` API. These timers are durable, meaning that they will survive orchestrator restarts and will fire even if the orchestrator is not actively in memory. Durable timers can be of any duration, from milliseconds to months.

### Sub-orchestrations

Orchestrations can start child orchestrations using the `call_sub_orchestrator` API. Child orchestrations are useful for encapsulating complex logic and for breaking up large orchestrations into smaller, more manageable pieces. Sub-orchestrations can also be versioned in a similar manner to their parent orchestrations, however, they do not inherit the parent orchestrator's version. Instead, they will use the default_version defined in the current worker's VersioningOptions unless otherwise specified during `call_sub_orchestrator`.

### External events

Orchestrations can wait for external events using the `wait_for_external_event` API. External events are useful for implementing human interaction patterns, such as waiting for a user to approve an order before continuing.

### Continue-as-new

Orchestrations can be continued as new using the `continue_as_new` API. This API allows an orchestration to restart itself from scratch, optionally with a new input.

### Suspend, resume, and terminate

Orchestrations can be suspended using the `suspend_orchestration` client API and will remain suspended until resumed using the `resume_orchestration` client API. A suspended orchestration will stop processing new events, but will continue to buffer any that happen to arrive until resumed, ensuring that no data is lost. An orchestration can also be terminated using the `terminate_orchestration` client API. Terminated orchestrations will stop processing new events and will discard any buffered events.

### Retry policies

Orchestrations can specify retry policies for activities and sub-orchestrations. These policies control how many times and how frequently an activity or sub-orchestration will be retried in the event of a transient error.