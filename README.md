# Durable Task SDK for Python

[![License:
MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Build
Validation](https://github.com/microsoft/durabletask-python/actions/workflows/pr-validation.yml/badge.svg)](https://github.com/microsoft/durabletask-python/actions/workflows/pr-validation.yml)
[![PyPI version](https://badge.fury.io/py/durabletask.svg)](https://badge.fury.io/py/durabletask)

This repository contains Python SDKs for building durable orchestrations with
[Azure Durable Task Scheduler](https://github.com/Azure/Durable-Task-Scheduler)
and [Azure Durable Functions](https://learn.microsoft.com/azure/azure-functions/durable/):

- [`durabletask`](./durabletask/) is the core orchestration SDK.
- [`durabletask.azuremanaged`](./durabletask-azuremanaged/) is the Azure Durable
  Task Scheduler provider.
- [`azure-functions-durable`](./azure-functions-durable/) is a preview Azure
  Durable Functions provider built on `durabletask`.

## Python Requirements

The core SDK and Azure Managed provider require Python 3.11 or later.
The Azure Functions Durable provider requires Python 3.13 or later.

## References

- [Supported Patterns](./docs/supported-patterns.md)
- [Available Features](./docs/features.md)
- [Getting Started](./docs/getting-started.md)
- [Development Guide](./docs/development.md)
- [Azure Functions Durable 2.x](./azure-functions-durable/README.md)
- [Contributing Guide](./CONTRIBUTING.md)

## Optional Features

### Task Hub Query Filters

Set `OrchestrationQuery.task_hub_names` to filter orchestration queries by one or
more task hubs. The filter is available through synchronous and asynchronous
core and Azure Managed clients; filtering support depends on the backend.

```python
from durabletask.client import OrchestrationQuery

query = OrchestrationQuery(task_hub_names=["orders", "billing"])
```

### Set Serialization

The default `JsonDataConverter` serializes sets and frozensets as JSON arrays
ordered by their canonical JSON representation, not numeric order. For example,
`{2, 10}` serializes as `[10, 2]`. To reconstruct annotated sets and frozensets
instead of receiving lists, configure the same converter on clients and workers:

```python
from durabletask.serialization import JsonDataConverter

converter = JsonDataConverter(enable_generic_sets=True)
```

Pass it using the `data_converter` parameter on core or Azure Managed clients
and workers. `enable_set_coercion=True` is an alias for the same option.
Azure Functions uses its own shared codec rather than this converter.

> [!WARNING]
> Set reconstruction is disabled by default to preserve the order and duplicates
> of recorded JSON arrays. Enabling it for existing orchestrations can change
> replay behavior. Iteration over reconstructed sets is not replay-safe; sort
> values explicitly before using their order to schedule durable work.

### Entity IDs

`EntityInstanceId` values are immutable, hashable, and usable as dictionary keys
or set elements. Construct a new ID to change an entity name or key; attribute
assignment and deletion raise `AttributeError`.

### Large Payload Externalization

Install the `azure-blob-payloads` extra to automatically offload oversized orchestration payloads to
Azure Blob Storage:

```bash
pip install durabletask[azure-blob-payloads]
```

See the [feature documentation](./docs/features.md#large-payload-externalization) and the
[example](./examples/large_payload/) for usage details.

### Failure Details Properties

Configure an `ExceptionPropertiesProvider` to attach portable diagnostic
properties to activity, entity, and orchestration failures:

```python
from durabletask import ExceptionPropertiesProvider
from durabletask.worker import TaskHubGrpcWorker


class FailureProperties(ExceptionPropertiesProvider):
    def get_exception_properties(self, exception: Exception):
        return {"error_code": getattr(exception, "error_code", None)}


worker = TaskHubGrpcWorker(exception_properties_provider=FailureProperties())
```

Properties support `None`, booleans, numbers, strings, nested mappings, and
lists. They are available from `TaskFailedError.details`, orchestration state,
and history as `FailureDetails.properties`.

> [!NOTE]
> Python intentionally treats `.NET`'s `dt:` and `dto:` property strings as
> ordinary strings, matching the Java SDK. Consequently, `datetime` values do
> not retain their type across Python/.NET failure-details property round trips;
> this is a .NET SDK parity gap.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of
Microsoft trademarks or logos is subject to and must follow [Microsoft's Trademark & Brand
Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion
or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those
third-party's policies.
