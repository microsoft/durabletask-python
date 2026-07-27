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

## References

- [Supported Patterns](./docs/supported-patterns.md)
- [Available Features](./docs/features.md)
- [Getting Started](./docs/getting-started.md)
- [Development Guide](./docs/development.md)
- [Azure Functions Durable 2.x](./azure-functions-durable/README.md)
- [Contributing Guide](./CONTRIBUTING.md)

## Optional Features

### Large Payload Externalization

Install the `azure-blob-payloads` extra to automatically offload oversized orchestration payloads to
Azure Blob Storage:

```bash
pip install durabletask[azure-blob-payloads]
```

See the [feature documentation](./docs/features.md#large-payload-externalization) and the
[example](./examples/large_payload/) for usage details.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of
Microsoft trademarks or logos is subject to and must follow [Microsoft's Trademark & Brand
Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion
or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those
third-party's policies.
