# Azure Functions Durable (Python) — 2.x

`azure-functions-durable` is the Python SDK provider for
[Durable Azure Functions](https://learn.microsoft.com/azure/azure-functions/durable/),
built on top of the [`durabletask`](https://pypi.org/project/durabletask/) SDK.

> [!NOTE]
> 2.x is a ground-up rewrite of the Durable Functions Python SDK on top of the
> `durabletask` runtime. It is currently a preview (beta) release; APIs may
> change before the stable 2.0.0. See [CHANGELOG.md](CHANGELOG.md) for the
> migration summary from 1.x, including breaking changes.

## Requirements

- Python 3.13+
- The decorator-based Azure Functions programming model (`DFApp` / `Blueprint`)

## Installation

```bash
pip install azure-functions-durable
```

## Overview

Author orchestrations, activities, and entities as Azure Functions and let the
Durable Task runtime handle scheduling, checkpointing, and replay. Both
durabletask-native two-argument functions (`def orchestrator(ctx, input)`) and
v1-style single-argument functions (`def orchestrator(context)`) are supported,
along with class-based entities and a compatibility layer over the v1 API.

Key capabilities include durable orchestrations and sub-orchestrations, durable
timers, external events, durable entities, retries, versioning, durable HTTP
calls (`context.call_http(...)`), recurring scheduled tasks, and history export.

## Links

- [Changelog](CHANGELOG.md)
- [Durable Functions documentation](https://learn.microsoft.com/azure/azure-functions/durable/)
- [`durabletask` on PyPI](https://pypi.org/project/durabletask/)
- [Azure Functions Durable 1.x source](https://github.com/Azure/azure-functions-durable-python)
- [Azure Functions Python library](https://github.com/Azure/azure-functions-python-library)
- [Azure Functions Python worker](https://github.com/Azure/azure-functions-python-worker)
- [Repository](https://github.com/microsoft/durabletask-python)

## License

Licensed under the [MIT License](LICENSE).
