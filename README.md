# Durable Task Client SDK for Python

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Build Validation](https://github.com/microsoft/durabletask-python/actions/workflows/pr-validation.yml/badge.svg)](https://github.com/microsoft/durabletask-python/actions/workflows/pr-validation.yml)

This repo contains a Python client SDK for use with the [Durable Task Framework for Go](https://github.com/microsoft/durabletask-go) and [Dapr Workflow](https://docs.dapr.io/developing-applications/building-blocks/workflow/workflow-overview/). With this SDK, you can define, schedule, and manage durable orchestrations using ordinary Python code.

⚠️ **This SDK is currently under active development and is not yet ready for production use.** ⚠️

> Note that this project is **not** currently affiliated with the [Durable Functions](https://docs.microsoft.com/azure/azure-functions/durable/durable-functions-overview) project for Azure Functions. If you are looking for a Python SDK for Durable Functions, please see [this repo](https://github.com/Azure/azure-functions-durable-python).

## Features

- [x] Orchestrations
- [x] Activities
- [x] Durable timers
- [x] Sub-orchestrations
- [ ] External events
- [ ] Suspend and resume
- [ ] Retry policies

## Supported patterns

The following orchestration patterns are currently supported.

### Function chaining

An orchestration can chain a sequence of function calls using the following syntax:

```python
def hello(ctx: task.ActivityContext, name: str) -> str:
    return f'Hello {name}!'


def sequence(ctx: task.OrchestrationContext, _):
    result1 = yield ctx.call_activity(hello, input='Tokyo')
    result2 = yield ctx.call_activity(hello, input='Seattle')
    result3 = yield ctx.call_activity(hello, input='London')

    return [result1, result2, result3]
```

### Fan-out/fan-in

An orchestration can fan-out a dynamic number of function calls in parallel and then fan-in the results using the following syntax:

```python
def get_work_items(ctx: task.ActivityContext, _) -> List[str]:
    # ...

def process_work_item(ctx: task.ActivityContext, item: str) -> int:
    # ...

def orchestrator(ctx: task.OrchestrationContext, _):
    work_items = yield ctx.call_activity(get_work_items)

    tasks = [ctx.call_activity(process_work_item, input=item) for item in work_items]
    results = yield task.when_all(tasks)

    return {'work_items': work_items, 'results': results, 'total': sum(results)}
```

## Getting Started

### Prerequisites

- Python 3.10 or higher
- A Durable Task-compatible sidecar, like [Dapr Workflow](https://docs.dapr.io/developing-applications/building-blocks/workflow/workflow-overview/)

### Installing the Durable Task Python client SDK

Installation is currently only supported from source. Ensure pip, setuptools, and wheel are up-to-date.

```sh
python3 -m pip install --upgrade pip setuptools wheel
```

To install this package from source, clone this repository and run the following command from the project root:

```sh
python3 -m pip install .
```

### Run the samples

See the [examples](./examples) directory for a list of sample orchestrations and instructions on how to run them.

## Development

The following is more information about how to develop this project. Note that development commands require that `make` is installed on your local machine. If you're using Windows, you can install `make` using [Chocolatey](https://chocolatey.org/) or use WSL.

### Generating protobufs

Protobuf definitions are stored in the [./submodules/durabletask-proto](./submodules/durabletask-proto) directory, which is a submodule. To update the submodule, run the following command from the project root:

```sh
git submodule update --init
```

Once the submodule is available, the corresponding source code can be regenerated using the following command from the project root:

```sh
make proto-gen
```

### Running unit tests

Unit tests can be run using the following command from the project root. Unit tests _don't_ require a sidecar process to be running.

```sh
make test-unit
```

### Running E2E tests

The E2E (end-to-end) tests require a sidecar process to be running. You can run a sidecar process using the following `docker` command (assumes you have Docker installed on your local system.)

```sh
docker run --name durabletask-sidecar -p 4001:4001 --env 'DURABLETASK_SIDECAR_LOGLEVEL=Debug' --rm cgillum/durabletask-sidecar:latest start --backend Emulator
```

To run the E2E tests, run the following command from the project root:

```sh
make test-e2e
```

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
