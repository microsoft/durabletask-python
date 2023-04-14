# Durable Task Client SDK for Python

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Build Validation](https://github.com/microsoft/durabletask-python/actions/workflows/pr-validation.yml/badge.svg)](https://github.com/microsoft/durabletask-python/actions/workflows/pr-validation.yml)

This repo contains a Python client SDK for use with the [Durable Task Framework for Go](https://github.com/microsoft/durabletask-go) and [Dapr Workflow](https://docs.dapr.io/developing-applications/building-blocks/workflow/workflow-overview/). With this SDK, you can define, schedule, and manage durable orchestrations using ordinary Python code.

⚠️ **This SDK is currently under active development and is not yet ready for production use.** ⚠️

> Note that this project is **not** currently affiliated with the [Durable Functions](https://docs.microsoft.com/azure/azure-functions/durable/durable-functions-overview) project for Azure Functions. If you are looking for a Python SDK for Durable Functions, please see [this repo](https://github.com/Azure/azure-functions-durable-python).

## Getting Started

### Prerequisites

- Python 3.10 or higher

### Installing

#### Install from PyPI
This package is not yet published to [PyPI](https://pypi.org/).

#### Install from source
TODO

## Development
The following is more information about how to develop this project.

### Generating protobufs
If the gRPC proto definitions need to be updated, the corresponding source code can be regenerated using the following command from the project root:

```bash
python3 -m grpc_tools.protoc --proto_path=./submodules/durabletask-protobuf/protos  --python_out=./durabletask/protos --pyi_out=./durabletask/protos --grpc_python_out=./durabletask/protos orchestrator_service.proto
```

### Linting and running unit tests

See the [pr-validation.yml](.github/workflows/pr-validation.yml) workflow for the full list of commands that are run as part of the CI/CD pipeline.

### Running E2E tests

The E2E (end-to-end) tests require a sidecar process to be running. You can run a sidecar process using the following `docker` command (assumes you have Docker installed on your local system.)

```sh
docker run --name durabletask-sidecar -p 4001:4001 --env 'DURABLETASK_SIDECAR_LOGLEVEL=Debug' --rm cgillum/durabletask-sidecar:latest start --backend Emulator
```

To run the E2E tests, run the following command from the project root (we assume you have `pytest` installed locally):

```sh
pytest -m e2e --verbose
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
