# Development

The following is more information about how to develop this project. Note that development commands require that `make` is installed on your local machine. If you're using Windows, you can install `make` using [Chocolatey](https://chocolatey.org/) or use WSL.

### Generating protobufs

```sh
pip3 install -r dev-requirements.txt
make gen-proto
```

This will download the `orchestrator_service.proto` from the `microsoft/durabletask-protobuf` repo and compile it using `grpcio-tools`. The version of the source proto file that was downloaded can be found in the file `durabletask/internal/PROTO_SOURCE_COMMIT_HASH`.

### Running unit tests

Unit tests can be run using the following command from the project root. Unit tests _don't_ require a sidecar process to be running.

```sh
make test-unit
```

### Running E2E tests

The E2E (end-to-end) tests require a sidecar process to be running. You can use the Durable Task test sidecar using the following `docker` command:

```sh
docker run --name durabletask-sidecar -p 4001:4001 --env 'DURABLETASK_SIDECAR_LOGLEVEL=Debug' --rm cgillum/durabletask-sidecar:latest start --backend Emulator
```

To run the E2E tests, run the following command from the project root:

```sh
make test-e2e
```