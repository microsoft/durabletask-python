# Development

The following is more information about how to develop this project. Note that development commands require that `make` is installed on your local machine. If you're using Windows, you can install `make` using [Chocolatey](https://chocolatey.org/) or use WSL.

### Generating protobufs

```sh
pip3 install -r dev-requirements.txt
make gen-proto
```

This will download the `orchestrator_service.proto` from the `microsoft/durabletask-protobuf` repo and compile it using `grpcio-tools`. The version of the source proto file that was downloaded can be found in the file `durabletask/internal/PROTO_SOURCE_COMMIT_HASH`.

### Running tests

Tests can be run using the following command from the project root.

```sh
make test
```