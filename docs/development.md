# Development

The following is more information about how to develop this project. Note that development commands
require that `make` is installed on your local machine. If you're using Windows, you can install
`make` using [Chocolatey](https://chocolatey.org/) or use WSL.

## Validation

Use Nox to run the same correctness checks as the GitHub Actions workflows:

```sh
python -m pip install -r dev-requirements.txt
nox -s ci
```

The `ci` session runs linting, strict type checks, the core SDK test matrix
(Python 3.10--3.14), Azure Managed emulator tests, Azure Functions unit tests,
and Azure Functions end-to-end tests. Install each Python version in the matrix
to run the complete suite locally. Nox reports a missing interpreter clearly
when one is unavailable.

Nox starts Azurite automatically for the core and Azure Functions tests. The
Azure Managed tests start a disposable DTS emulator Docker container. Start
Docker before running that session. Azure Functions end-to-end tests also
require the Azure Functions Core Tools (`func`) on `PATH`. Azure Managed Nox
runs use a disposable DTS emulator with an automatically assigned port, and
Functions E2E uses a unique Durable Functions hub. Both can run alongside other
local validation sessions.

CodeQL remains a GitHub-hosted security scan; it requires the CodeQL CLI and
its query packs, so it is not included in the local `ci` session.

For iterative validation, use `-R` to reuse the session virtual environment.
The local packages are editable in test sessions, so source changes are picked
up without reinstalling. Pass file paths, test node IDs, or pytest selectors
after `--`; Nox forwards them unchanged to the underlying tool. Multiple paths
and selectors are supported.

For a focused change, run just the relevant session:

```sh
nox -R -s lint -- durabletask/client.py tests/durabletask/test_client.py
nox -R -s typecheck_core -- durabletask/client.py examples/history_export
nox -R -s typecheck_functions -- azure-functions-durable/azure
nox -R -s core_tests-3.10 -- tests/durabletask/test_client.py::test_get_grpc_channel_insecure
nox -R -s azuremanaged_tests-3.10 -- tests/durabletask-azuremanaged/test_dts_orchestration_e2e.py
nox -R -s functions_unit-3.13 -- tests/azure-functions-durable/test_client_compat.py
nox -R -s functions_e2e -- -k "dtask_client"
```

> [!NOTE]
> The Azure Managed session still starts a fresh isolated DTS emulator for every
> run, and Functions E2E starts a fresh Functions host. `-R` skips environment
> provisioning, not these required runtime services.

## Generating protobufs

```sh
pip3 install -r dev-requirements.txt
make gen-proto
```

This will download the `orchestrator_service.proto` from the `microsoft/durabletask-protobuf` repo
and compile it using `grpcio-tools`. The version of the source proto file that was downloaded can be
found in the file `durabletask/internal/PROTO_SOURCE_COMMIT_HASH`.
