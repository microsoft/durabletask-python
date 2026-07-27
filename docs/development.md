# Development

The following is more information about how to develop this project.

## Validation

Use Nox to run the representative local correctness checks:

```sh
python -m pip install -r dev-requirements.txt
nox -s ci
```

The `ci` session runs linting, strict type checks, core SDK and Azure Managed
tests on Python 3.10, Azure Functions unit tests on Python 3.13, and Azure
Functions end-to-end tests on Python 3.13. It intentionally runs one
representative version rather than the complete CI matrix, keeping routine
local validation fast. To use another supported version (3.10--3.14) for the
core SDK and Azure Managed tests, pass it after `--`:

```sh
nox -s ci -- 3.14
```

Run the versioned test sessions directly when a change needs complete matrix
coverage, for example `nox -s core_tests` or `nox -s azuremanaged_tests`.
Nox fails rather than silently skipping a session when its required Python
interpreter is unavailable.

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
up without reinstalling. Pass file or directory paths after `--` to lint and
type-check sessions. Pytest-based sessions also accept test node IDs and pytest
selectors such as `-k`. Nox forwards these arguments unchanged to the
underlying tool, and multiple paths or selectors are supported.

For a focused change, run just the relevant session:

```sh
nox -R -s lint -- durabletask/client.py tests/durabletask/test_client.py
nox -R -s typecheck_core -- durabletask/client.py durabletask/extensions/history_export
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

These commands require `make`. If you're using Windows, you can install `make`
using [Chocolatey](https://chocolatey.org/) or use WSL.

```sh
pip3 install -r dev-requirements.txt
make gen-proto
```

This will download the `orchestrator_service.proto` from the `microsoft/durabletask-protobuf` repo
and compile it using `grpcio-tools`. The version of the source proto file that was downloaded can be
found in the file `durabletask/internal/PROTO_SOURCE_COMMIT_HASH`.
