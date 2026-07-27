# Copilot Instructions for durabletask-python

## Project Overview

This repository provides the Durable Task Python SDK and Azure Functions
provider implementations for building durable orchestrations. It contains
three packages:

- `durabletask` — core SDK (in `durabletask/`)
- `durabletask.azuremanaged` — Azure Durable Task Scheduler provider (in `durabletask-azuremanaged/`)
- `azure-functions-durable` — Azure Durable Functions provider (in
  `azure-functions-durable/`)

## Changelog Requirements

- ALWAYS document user-facing changes in the applicable changelog under
  `## Unreleased`. Create that section if the changelog does not yet have one.
- Update `CHANGELOG.md` for core SDK changes,
  `durabletask-azuremanaged/CHANGELOG.md` for Durable Task Scheduler provider
  changes, and `azure-functions-durable/CHANGELOG.md` for Azure Functions
  provider changes.
- If a change affects multiple packages, update each affected package's
  changelog.
- Include changelog entries for externally observable outcomes only, such as
  new public APIs, behavior changes, bug fixes users can notice, breaking
  changes, and new configuration capabilities.
- Do NOT document internal-only changes in changelogs, including CI/workflow
  updates, test-only changes, refactors with no user-visible behavior change,
  and implementation details that do not affect public behavior or API.
- When in doubt, write the changelog entry in terms of user impact (what users
  can now do or what behavior changed), not implementation mechanism (how it
  was implemented internally).
- Changelogs are not covered by the CI Markdown lint step. Review changes to
  them manually.
- Use the current unindented changelog style: category labels such as `ADDED`,
  `CHANGED`, and `FIXED` are plain, unindented lines, and wrapped entry text
  remains unindented rather than being aligned beneath the bullet.

Examples:

- Include: "Added `get_orchestration_history()` to retrieve orchestration history from the client."
- Exclude: "Added internal helper functions to aggregate streamed history chunks."

## Language and Style

- Python 3.10+ is required.
- Use type hints for all public API signatures.
- Follow PEP 8 conventions.
- Use `autopep8` for Python formatting.

## Copyright Headers

Every new Python (`.py`) source file MUST begin with the following copyright
header as the first two lines, followed by a blank line before any code,
docstring, or imports:

```python
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
```

This applies to all hand-written Python files, including `__init__.py` files,
tests, and examples. The only exceptions are auto-generated protobuf files
(`*_pb2.py` and `*_pb2_grpc.py`), which carry their own generated header.

## Python Type Checking

Before linting, check for and fix any Pylance errors in the files you
changed. Use the editor's diagnostics (or the `get_errors` tool) to
identify type errors and resolve them first — type safety takes
priority over style.

## Python Linting

This repository uses [flake8](https://flake8.pycqa.org/) for Python
linting. Run it after making changes to verify there are no issues. Lint
package source and its tests separately, matching CI:

```bash
python -m flake8 durabletask
python -m flake8 tests/durabletask
python -m flake8 durabletask-azuremanaged
python -m flake8 tests/durabletask-azuremanaged
python -m flake8 azure-functions-durable
python -m flake8 tests/azure-functions-durable
```

## Markdown Style

Use GitHub-style callouts for notes, warnings, and tips in Markdown files:

```markdown
> [!NOTE]
> This is a note.

> [!WARNING]
> This is a warning.

> [!TIP]
> This is a tip.
```

Do **not** use bold-text callouts like `**NOTE:**` or `> **Note:**`.

When providing shell commands in Markdown, include both Bash and
PowerShell examples if the syntax differs between them. Common cases
include multiline commands (Bash uses `\` for line continuation while
PowerShell uses a backtick `` ` ``), environment variable syntax, and
path separators. If a command is identical in both shells, a single
example is sufficient.

## Markdown Linting

This repository uses [pymarkdownlnt](https://pypi.org/project/pymarkdownlnt/)
for linting Markdown files. Configuration is in `.pymarkdown.json` at the
repository root.

To lint a single file:

```bash
python -m pymarkdown -c .pymarkdown.json scan path/to/file.md
```

To lint all Markdown files in the repository:

```bash
python -m pymarkdown -c .pymarkdown.json scan **/*.md
```

Install the linter via the dev dependencies:

```bash
python -m pip install -r dev-requirements.txt
```

## Building and Testing

Use the repository-root `.venv` for core and Azure Managed development, and
for Azure Functions Durable linting, type checking, and unit tests. The Azure
Functions Durable package requires Python 3.13+, so use a 3.13+ root virtual
environment for that work. Install packages locally in editable mode:

```bash
python -m pip install -e . -e ./durabletask-azuremanaged \
  -e ./azure-functions-durable
```

Run the applicable unit tests with pytest. Azure Functions Durable unit tests
exclude tests that require an Azure Functions host or external services:

```bash
python -m pytest
python -m pytest tests/azure-functions-durable \
  -m "not dts and not azurite and not functions_e2e"
```

Run Azure Functions Durable E2E tests through Nox, not directly from the root
virtual environment. Nox creates an isolated Python 3.13 session environment,
installs the local packages editable, and links it into each sample Function
app so the Functions worker loads the app's grpc/protobuf dependencies. The
suite requires Azure Functions Core Tools (`func`) on `PATH` and a running
Azurite instance with blob storage on port 10000:

```bash
nox -s functions_e2e
```

After the first successful run, use `nox -R -s functions_e2e` for E2E reruns.
`-R` reuses the Nox environment and skips reinstalls; because the packages are
editable, source changes are still picked up. Pass pytest selectors after `--`,
for example `nox -R -s functions_e2e -- -k "dtask_client"`. Do not manually
activate or modify the per-app `.venv` directories created by Nox.

## Project Structure

- `durabletask/` — core SDK source
  - `payload/` — public payload externalization API (`PayloadStore` ABC,
    `LargePayloadStorageOptions`, helper functions)
  - `extensions/azure_blob_payloads/` — Azure Blob Storage payload store
    (installed via `pip install durabletask[azure-blob-payloads]`)
  - `entities/` — durable entity support
  - `testing/` — in-memory backend for testing without a sidecar
  - `internal/` — protobuf definitions, gRPC helpers, tracing (not public API)
- `durabletask-azuremanaged/` — Azure managed provider source
- `azure-functions-durable/` — Azure Durable Functions provider source
- `examples/` — example orchestrations (see `examples/README.md`)
- `tests/` — test suite
- `dev-requirements.txt` — development dependencies

## External Dependencies

The Azure Functions Durable provider integrates with APIs and runtime behavior
owned by these repositories. Consult their current source when changing
decorators, bindings, converters, or worker integration behavior:

- [Azure Functions Python library](https://github.com/Azure/azure-functions-python-library)
  — application, decorator, and binding APIs.
- [Azure Functions Python worker](https://github.com/Azure/azure-functions-python-worker)
  — function loading, binding conversion, dependency isolation, and invocation
  behavior.

## Cross-Package Compatibility

The `durabletask-azuremanaged` package extends the core `durabletask`
package (e.g. `DurableTaskSchedulerWorker` subclasses
`TaskHubGrpcWorker`). When adding or changing features in
`durabletask/`, always verify that `durabletask-azuremanaged` still
works correctly:

- Check whether the azuremanaged worker, client, or tests override or
  depend on the code you changed.
- Run the azuremanaged unit tests if they exist for the affected area.
- If a new public API is added to the core SDK (e.g. a method on
  `OrchestrationContext`), confirm it is accessible through the
  azuremanaged package and add a test or example if appropriate.
