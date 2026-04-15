# Copilot Instructions for durabletask-python

## Project Overview

This is the Durable Task Python SDK, providing a client and worker for
building durable orchestrations. The repo contains two packages:

- `durabletask` — core SDK (in `durabletask/`)
- `durabletask.azuremanaged` — Azure Durable Task Scheduler provider (in `durabletask-azuremanaged/`)

## Changelog Requirements

- ALWAYS document user-facing changes in the appropriate changelog under
  `## Unreleased`.
- Update `CHANGELOG.md` for core SDK changes and
  `durabletask-azuremanaged/CHANGELOG.md` for provider changes.
- If a change affects both packages, update both changelogs.
- Include changelog entries for externally observable outcomes only, such as
  new public APIs, behavior changes, bug fixes users can notice, breaking
  changes, and new configuration capabilities.
- Do NOT document internal-only changes in changelogs, including CI/workflow
  updates, test-only changes, refactors with no user-visible behavior change,
  and implementation details that do not affect public behavior or API.
- When in doubt, write the changelog entry in terms of user impact (what users
  can now do or what behavior changed), not implementation mechanism (how it
  was implemented internally).

Examples:
- Include: "Added `get_orchestration_history()` to retrieve orchestration history from the client."
- Exclude: "Added internal helper functions to aggregate streamed history chunks."

## Language and Style

- Python 3.10+ is required.
- Use type hints for all public API signatures.
- Follow PEP 8 conventions.
- Use `autopep8` for Python formatting.

## Python Type Checking

Before linting, check for and fix any Pylance errors in the files you
changed. Use the editor's diagnostics (or the `get_errors` tool) to
identify type errors and resolve them first — type safety takes
priority over style.

## Python Linting

This repository uses [flake8](https://flake8.pycqa.org/) for Python
linting. Run it after making changes to verify there are no issues:

```bash
python -m flake8 path/to/changed/file.py
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

Install the packages locally in editable mode:

```bash
python -m pip install -e . -e ./durabletask-azuremanaged
```

Run tests with pytest:

```bash
python -m pytest
```

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
- `examples/` — example orchestrations (see `examples/README.md`)
- `tests/` — test suite
- `dev-requirements.txt` — development dependencies
