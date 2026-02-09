# Copilot Instructions for durabletask-python

## Project Overview

This is the Durable Task Python SDK, providing a client and worker for
building durable orchestrations. The repo contains two packages:

- `durabletask` — core SDK (in `durabletask/`)
- `durabletask.azuremanaged` — Azure Durable Task Scheduler provider (in `durabletask-azuremanaged/`)

## Language and Style

- Python 3.10+ is required.
- Use type hints for all public API signatures.
- Follow PEP 8 conventions.
- Use `autopep8` for Python formatting.

## Python Linting

This repository uses [flake8](https://flake8.pycqa.org/) for Python
linting. Run it after making changes to verify there are no issues:

```bash
flake8 path/to/changed/file.py
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
pymarkdown -c .pymarkdown.json scan path/to/file.md
```

To lint all Markdown files in the repository:

```bash
pymarkdown -c .pymarkdown.json scan **/*.md
```

Install the linter via the dev dependencies:

```bash
pip install -r dev-requirements.txt
```

## Building and Testing

Install the packages locally in editable mode:

```bash
pip install -e . -e ./durabletask-azuremanaged
```

Run tests with pytest:

```bash
pytest
```

## Project Structure

- `durabletask/` — core SDK source
- `durabletask-azuremanaged/` — Azure managed provider source
- `examples/` — example orchestrations (see `examples/README.md`)
- `tests/` — test suite
- `dev-requirements.txt` — development dependencies
