# In-Memory Backend Example

This example demonstrates how to structure a Durable Task Python project
so that orchestrators and activities are defined separately from
infrastructure code, enabling fast, fully in-process unit tests using the
in-memory backend.

You can copy this entire folder into a new directory and run it as a
standalone project.

## Folder Structure

```text
in_memory_backend_example/
├── README.md
├── requirements.txt
├── src/
│   ├── __init__.py
│   ├── workflows.py   # Orchestrators & activities (pure logic, no infra)
│   └── app.py          # Runs workflows against a real DTS backend
└── test/
    ├── __init__.py
    └── test_workflows.py  # Unit tests using the in-memory backend
```

### Key ideas

- **[src/workflows.py](src/workflows.py)** — All orchestrator and activity
  definitions live here. They import only from `durabletask` and have no
  dependency on a particular backend, making them portable and testable.

- **[src/app.py](src/app.py)** — The entry point that wires up the real
  Durable Task Scheduler (or emulator) client and worker, registers the
  workflow functions, and schedules an orchestration.

- **[test/test_workflows.py](test/test_workflows.py)** — Pytest tests that
  exercise every workflow path using `create_test_backend()`. No sidecar,
  emulator, or Azure resources are needed.

## Getting Started

1. Copy this folder to a new location and `cd` into it:

   ```bash
   cd in_memory_backend_example
   ```

1. Create and activate a virtual environment:

   Bash:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

   PowerShell:

   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

## Running the Tests

From the `in_memory_backend_example/` directory:

```bash
pytest test/
```

## Running the App Against the Emulator

Start the DTS emulator, then from the `in_memory_backend_example/`
directory:

```bash
python -m src.app
```

## Patterns Demonstrated

| Pattern | Where |
|---|---|
| Activity chaining | `process_order` — validate → calculate → pay |
| Fan-out / fan-in | `process_order` — ship all items in parallel |
| Sub-orchestration | `order_with_approval` calls `process_order` |
| Human interaction (external event + timer) | `order_with_approval` — wait for approval or timeout |
| Error handling | Tests verify validation failures propagate correctly |
