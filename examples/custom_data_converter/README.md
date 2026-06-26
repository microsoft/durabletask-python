# Custom DataConverter Example (pydantic)

This example shows how to plug a **third-party serialization library** into the
Durable Task Python SDK by implementing a custom
[`DataConverter`](../../durabletask/serialization.py). It uses
[pydantic](https://docs.pydantic.dev/) to serialize and *validate* payloads, and
proves the integration works end-to-end with in-process tests that need no
sidecar, emulator, or Azure resources.

You can copy this entire folder into a new directory and run it as a standalone
project.

## Why a custom converter?

By default the SDK serializes payloads with its built-in JSON codec
(`JsonDataConverter`), which understands builtins, dataclasses, and objects that
expose `to_json()` / `from_json()` hooks. It does **not** know about pydantic
models. Supplying a custom `DataConverter` lets you route serialization through
any library you like — pydantic, `attrs`, `marshmallow`, a schema registry, an
encryption layer, etc. — at every payload boundary the SDK touches.

## How the seam works

Both the worker and the client accept a `data_converter` argument, and the SDK
routes **every** payload boundary through it — orchestrator / activity / entity
inputs and outputs, external events, and custom status:

```python
converter = PydanticDataConverter()

worker = DurableTaskSchedulerWorker(..., data_converter=converter)
client = DurableTaskSchedulerClient(..., data_converter=converter)
```

> [!IMPORTANT]
> Pass an equivalent converter to **both** the worker and the client. A payload
> serialized by one side is reconstructed by the other, so they must agree on
> the format.

A `DataConverter` implements three methods:

| Method | Direction | Used when |
|---|---|---|
| `serialize(value)` | Python value → JSON string | Any value leaves the process |
| `deserialize(data, target_type)` | JSON string → Python value (optionally typed) | A value arrives and the SDK knows the target type (from a function annotation, `return_type=`, or a typed client accessor) |
| `coerce(value, target_type)` | already-parsed value → typed value | The SDK already holds a parsed value (e.g. entity state) |

The converter in [src/converter.py](src/converter.py) recognizes
`pydantic.BaseModel` subclasses and uses pydantic for them, **delegating
everything else** to the default `JsonDataConverter`. This "handle my types,
delegate the rest" shape is the recommended pattern for a real converter — it
costs nothing for non-pydantic payloads.

## Inbound inputs: `can_reconstruct`

There is one extra detail for reconstructing **inbound** orchestrator/activity
*inputs*. Before the SDK hands an input to your converter, it asks the converter
whether the function's annotated input type is something it can rebuild, via
`DataConverter.can_reconstruct(target_type)`. The default implementation
recognizes dataclasses and `from_json()`-capable types (and `Optional` / `list`
wrappers) — it does **not** know about pydantic models, so without an override an
input annotated `order: Order` would arrive as a plain `dict`.

The converter overrides `can_reconstruct` to also recognize
`pydantic.BaseModel` subclasses, deferring everything else to the same
`JsonDataConverter` fallback it uses for serialization:

```python
def can_reconstruct(self, target_type):
    if _is_model_type(target_type):
        return True
    return self._fallback.can_reconstruct(target_type)  # dataclasses, from_json, ...
```

The base `DataConverter.can_reconstruct` is conservative — it returns `False`,
so a converter only claims the types it actually rebuilds. Outbound values,
`return_type=` arguments, and typed client accessors (`state.get_output(Receipt)`)
don't depend on this hook — they pass the type to the converter directly.



## Folder structure

```text
custom_data_converter/
├── README.md
├── requirements.txt
├── src/
│   ├── __init__.py
│   ├── converter.py    # PydanticDataConverter — the integration point
│   ├── workflows.py    # pydantic models + orchestrator/activities
│   └── app.py          # runs against a real DTS backend / emulator
└── test/
    ├── __init__.py
    └── test_custom_converter.py  # in-process proof using the in-memory backend
```

## What the example proves

The models in [src/workflows.py](src/workflows.py) are plain
`pydantic.BaseModel` subclasses — *not* dataclasses — so they only round-trip
correctly **because** of the custom converter. The tests in
[test/test_custom_converter.py](test/test_custom_converter.py) verify:

1. A pydantic `Order` passed as orchestration input arrives at the
   orchestrator/activity as a **validated model instance** (attribute access),
   not a raw dict.
2. The orchestration's pydantic `Receipt` result is reconstructed, typed, on
   the client via `state.get_output(Receipt)`.
3. The wire payload is genuine pydantic JSON (`model_dump_json`), confirming the
   custom converter — not the default codec — handled it.
4. An input that violates a pydantic constraint fails the orchestration with a
   validation error, instead of passing bad data through.
5. For contrast, the default `JsonDataConverter` **cannot serialize** a pydantic
   model at all (it raises `TypeError`) — which is exactly what motivates the
   custom converter.

## Getting started

1. Copy this folder to a new location and `cd` into it:

   ```bash
   cd custom_data_converter
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

## Running the tests (no backend required)

From the `custom_data_converter/` directory:

```bash
pytest test/
```

This is the self-contained proof: it runs the full orchestration in-process
against the in-memory backend.

## Running the app against the emulator

Start the [DTS emulator](../README.md#running-with-the-emulator), then from the
`custom_data_converter/` directory:

```bash
python -m src.app
```
