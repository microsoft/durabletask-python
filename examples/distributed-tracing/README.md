# Distributed Tracing Example

This example demonstrates how to set up **distributed tracing** with the
Durable Task Python SDK using [OpenTelemetry](https://opentelemetry.io/)
and [Jaeger](https://www.jaegertracing.io/) as the trace backend.

The sample orchestration showcases three key Durable Task features that
all produce correlated trace spans:

1. **Timers** — a short delay before starting work.
1. **Sub-orchestration** — delegates city-level weather collection to a
   child orchestration.
1. **Activities** — individual activity calls to fetch weather data and
   produce a summary.

## Prerequisites

- [Docker](https://www.docker.com/) (for the emulator and Jaeger)
- Python 3.10+

## Quick Start

### 1. Start the DTS Emulator

```bash
docker run --name dtsemulator -d -p 8080:8080 mcr.microsoft.com/dts/dts-emulator:latest
```

### 2. Start Jaeger

Jaeger's all-in-one image accepts OTLP over gRPC on port **4317** and
serves the UI on port **16686**:

```bash
docker run --name jaeger -d \
  -p 4317:4317 \
  -p 16686:16686 \
  jaegertracing/all-in-one:latest
```

PowerShell:

```powershell
docker run --name jaeger -d `
  -p 4317:4317 `
  -p 16686:16686 `
  jaegertracing/all-in-one:latest
```

### 3. Install Dependencies

Create and activate a virtual environment, then install the required
packages:

```bash
python -m venv .venv
```

Bash:

```bash
source .venv/bin/activate
```

PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Install requirements:

```bash
pip install -r requirements.txt
```

If you are running from a local clone of the repository, install the
local packages in editable mode instead (run from the repo root):

```bash
pip install -e ".[opentelemetry]" -e ./durabletask-azuremanaged
```

### 4. Run the Example

```bash
python app.py
```

Once the orchestration completes, open the Jaeger UI at
<http://localhost:16686>, select the **durabletask-tracing-example**
service, and click **Find Traces** to explore the spans.

## What You Will See in Jaeger

A single trace for the orchestration will contain spans for:

- **`orchestration:weather_report_orchestrator`** — the top-level
  orchestration span.
- **`timer`** — the 2-second timer delay.
- **`orchestration:collect_weather`** — the sub-orchestration span.
- **`activity:get_weather`** — one span per city
  (Tokyo, Seattle, London).
- **`activity:summarize`** — the final summarization activity.

All spans share the same trace ID, so you can follow the full execution
flow from the parent orchestration through the sub-orchestration and
into each activity.

## Configuration

The example reads the following environment variables (all optional):

| Variable | Default | Description |
|---|---|---|
| `ENDPOINT` | `http://localhost:8080` | DTS emulator / scheduler endpoint |
| `TASKHUB` | `default` | Task hub name |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP gRPC endpoint (Jaeger) |

## Important Usage Guidelines for Distributed Tracing

### Install the OpenTelemetry extras

The SDK ships OpenTelemetry as an **optional** dependency. Install it
with the `opentelemetry` extra:

```bash
pip install "durabletask[opentelemetry]"
```

Without these packages the SDK still works, but no trace spans are
emitted.

### Configure the `TracerProvider` before starting the worker

OpenTelemetry requires a configured `TracerProvider` with at least one
`SpanProcessor` and exporter **before** any spans are created. In
practice this means setting it up at the top of your entry-point module,
before constructing the worker or client:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

resource = Resource.create({"service.name": "my-app"})
provider = TracerProvider(resource=resource)
provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True))
)
trace.set_tracer_provider(provider)
```

### Flush spans before exiting

The `BatchSpanProcessor` buffers spans and exports them in the
background. If the process exits before the buffer is flushed, some
spans may be lost. Call `provider.force_flush()` (and optionally add a
short sleep) before your program terminates:

```python
provider.force_flush()
```

### Orchestrator code must remain deterministic

Distributed tracing does **not** change the determinism requirement for
orchestrator functions. Do not create your own OpenTelemetry spans
inside orchestrator code — the SDK handles span creation automatically.
Activity functions and client code are free to create additional spans
as needed.

### Use `BatchSpanProcessor` in production

`SimpleSpanProcessor` exports every span synchronously, which adds
latency to every operation. Use `BatchSpanProcessor` for production
workloads to avoid performance overhead.

### Choose the right exporter for your backend

This example uses the OTLP/gRPC exporter, which is compatible with
Jaeger 1.35+, the OpenTelemetry Collector, Azure Monitor (via the
Azure Monitor OpenTelemetry exporter), and many other backends. Swap
the exporter if your tracing backend uses a different protocol.
