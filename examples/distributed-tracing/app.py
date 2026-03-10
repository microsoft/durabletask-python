# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Distributed tracing example using OpenTelemetry and Jaeger.

This example demonstrates how to configure OpenTelemetry distributed tracing
with the Durable Task Python SDK. The orchestration showcases timers,
activities, and a sub-orchestration, all producing correlated trace spans
visible in the Jaeger UI.

Prerequisites:
  - DTS emulator running on localhost:8080
  - Jaeger running on localhost:4317 (OTLP gRPC) / localhost:16686 (UI)
  - pip install -r requirements.txt
"""

import os
import time
from datetime import timedelta

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from azure.identity import DefaultAzureCredential

from durabletask import client, task
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker


# ---------------------------------------------------------------------------
# OpenTelemetry configuration — MUST be done before any spans are created
# ---------------------------------------------------------------------------

OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

resource = Resource.create({"service.name": "durabletask-tracing-example"})
provider = TracerProvider(resource=resource)
provider.add_span_processor(
    BatchSpanProcessor(
        OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True)
    )
)
trace.set_tracer_provider(provider)


# ---------------------------------------------------------------------------
# Activity functions
# ---------------------------------------------------------------------------

def get_weather(ctx: task.ActivityContext, city: str) -> str:
    """Simulate fetching weather data for a city."""
    # In a real app this would call an external API
    weather_data = {
        "Tokyo": "Sunny, 22°C",
        "Seattle": "Rainy, 12°C",
        "London": "Cloudy, 15°C",
    }
    result = weather_data.get(city, "Unknown")
    print(f"  [Activity] get_weather({city}) -> {result}")
    return result


def summarize(ctx: task.ActivityContext, reports: list) -> str:
    """Combine individual weather reports into a summary string."""
    summary = " | ".join(reports)
    print(f"  [Activity] summarize -> {summary}")
    return summary


# ---------------------------------------------------------------------------
# Sub-orchestration
# ---------------------------------------------------------------------------

def collect_weather(ctx: task.OrchestrationContext, cities: list):
    """Sub-orchestration that collects weather for a list of cities."""
    results = []
    for city in cities:
        weather = yield ctx.call_activity(get_weather, input=city)
        results.append(f"{city}: {weather}")
    return results


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def weather_report_orchestrator(ctx: task.OrchestrationContext, cities: list):
    """Top-level orchestration demonstrating timers, activities, and sub-orchestrations.

    Flow:
      1. Wait for a short timer (simulating a scheduled delay).
      2. Call a sub-orchestration to collect weather data for each city.
      3. Call an activity to summarize the results.
    """
    # Step 1 — Timer: wait briefly before starting work
    yield ctx.create_timer(timedelta(milliseconds=100))
    if not ctx.is_replaying:
        print("  [Orchestrator] Timer fired — starting weather collection")

    # Step 2 — Sub-orchestration: delegate city-level work
    reports = yield ctx.call_sub_orchestrator(collect_weather, input=cities)

    # Step 3 — Activity: summarize the collected reports
    summary = yield ctx.call_activity(summarize, input=reports)

    return summary


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Use environment variables if provided, otherwise use default emulator values
    taskhub_name = os.getenv("TASKHUB", "default")
    endpoint = os.getenv("ENDPOINT", "http://localhost:8080")

    print(f"Using taskhub: {taskhub_name}")
    print(f"Using endpoint: {endpoint}")
    print(f"OTLP endpoint: {OTEL_ENDPOINT}")

    # Set credential to None for emulator, or DefaultAzureCredential for Azure
    secure_channel = endpoint.startswith("https://")
    credential = DefaultAzureCredential() if secure_channel else None

    with DurableTaskSchedulerWorker(
        host_address=endpoint,
        secure_channel=secure_channel,
        taskhub=taskhub_name,
        token_credential=credential,
    ) as w:
        # Register orchestrators and activities
        w.add_orchestrator(weather_report_orchestrator)
        w.add_orchestrator(collect_weather)
        w.add_activity(get_weather)
        w.add_activity(summarize)
        w.start()
        print("Worker started.")

        # Create client, schedule the orchestration, and wait for completion
        c = DurableTaskSchedulerClient(
            host_address=endpoint,
            secure_channel=secure_channel,
            taskhub=taskhub_name,
            token_credential=credential,
        )

        cities = ["Tokyo", "Seattle", "London"]
        instance_id = c.schedule_new_orchestration(
            weather_report_orchestrator, input=cities,
        )
        print(f"Orchestration started: {instance_id}")

        state = c.wait_for_orchestration_completion(instance_id, timeout=60)
        if state and state.runtime_status == client.OrchestrationStatus.COMPLETED:
            print(f"Orchestration completed! Result: {state.serialized_output}")
        elif state:
            print(f"Orchestration failed: {state.failure_details}")

        # Flush any remaining spans to the exporter
        provider.force_flush()
        time.sleep(1)

    print("Done. Open Jaeger at http://localhost:16686 to view traces.")
