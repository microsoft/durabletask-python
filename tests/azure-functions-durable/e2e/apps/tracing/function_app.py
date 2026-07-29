# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Minimal Durable Functions app for distributed tracing validation."""

import json
from typing import Any

import azure.functions as func
import azure.durable_functions as df
from durabletask import task
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

provider = TracerProvider(
    resource=Resource.create({"service.name": "durable-functions-python-worker"}))
provider.add_span_processor(SimpleSpanProcessor(OTLPSpanExporter()))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("issue-179-user-code")

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="ping", methods=["GET"])
def ping(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("pong")


@app.route(route="start/{name}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start_orchestration(
        req: func.HttpRequest,
        client: df.DurableFunctionsClient) -> func.HttpResponse:
    with tracer.start_as_current_span("user-starter") as span:
        instance_id = await client.schedule_new_orchestration(
            req.route_params["name"])
        span.set_attribute("test.instance_id", instance_id)
    return func.HttpResponse(
        json.dumps({"id": instance_id}),
        status_code=202,
        mimetype="application/json",
    )


@app.route(route="status/{id}", methods=["GET"])
@app.durable_client_input(client_name="client")
async def get_status(
        req: func.HttpRequest,
        client: df.DurableFunctionsClient) -> func.HttpResponse:
    state = await client.get_orchestration_state(
        req.route_params["id"], fetch_payloads=True)
    payload = {
        "runtimeStatus": state.runtime_status.name if state else None,
        "output": state.serialized_output if state else None,
    }
    return func.HttpResponse(
        json.dumps(payload),
        mimetype="application/json",
    )


@app.orchestration_trigger(context_name="context")
def correlated_orchestrator(
        ctx: task.OrchestrationContext,
        _: Any) -> str:
    with tracer.start_as_current_span(
            "user-orchestrator",
            attributes={"test.instance_id": ctx.instance_id}):
        return "done"
