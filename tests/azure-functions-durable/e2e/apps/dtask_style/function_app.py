# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""durabletask-native-style Durable Functions sample app for E2E testing.

Every orchestrator and entity here uses the modern durabletask authoring style:
two-argument orchestrators (``def orch(ctx, input):``) that use the durabletask
``OrchestrationContext`` API directly (``ctx.call_activity(name, input=...)``,
``task.when_all``, ``ctx.call_sub_orchestrator``, ``ctx.call_entity`` with an
``EntityInstanceId``), two-argument entity functions (``def entity(ctx, input):``),
and the durabletask client method names (``schedule_new_orchestration``,
``get_orchestration_state``, ``raise_orchestration_event``,
``terminate_orchestration``, ``purge_orchestration``, ``get_entity``,
``signal_entity``).

Together with the v1-style app it exercises both authoring surfaces the
compatibility layer supports, end-to-end against a real Functions host.
"""

import json
from typing import Any

import azure.functions as func

import azure.durable_functions as df
from durabletask import entities, task

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# ---------------------------------------------------------------------------
# Activities (dispatched by the host; single-argument input, as in Functions)
# ---------------------------------------------------------------------------

@app.activity_trigger(input_name="name")
def say_hello(name: str) -> str:
    return f"Hello {name}!"


@app.activity_trigger(input_name="n")
def square(n: int) -> int:
    return n * n


# ---------------------------------------------------------------------------
# Entity (durabletask native style: two arguments)
# ---------------------------------------------------------------------------

@app.entity_trigger(context_name="context")
def counter(ctx: entities.EntityContext, input: Any = None) -> Any:
    if ctx.operation == "add":
        new_state = ctx.get_state(int, 0) + (input or 0)
        ctx.set_state(new_state)
        return new_state
    if ctx.operation == "reset":
        ctx.set_state(0)
        return 0
    return ctx.get_state(int, 0)


# ---------------------------------------------------------------------------
# Orchestrators (durabletask native style: two arguments)
# ---------------------------------------------------------------------------

@app.orchestration_trigger(context_name="context")
def activity_chain(ctx: task.OrchestrationContext, _: Any):
    first = yield ctx.call_activity("say_hello", input="Tokyo")
    second = yield ctx.call_activity("say_hello", input="Seattle")
    third = yield ctx.call_activity("say_hello", input="London")
    return [first, second, third]


@app.orchestration_trigger(context_name="context")
def fan_out_fan_in(ctx: task.OrchestrationContext, count: Any):
    count = count or 5
    tasks = [ctx.call_activity("square", input=i) for i in range(1, count + 1)]
    results = yield task.when_all(tasks)
    return sum(results)


@app.orchestration_trigger(context_name="context")
def sub_orchestration_parent(ctx: task.OrchestrationContext, _: Any):
    child_result = yield ctx.call_sub_orchestrator("activity_chain")
    return {"from_child": child_result}


@app.orchestration_trigger(context_name="context")
def wait_for_approval(ctx: task.OrchestrationContext, _: Any):
    ctx.set_custom_status("waiting")
    approved = yield ctx.wait_for_external_event("approval")
    ctx.set_custom_status("received")
    return {"approved": approved}


@app.orchestration_trigger(context_name="context")
def counter_orchestration(ctx: task.OrchestrationContext, _: Any):
    entity_id = entities.EntityInstanceId("counter", ctx.instance_id)
    yield ctx.call_entity(entity_id, "add", 5)
    yield ctx.call_entity(entity_id, "add", 3)
    total = yield ctx.call_entity(entity_id, "get")
    return total


@app.orchestration_trigger(context_name="context")
def continue_as_new_counter(ctx: task.OrchestrationContext, value: Any):
    value = (value or 0) + 1
    if value < 5:
        ctx.continue_as_new(value)
        return value
    return value


# ---------------------------------------------------------------------------
# HTTP routes: starter + client management surface (durabletask method names)
# ---------------------------------------------------------------------------

def _state_to_json(state: Any) -> dict[str, Any]:
    if state is None:
        return {"runtimeStatus": None}
    output = None
    if state.serialized_output is not None:
        try:
            output = json.loads(state.serialized_output)
        except (TypeError, ValueError):
            output = state.serialized_output
    custom_status = None
    if state.serialized_custom_status is not None:
        try:
            custom_status = json.loads(state.serialized_custom_status)
        except (TypeError, ValueError):
            custom_status = state.serialized_custom_status
    return {
        "instanceId": state.instance_id,
        "name": state.name,
        "runtimeStatus": state.runtime_status.name,
        "output": output,
        "customStatus": custom_status,
    }


@app.route(route="ping", methods=["GET"])
def ping(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("pong")


@app.route(route="start/{name}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    name = req.route_params["name"]
    body = req.get_json()
    instance_id = await client.schedule_new_orchestration(name, input=body.get("input"))
    return func.HttpResponse(
        json.dumps({"id": instance_id}), status_code=202, mimetype="application/json")


@app.route(route="status/{id}", methods=["GET"])
@app.durable_client_input(client_name="client")
async def get_orchestration_status(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    state = await client.get_orchestration_state(req.route_params["id"], fetch_payloads=True)
    return func.HttpResponse(json.dumps(_state_to_json(state)), mimetype="application/json")


@app.route(route="raise/{id}/{event}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def raise_orchestration_event(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    body = req.get_json()
    await client.raise_orchestration_event(
        req.route_params["id"], req.route_params["event"], data=body.get("data"))
    return func.HttpResponse(status_code=202)


@app.route(route="terminate/{id}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def terminate_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    await client.terminate_orchestration(req.route_params["id"], output="e2e-terminate")
    return func.HttpResponse(status_code=202)


@app.route(route="purge/{id}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def purge_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    result = await client.purge_orchestration(req.route_params["id"])
    return func.HttpResponse(
        json.dumps({"instancesDeleted": result.deleted_instance_count}),
        mimetype="application/json")


@app.route(route="entity/{name}/{key}", methods=["GET"])
@app.durable_client_input(client_name="client")
async def read_entity(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    entity_id = entities.EntityInstanceId(req.route_params["name"], req.route_params["key"])
    metadata = await client.get_entity(entity_id)
    if metadata is None:
        payload = {"exists": False, "state": None}
    else:
        state = metadata.get_typed_state() if metadata.includes_state else None
        payload = {"exists": True, "state": state}
    return func.HttpResponse(json.dumps(payload), mimetype="application/json")


@app.route(route="signal/{name}/{key}/{op}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def signal_entity(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    body = req.get_json()
    entity_id = entities.EntityInstanceId(req.route_params["name"], req.route_params["key"])
    await client.signal_entity(entity_id, req.route_params["op"], input=body.get("input"))
    return func.HttpResponse(status_code=202)
