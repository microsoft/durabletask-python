# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""V1-style Durable Functions sample app for end-to-end testing.

Every orchestrator and entity here uses the classic ``azure-functions-durable``
v1 authoring style: single-argument generator orchestrators
(``def orch(context):``), single-argument entity functions
(``def entity(context):``), and the deprecated v1 client method names
(``start_new``, ``get_status``, ``raise_event``, ``terminate``,
``purge_instance_history``, ``read_entity_state``, ``signal_entity``).

The app is driven by the E2E suite through its HTTP routes. It is intentionally
broad, covering activity chaining, fan-out/fan-in, sub-orchestrations, external
events + timers, entities, custom status, continue-as-new, durable HTTP, and the
client management surface.
"""

import json

import azure.functions as func

import azure.durable_functions as df

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------

@app.activity_trigger(input_name="name")
def say_hello(name: str) -> str:
    return f"Hello {name}!"


@app.activity_trigger(input_name="n")
def square(n: int) -> int:
    return n * n


# ---------------------------------------------------------------------------
# Entity (v1 style: single context argument)
# ---------------------------------------------------------------------------

@app.entity_trigger(context_name="context")
def counter(context: df.DurableEntityContext) -> None:
    current = context.get_state(initializer=lambda: 0)
    operation = context.operation_name
    if operation == "add":
        current += context.get_input()
        context.set_state(current)
    elif operation == "reset":
        current = 0
        context.set_state(current)
    context.set_result(current)


# ---------------------------------------------------------------------------
# Orchestrators (v1 style: single context argument, generator)
# ---------------------------------------------------------------------------

@app.orchestration_trigger(context_name="context")
def activity_chain(context: df.DurableOrchestrationContext):
    first = yield context.call_activity("say_hello", "Tokyo")
    second = yield context.call_activity("say_hello", "Seattle")
    third = yield context.call_activity("say_hello", "London")
    return [first, second, third]


@app.orchestration_trigger(context_name="context")
def fan_out_fan_in(context: df.DurableOrchestrationContext):
    count = context.get_input() or 5
    tasks = [context.call_activity("square", i) for i in range(1, count + 1)]
    results = yield context.task_all(tasks)
    return sum(results)


@app.orchestration_trigger(context_name="context")
def sub_orchestration_parent(context: df.DurableOrchestrationContext):
    child_result = yield context.call_sub_orchestrator("activity_chain")
    return {"from_child": child_result}


@app.orchestration_trigger(context_name="context")
def wait_for_approval(context: df.DurableOrchestrationContext):
    context.set_custom_status("waiting")
    approved = yield context.wait_for_external_event("approval")
    context.set_custom_status("received")
    return {"approved": approved}


@app.orchestration_trigger(context_name="context")
def counter_orchestration(context: df.DurableOrchestrationContext):
    entity_id = df.EntityId("counter", context.instance_id)
    yield context.call_entity(entity_id, "add", 5)
    yield context.call_entity(entity_id, "add", 3)
    total = yield context.call_entity(entity_id, "get")
    return total


@app.orchestration_trigger(context_name="context")
def continue_as_new_counter(context: df.DurableOrchestrationContext):
    # Count up to 5 across continue-as-new generations, then stop.
    value = context.get_input() or 0
    value += 1
    if value < 5:
        context.continue_as_new(value)
        return value
    return value


@app.orchestration_trigger(context_name="context")
def http_call(context: df.DurableOrchestrationContext):
    url = context.get_input()
    response = yield context.call_http("GET", url)
    return {"status_code": response.status_code, "content": response.content}


# ---------------------------------------------------------------------------
# HTTP routes: starter + client management surface
# ---------------------------------------------------------------------------

@app.route(route="ping", methods=["GET"])
def ping(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("pong")


@app.route(route="start/{name}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    name = req.route_params["name"]
    body = req.get_json()
    instance_id = await client.start_new(name, client_input=body.get("input"))
    return client.create_check_status_response(req, instance_id)


@app.route(route="status/{id}", methods=["GET"])
@app.durable_client_input(client_name="client")
async def get_orchestration_status(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    status = await client.get_status(req.route_params["id"], show_input=True)
    return func.HttpResponse(json.dumps(status.to_json()), mimetype="application/json")


@app.route(route="raise/{id}/{event}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def raise_orchestration_event(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    body = req.get_json()
    await client.raise_event(
        req.route_params["id"], req.route_params["event"], event_data=body.get("data"))
    return func.HttpResponse(status_code=202)


@app.route(route="terminate/{id}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def terminate_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    await client.terminate(req.route_params["id"], reason="e2e-terminate")
    return func.HttpResponse(status_code=202)


@app.route(route="purge/{id}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def purge_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    result = await client.purge_instance_history(req.route_params["id"])
    return func.HttpResponse(
        json.dumps({"instancesDeleted": result.instances_deleted}),
        mimetype="application/json")


@app.route(route="entity/{name}/{key}", methods=["GET"])
@app.durable_client_input(client_name="client")
async def read_entity(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    entity_id = df.EntityId(req.route_params["name"], req.route_params["key"])
    state = await client.read_entity_state(entity_id)
    return func.HttpResponse(
        json.dumps({"exists": state.entity_exists, "state": state.entity_state}),
        mimetype="application/json")


@app.route(route="signal/{name}/{key}/{op}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def signal_entity(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    body = req.get_json()
    entity_id = df.EntityId(req.route_params["name"], req.route_params["key"])
    await client.signal_entity(entity_id, req.route_params["op"], input=body.get("input"))
    return func.HttpResponse(status_code=202)
