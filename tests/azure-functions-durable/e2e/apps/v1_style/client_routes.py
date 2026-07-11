# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""HTTP routes for the V1-style sample app (blueprint).

Exposes the app's control plane: a ``ping`` health check, helper endpoints used
by the durable-HTTP tests (``fail`` / ``echo``), the orchestration starter, and
the full deprecated v1 ``DurableOrchestrationClient`` management surface
(``start_new``, ``get_status``, ``get_status_all``, ``get_status_by``,
``raise_event``, ``terminate``, ``suspend``, ``resume``, ``restart``,
``purge_instance_history``, ``purge_instance_history_by``, ``read_entity_state``,
``signal_entity``, ``create_http_management_payload``,
``get_client_response_links``,
``wait_for_completion_or_create_check_status_response``, and the ``rewind`` stub).
"""

import json
from datetime import datetime

import azure.functions as func

import azure.durable_functions as df

bp = df.Blueprint()


# ---------------------------------------------------------------------------
# Health + durable-HTTP helper endpoints
# ---------------------------------------------------------------------------

@bp.route(route="ping", methods=["GET"])
def ping(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("pong")


@bp.route(route="fail", methods=["GET"])
def fail(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("nope", status_code=500)


@bp.route(route="echo", methods=["POST"])
def echo(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(req.get_body(), mimetype="application/json")


# ---------------------------------------------------------------------------
# Starter + status
# ---------------------------------------------------------------------------

@bp.route(route="start/{name}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def start_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    name = req.route_params["name"]
    body = req.get_json()
    instance_id = await client.start_new(name, client_input=body.get("input"))
    return client.create_check_status_response(req, instance_id)


@bp.route(route="status/{id}", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def get_orchestration_status(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    status = await client.get_status(req.route_params["id"], show_input=True)
    return func.HttpResponse(json.dumps(status.to_json()), mimetype="application/json")


@bp.route(route="status_all", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def get_status_all(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    statuses = await client.get_status_all()
    ids = [s.instance_id for s in statuses if s]
    return func.HttpResponse(json.dumps({"ids": ids}), mimetype="application/json")


@bp.route(route="status_by/{runtime_status}", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def get_status_by(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    runtime_status = df.OrchestrationRuntimeStatus(req.route_params["runtime_status"])
    statuses = await client.get_status_by(runtime_status=[runtime_status])
    ids = [s.instance_id for s in statuses if s]
    return func.HttpResponse(json.dumps({"ids": ids}), mimetype="application/json")


# ---------------------------------------------------------------------------
# Lifecycle: events, terminate, suspend/resume, restart, purge
# ---------------------------------------------------------------------------

@bp.route(route="raise/{id}/{event}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def raise_orchestration_event(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    body = req.get_json()
    await client.raise_event(
        req.route_params["id"], req.route_params["event"], event_data=body.get("data"))
    return func.HttpResponse(status_code=202)


@bp.route(route="terminate/{id}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def terminate_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    await client.terminate(req.route_params["id"], reason="e2e-terminate")
    return func.HttpResponse(status_code=202)


@bp.route(route="suspend/{id}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def suspend_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    await client.suspend(req.route_params["id"], reason="e2e-suspend")
    return func.HttpResponse(status_code=202)


@bp.route(route="resume/{id}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def resume_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    await client.resume(req.route_params["id"], reason="e2e-resume")
    return func.HttpResponse(status_code=202)


@bp.route(route="restart/{id}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def restart_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    new_id = await client.restart(req.route_params["id"])
    return func.HttpResponse(
        json.dumps({"id": new_id}), status_code=202, mimetype="application/json")


@bp.route(route="purge/{id}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def purge_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    result = await client.purge_instance_history(req.route_params["id"])
    return func.HttpResponse(
        json.dumps({"instancesDeleted": result.instances_deleted}),
        mimetype="application/json")


@bp.route(route="purge_by", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def purge_orchestration_by(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    body = req.get_json()
    created_time_from = datetime.fromisoformat(body["from"]) if body.get("from") else None
    runtime_status = [df.OrchestrationRuntimeStatus(body["runtimeStatus"])]
    result = await client.purge_instance_history_by(
        created_time_from=created_time_from, runtime_status=runtime_status)
    return func.HttpResponse(
        json.dumps({"instancesDeleted": result.instances_deleted}),
        mimetype="application/json")


# ---------------------------------------------------------------------------
# Management payload + wait-or-check + rewind
# ---------------------------------------------------------------------------

@bp.route(route="mgmt_payload/{id}", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def management_payload(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    instance_id = req.route_params["id"]
    payload = client.create_http_management_payload(req, instance_id)
    links = client.get_client_response_links(req, instance_id)
    return func.HttpResponse(
        json.dumps({"payload": dict(payload), "links": dict(links)}),
        mimetype="application/json")


@bp.route(route="wait_or_check/{id}", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def wait_or_check(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    return await client.wait_for_completion_or_create_check_status_response(
        req, req.route_params["id"], timeout_in_milliseconds=15000)


@bp.route(route="rewind/{id}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def rewind_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    await client.rewind(req.route_params["id"], reason="e2e-rewind")
    return func.HttpResponse(status_code=202)


# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------

@bp.route(route="entity/{name}/{key}", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def read_entity(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    entity_id = df.EntityId(req.route_params["name"], req.route_params["key"])
    state = await client.read_entity_state(entity_id)
    return func.HttpResponse(
        json.dumps({"exists": state.entity_exists, "state": state.entity_state}),
        mimetype="application/json")


@bp.route(route="signal/{name}/{key}/{op}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def signal_entity(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    body = req.get_json()
    entity_id = df.EntityId(req.route_params["name"], req.route_params["key"])
    await client.signal_entity(entity_id, req.route_params["op"], input=body.get("input"))
    return func.HttpResponse(status_code=202)
