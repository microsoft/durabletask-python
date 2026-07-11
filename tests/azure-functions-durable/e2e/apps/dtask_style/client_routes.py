# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""HTTP routes for the durabletask-native-style sample app (blueprint).

Exposes the starter and the durabletask client management surface using the
native method names: ``schedule_new_orchestration``,
``get_orchestration_state``, ``get_all_orchestration_states``,
``get_orchestration_history``, ``raise_orchestration_event``,
``terminate_orchestration``, ``suspend_orchestration``, ``resume_orchestration``,
``restart_orchestration``, ``purge_orchestration``,
``wait_for_orchestration_start``, ``wait_for_orchestration_completion``,
``get_entity``, and ``signal_entity``.
"""

import json
from typing import Any

import azure.functions as func

import azure.durable_functions as df
from durabletask import entities

bp = df.Blueprint()


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


@bp.route(route="ping", methods=["GET"])
def ping(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("pong")


@bp.route(route="start/{name}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def start_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    name = req.route_params["name"]
    body = req.get_json()
    instance_id = await client.schedule_new_orchestration(name, input=body.get("input"))
    return func.HttpResponse(
        json.dumps({"id": instance_id}), status_code=202, mimetype="application/json")


@bp.route(route="status/{id}", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def get_orchestration_status(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    state = await client.get_orchestration_state(req.route_params["id"], fetch_payloads=True)
    return func.HttpResponse(json.dumps(_state_to_json(state)), mimetype="application/json")


@bp.route(route="states", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def get_all_states(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    states = await client.get_all_orchestration_states()
    ids = [s.instance_id for s in states]
    return func.HttpResponse(json.dumps({"ids": ids}), mimetype="application/json")


@bp.route(route="history/{id}", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def get_history(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    events = await client.get_orchestration_history(req.route_params["id"])
    return func.HttpResponse(
        json.dumps({"eventCount": len(events)}), mimetype="application/json")


@bp.route(route="wait_start/{id}", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def wait_start(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    state = await client.wait_for_orchestration_start(req.route_params["id"], timeout=30)
    return func.HttpResponse(json.dumps(_state_to_json(state)), mimetype="application/json")


@bp.route(route="wait_complete/{id}", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def wait_complete(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    state = await client.wait_for_orchestration_completion(req.route_params["id"], timeout=30)
    return func.HttpResponse(json.dumps(_state_to_json(state)), mimetype="application/json")


@bp.route(route="raise/{id}/{event}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def raise_orchestration_event(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    body = req.get_json()
    await client.raise_orchestration_event(
        req.route_params["id"], req.route_params["event"], data=body.get("data"))
    return func.HttpResponse(status_code=202)


@bp.route(route="terminate/{id}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def terminate_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    await client.terminate_orchestration(req.route_params["id"], output="e2e-terminate")
    return func.HttpResponse(status_code=202)


@bp.route(route="suspend/{id}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def suspend_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    await client.suspend_orchestration(req.route_params["id"])
    return func.HttpResponse(status_code=202)


@bp.route(route="resume/{id}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def resume_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    await client.resume_orchestration(req.route_params["id"])
    return func.HttpResponse(status_code=202)


@bp.route(route="restart/{id}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def restart_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    new_id = await client.restart_orchestration(req.route_params["id"])
    return func.HttpResponse(
        json.dumps({"id": new_id}), status_code=202, mimetype="application/json")


@bp.route(route="purge/{id}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def purge_orchestration(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    result = await client.purge_orchestration(req.route_params["id"])
    return func.HttpResponse(
        json.dumps({"instancesDeleted": result.deleted_instance_count}),
        mimetype="application/json")


@bp.route(route="entity/{name}/{key}", methods=["GET"])
@bp.durable_client_input(client_name="client")
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


@bp.route(route="signal/{name}/{key}/{op}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def signal_entity(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    body = req.get_json()
    entity_id = entities.EntityInstanceId(req.route_params["name"], req.route_params["key"])
    await client.signal_entity(entity_id, req.route_params["op"], input=body.get("input"))
    return func.HttpResponse(status_code=202)
