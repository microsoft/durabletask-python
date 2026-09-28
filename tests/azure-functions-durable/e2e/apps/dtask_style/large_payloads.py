# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Blob-backed payload round trips through the real Functions bindings."""

import json
from pathlib import Path
from typing import Any

import azure.functions as func
import azure.durable_functions as df
from durabletask import history, task
from durabletask.entities import EntityInstanceId

bp = df.Blueprint()


@bp.durable_client_input(client_name="client")
@bp.activity_trigger(input_name="payload")
def payload_echo(payload: dict, client: df.SyncDurableFunctionsClient, context: func.Context) -> dict:
    assert isinstance(client, df.SyncDurableFunctionsClient)
    assert context.thread_local_storage.invocation_id == context.invocation_id
    assert json.loads((Path(context.function_directory) / "host.json").read_text())["version"] == "2.0"
    return {"data": payload["data"], "stages": [*payload["stages"], "activity"]}


@bp.orchestration_trigger(context_name="context")
def payload_roundtrip(ctx: task.OrchestrationContext, payload: dict[str, Any]):
    first = yield ctx.call_activity("payload_echo", input=payload)
    second = yield ctx.call_activity("payload_echo_async", input=first)
    ctx.set_custom_status(second)
    return second


@bp.activity_trigger(input_name="payload")
@bp.durable_client_input(client_name="client")
async def payload_echo_async(payload: dict, client: df.DurableFunctionsClient, context: func.Context) -> dict:
    assert isinstance(client, df.DurableFunctionsClient)
    assert json.loads((Path(context.function_directory) / "host.json").read_text())["version"] == "2.0"
    return {"data": payload["data"], "stages": [*payload["stages"], "activity"]}


@bp.orchestration_trigger(context_name="context")
def payload_entity_roundtrip(ctx: task.OrchestrationContext, payload: dict[str, Any]):
    entity_id = EntityInstanceId("probe", ctx.instance_id)
    written = yield ctx.call_entity(entity_id, "set", input=payload)
    restored = yield ctx.call_entity(entity_id, "get")
    assert written == payload, f"Unexpected entity result: {str(written)[:120]}"
    assert restored == payload, f"Unexpected entity state: {str(restored)[:120]}"
    yield ctx.call_entity(entity_id, "delete")
    return restored


@bp.orchestration_trigger(context_name="context")
def payload_event_roundtrip(ctx: task.OrchestrationContext, payload: Any):
    return (yield ctx.wait_for_external_event("payload"))


@bp.orchestration_trigger(context_name="context")
def payload_continue_roundtrip(ctx: task.OrchestrationContext, payload: dict[str, Any]):
    if not payload["stages"]:
        ctx.continue_as_new({"data": payload["data"], "stages": ["continued"]})
        return
    return (yield ctx.call_sub_orchestrator("payload_roundtrip", input=payload))


@bp.route(route="payload-start-sync", methods=["POST"])
@bp.durable_client_input(client_name="client")
def payload_start_sync(
        req: func.HttpRequest, client: df.SyncDurableFunctionsClient) -> func.HttpResponse:
    instance_id = client.schedule_new_orchestration("payload_roundtrip", input=req.get_json())
    return func.HttpResponse(json.dumps({"id": instance_id}), status_code=202)


@bp.route(route="payload-status-sync/{id}", methods=["GET"])
@bp.durable_client_input(client_name="client")
def payload_status_sync(
        req: func.HttpRequest, client: df.SyncDurableFunctionsClient) -> func.HttpResponse:
    state = client.get_orchestration_state(req.route_params["id"], fetch_payloads=True)
    assert state is not None
    return func.HttpResponse(json.dumps({
        "input": json.loads(state.serialized_input or "null"),
        "output": json.loads(state.serialized_output or "null"),
    }), mimetype="application/json")


def _entity_history_response(events: list[history.HistoryEvent]) -> func.HttpResponse:
    inputs = [json.loads(event.input or "null") for event in events
              if isinstance(event, (history.EventSentEvent, history.EventRaisedEvent))]
    return func.HttpResponse(json.dumps(inputs), mimetype="application/json")


@bp.route(route="payload-history-sync/{id}", methods=["GET"])
@bp.durable_client_input(client_name="client")
def payload_history_sync(
        req: func.HttpRequest, client: df.SyncDurableFunctionsClient) -> func.HttpResponse:
    return _entity_history_response(client.get_orchestration_history(req.route_params["id"]))


@bp.route(route="payload-history-async/{id}", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def payload_history_async(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    return _entity_history_response(await client.get_orchestration_history(req.route_params["id"]))
