# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from collections.abc import Generator
from datetime import timedelta
from typing import Any

import azure.functions as func
import azure.durable_functions as df
from durabletask import task


app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="approvals", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start_approval(
        req: func.HttpRequest,
        client: df.DurableFunctionsClient) -> func.HttpResponse:
    instance_id = await client.schedule_new_orchestration("wait_for_approval")
    return client.create_check_status_response(req, instance_id)


@app.route(route="approvals/{instance_id}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def submit_approval(
        req: func.HttpRequest,
        client: df.DurableFunctionsClient) -> func.HttpResponse:
    await client.raise_orchestration_event(
        req.route_params["instance_id"],
        "approval",
        data=req.get_json())
    return func.HttpResponse(status_code=202)


@app.orchestration_trigger(context_name="context")
def wait_for_approval(
        ctx: task.OrchestrationContext,
        _: Any) -> Generator[task.Task[Any], Any, Any]:
    approval = ctx.wait_for_external_event("approval")
    timeout = ctx.create_timer(
        ctx.current_utc_datetime + timedelta(minutes=1))

    winner: task.Task[Any] = yield task.when_any([approval, timeout])
    if winner is approval:
        timeout.cancel()
        return approval.get_result()

    return {"approved": False, "reason": "timed out"}
