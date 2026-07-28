# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from collections.abc import Generator
from typing import Any

import azure.functions as func
import azure.durable_functions as df
from durabletask import task


app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="orchestrators/{function_name}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start_orchestration(
        req: func.HttpRequest,
        client: df.DurableFunctionsClient) -> func.HttpResponse:
    instance_id = await client.schedule_new_orchestration(
        req.route_params["function_name"])
    return client.create_check_status_response(req, instance_id)


@app.orchestration_trigger(context_name="context")
def hello_cities(
        ctx: task.OrchestrationContext,
        _: Any) -> Generator[task.Task[Any], Any, list[str]]:
    result1: str = yield ctx.call_activity(
        "say_hello", input="Tokyo", return_type=str)
    result2: str = yield ctx.call_activity(
        "say_hello", input="Seattle", return_type=str)
    result3: str = yield ctx.call_activity(
        "say_hello", input="London", return_type=str)
    return [result1, result2, result3]


@app.activity_trigger(input_name="city")
def say_hello(city: str) -> str:
    return f"Hello {city}!"
