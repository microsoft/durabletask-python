# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from collections.abc import Generator
from typing import Any

import azure.functions as func
import azure.durable_functions as df
from durabletask import task


app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="fan-out-fan-in", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start_fan_out_fan_in(
        req: func.HttpRequest,
        client: df.DurableFunctionsClient) -> func.HttpResponse:
    numbers = req.get_json()
    instance_id = await client.schedule_new_orchestration(
        "fan_out_fan_in", input=numbers)
    return client.create_check_status_response(req, instance_id)


@app.orchestration_trigger(context_name="context")
def fan_out_fan_in(
        ctx: task.OrchestrationContext,
        numbers: list[int]) -> Generator[task.Task[Any], Any, int]:
    activities: list[task.Task[int]] = [
        ctx.call_activity("square", input=number, return_type=int)
        for number in numbers
    ]
    squared_numbers: list[int] = yield task.when_all(activities)
    return sum(squared_numbers)


@app.activity_trigger(input_name="number")
def square(number: int) -> int:
    return number * number
