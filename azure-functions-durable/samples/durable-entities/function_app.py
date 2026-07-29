# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from collections.abc import Generator
from typing import Any

import azure.functions as func
import azure.durable_functions as df
from durabletask import entities, task
from durabletask.entities import DurableEntity


app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="counters/{key}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def add_to_counter(
        req: func.HttpRequest,
        client: df.DurableFunctionsClient) -> func.HttpResponse:
    input_ = {
        "key": req.route_params["key"],
        "amount": req.get_json().get("amount", 1),
    }
    instance_id = await client.schedule_new_orchestration(
        "update_counter", input=input_)
    return client.create_check_status_response(req, instance_id)


@app.orchestration_trigger(context_name="context")
def update_counter(
        ctx: task.OrchestrationContext,
        input_: dict[str, Any]) -> Generator[task.Task[Any], Any, int]:
    counter_id = entities.EntityInstanceId("counter", input_["key"])
    yield ctx.call_entity(counter_id, "add", input_["amount"])
    value: int = yield ctx.call_entity(
        counter_id, "get", return_type=int)
    return value


@app.entity_trigger(context_name="context")
class Counter(DurableEntity):
    def add(self, input_: Any = None) -> int:
        value = self.get_state(int, 0) + (input_ or 0)
        self.set_state(value)
        return value

    def get(self, input_: Any = None) -> int:
        return self.get_state(int, 0)

    def reset(self, input_: Any = None) -> None:
        self.set_state(0)
