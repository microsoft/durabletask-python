# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json

import azure.durable_functions as df
from azure.durable_functions import DurableFunctionsClient
from azure.durable_functions.constants import (
    ACTIVITY_TRIGGER,
    DURABLE_CLIENT,
    ENTITY_TRIGGER,
    ORCHESTRATION_TRIGGER,
)


_CLIENT_CONFIG = json.dumps({
    "taskHubName": "TestHub",
    "requiredQueryStringParameters": "code=xyz",
    "baseUrl": "http://localhost:7071/runtime/webhooks/durabletask",
    "rpcBaseUrl": "http://localhost:8080/",
})


def _trigger(fb):
    return fb._function.get_trigger()


# ---------------------------------------------------------------------------
# orchestration_trigger
# ---------------------------------------------------------------------------

def test_orchestration_trigger_v1_signature():
    app = df.DFApp()

    def my_orchestrator(context):
        return 1

    fb = app.orchestration_trigger(
        context_name="context", orchestration="MyOrchestrator")(my_orchestrator)
    trigger = _trigger(fb)
    assert trigger.get_binding_name() == ORCHESTRATION_TRIGGER
    assert trigger.name == "context"
    assert trigger.orchestration == "MyOrchestrator"


def test_orchestration_trigger_accepts_input_type():
    app = df.DFApp()

    def my_orchestrator(context):
        return 1

    # v1 parity: the input_type keyword must be accepted and stashed.
    fb = app.orchestration_trigger(
        context_name="context", input_type=dict)(my_orchestrator)
    assert fb is not None
    assert my_orchestrator._df_input_type is dict


# ---------------------------------------------------------------------------
# activity_trigger
# ---------------------------------------------------------------------------

def test_activity_trigger_v1_signature():
    app = df.DFApp()

    def my_activity(myinput):
        return myinput

    fb = app.activity_trigger(
        input_name="myinput", activity="MyActivity")(my_activity)
    trigger = _trigger(fb)
    assert trigger.get_binding_name() == ACTIVITY_TRIGGER
    assert trigger.name == "myinput"
    assert trigger.activity == "MyActivity"


# ---------------------------------------------------------------------------
# entity_trigger
# ---------------------------------------------------------------------------

def test_entity_trigger_v1_signature():
    app = df.DFApp()

    def my_entity(context):
        return None

    fb = app.entity_trigger(
        context_name="context", entity_name="MyEntity")(my_entity)
    trigger = _trigger(fb)
    assert trigger.get_binding_name() == ENTITY_TRIGGER
    assert trigger.name == "context"
    assert trigger.entity_name == "MyEntity"


# ---------------------------------------------------------------------------
# durable_client_input
# ---------------------------------------------------------------------------

def test_durable_client_input_v1_signature_registers_binding():
    app = df.DFApp()

    async def starter(client):
        return None

    fb = app.durable_client_input(
        client_name="client", task_hub="hub", connection_name="conn")(starter)
    bindings = fb._function.get_bindings()
    client_bindings = [b for b in bindings if b.get_binding_name() == DURABLE_CLIENT]
    assert len(client_bindings) == 1
    binding = client_bindings[0]
    assert binding.name == "client"
    assert binding.task_hub == "hub"
    assert binding.connection_name == "conn"


async def test_durable_client_input_injects_rich_client():
    app = df.DFApp()
    received = {}

    async def starter(client):
        received["client"] = client

    fb = app.durable_client_input(client_name="client")(starter)
    # _add_rich_client replaces the user function with middleware that builds
    # a DurableFunctionsClient from the binding's JSON string.
    middleware = fb._function._func
    await middleware(client=_CLIENT_CONFIG)

    client = received["client"]
    assert isinstance(client, DurableFunctionsClient)
    try:
        assert client.taskHubName == "TestHub"
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# All decorators register a function builder
# ---------------------------------------------------------------------------

def test_decorators_register_function_builders():
    app = df.DFApp()

    def orch(context):
        return 1

    app.orchestration_trigger(context_name="context")(orch)
    assert len(app._function_builders) == 1
