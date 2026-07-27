# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import azure.durable_functions as df
from azure.durable_functions.constants import (
    ACTIVITY_TRIGGER,
    DURABLE_CLIENT,
    ENTITY_TRIGGER,
    ORCHESTRATION_TRIGGER,
)


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


def test_activity_trigger_adapts_durabletask_native_two_param():
    app = df.DFApp()

    def my_activity(ctx, payload):
        return {"echo": payload}

    fb = app.activity_trigger(
        input_name="payload", activity="MyActivity")(my_activity)
    trigger = _trigger(fb)
    assert trigger.name == "payload"
    assert trigger.activity == "MyActivity"

    # The registered function is adapted to a single-input signature named after
    # ``input_name``, invoking the original with a placeholder activity context.
    import inspect
    registered = fb._function._func
    assert list(inspect.signature(registered).parameters) == ["payload"]
    assert registered.__name__ == "my_activity"
    assert registered("hello") == {"echo": "hello"}


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


async def test_durable_client_input_wraps_host_configuration_as_async_client():
    app = df.DFApp()

    async def starter(client):
        return type(client).__name__

    fb = app.durable_client_input(client_name="client")(starter)
    assert await fb._function._func(client="{}") == "DurableFunctionsClient"


async def test_durable_client_input_sync_injects_sync_client():
    app = df.DFApp()

    def starter(client):
        return type(client).__name__

    fb = app.durable_client_input_sync(client_name="client")(starter)
    assert await fb._function._func(client="{}") == "SyncDurableFunctionsClient"


# ---------------------------------------------------------------------------
# All decorators register a function builder
# ---------------------------------------------------------------------------

def test_decorators_register_function_builders():
    app = df.DFApp()
    baseline = len(app._function_builders)

    def orch(context):
        return 1

    app.orchestration_trigger(context_name="context")(orch)
    assert len(app._function_builders) == baseline + 1


# ---------------------------------------------------------------------------
# Blueprint registration
# ---------------------------------------------------------------------------

def _function_names(app):
    return [fb._function.get_function_name() for fb in app._function_builders]


def test_register_functions_dedupes_builtin_http_functions():
    # Both the DFApp and every Blueprint auto-register the reserved built-in
    # durable-HTTP functions. Registering a blueprint must not produce a
    # duplicate-function-name conflict for those reserved names.
    app = df.DFApp()
    bp = df.Blueprint()

    @bp.activity_trigger(input_name="name")
    def hello(name):
        return name

    app.register_functions(bp)

    names = _function_names(app)
    assert names.count("BuiltIn__HttpActivity") == 1
    assert names.count("BuiltIn__HttpPollOrchestrator") == 1
    assert "hello" in names


def test_register_blueprint_dedupes_builtin_http_functions():
    # register_blueprint is an alias of register_functions in the base class
    # and must get the same built-in de-duplication.
    app = df.DFApp()
    bp = df.Blueprint()

    @bp.orchestration_trigger(context_name="context")
    def orch(context):
        return 1

    app.register_blueprint(bp)

    names = _function_names(app)
    assert names.count("BuiltIn__HttpActivity") == 1
    assert names.count("BuiltIn__HttpPollOrchestrator") == 1
    assert "orch" in names


def test_register_functions_is_non_destructive_to_blueprint():
    # The same blueprint may be registered into more than one app, so its own
    # function builders (including the built-ins) must be left intact.
    app1 = df.DFApp()
    app2 = df.DFApp()
    bp = df.Blueprint()

    @bp.activity_trigger(input_name="name")
    def hello(name):
        return name

    app1.register_functions(bp)
    app2.register_functions(bp)

    bp_names = [fb._function.get_function_name() for fb in bp._function_builders]
    assert "BuiltIn__HttpActivity" in bp_names
    assert "BuiltIn__HttpPollOrchestrator" in bp_names
    assert "hello" in bp_names

    for app in (app1, app2):
        names = _function_names(app)
        assert names.count("BuiltIn__HttpActivity") == 1
        assert names.count("BuiltIn__HttpPollOrchestrator") == 1
        assert "hello" in names


def test_register_multiple_blueprints_no_conflict():
    app = df.DFApp()
    bp1 = df.Blueprint()
    bp2 = df.Blueprint()

    @bp1.activity_trigger(input_name="name")
    def hello(name):
        return name

    @bp2.orchestration_trigger(context_name="context")
    def orch(context):
        return 1

    app.register_functions(bp1)
    app.register_functions(bp2)

    # Building the functions is what the host does at indexing time; it raises
    # on duplicate names, so a successful build proves there is no conflict.
    names = [fn.get_function_name() for fn in app.get_functions()]
    assert names.count("BuiltIn__HttpActivity") == 1
    assert names.count("BuiltIn__HttpPollOrchestrator") == 1
    assert "hello" in names
    assert "orch" in names
