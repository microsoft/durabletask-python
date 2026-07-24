# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the documented Durable Functions unit-testing patterns.

These mirror the examples in the "Unit testing Durable Functions and Durable
Task SDKs" article (Durable Functions / Python tab), confirming that v2.x
exposes the same testing surface as v1:

* ``azure.durable_functions.testing.orchestrator_generator_wrapper`` drives an
  orchestrator generator with mocked task results.
* Orchestrator handles expose ``.orchestrator_function``.
* Client (trigger) functions expose ``.client_function``.
"""

import asyncio
from unittest.mock import AsyncMock, Mock, call

import azure.durable_functions as df
import azure.functions as func
from azure.durable_functions.testing import orchestrator_generator_wrapper


app = df.DFApp()


@app.orchestration_trigger(context_name="context")
def my_orchestrator(context):
    result1 = yield context.call_activity("say_hello", "Tokyo")
    result2 = yield context.call_activity("say_hello", "Seattle")
    result3 = yield context.call_activity("say_hello", "London")
    return [result1, result2, result3]


@app.route(route="start")
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    instance_id = await client.start_new("my_orchestrator")
    return client.create_check_status_response(req, instance_id)


def _mock_activity(activity_name, input_value):
    mock_task = Mock()
    mock_task.result = f"Hello {input_value}!"
    return mock_task


def test_chaining_orchestrator():
    """The documented orchestrator test pattern works end to end."""
    func_call = my_orchestrator.build().get_user_function().orchestrator_function

    context = Mock()
    context.call_activity = Mock(side_effect=_mock_activity)

    user_orchestrator = func_call(context)
    values = list(orchestrator_generator_wrapper(user_orchestrator))

    expected_calls = [
        call("say_hello", "Tokyo"),
        call("say_hello", "Seattle"),
        call("say_hello", "London"),
    ]
    assert context.call_activity.call_args_list == expected_calls
    assert values[-1] == ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]


def test_orchestrator_wrapper_yields_each_task_then_result():
    """The wrapper re-yields every task before the final return value."""
    func_call = my_orchestrator.build().get_user_function().orchestrator_function

    context = Mock()
    context.call_activity = Mock(side_effect=_mock_activity)

    values = list(orchestrator_generator_wrapper(func_call(context)))

    # Three task objects, then the final orchestrator output.
    assert len(values) == 4
    assert all(hasattr(v, "result") for v in values[:3])
    assert values[-1] == ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]


def test_orchestrator_wrapper_throws_task_exception_into_generator():
    """A failing task result is thrown back into the orchestrator."""

    @app.orchestration_trigger(context_name="context")
    def handles_failure(context):
        try:
            yield context.call_activity("boom", None)
        except ValueError as e:
            return f"caught: {e}"
        return "no error"

    func_call = handles_failure.build().get_user_function().orchestrator_function

    failing_task = Mock()
    type(failing_task).result = property(
        lambda self: (_ for _ in ()).throw(ValueError("activity failed")))

    context = Mock()
    context.call_activity = Mock(return_value=failing_task)

    values = list(orchestrator_generator_wrapper(func_call(context)))
    assert values[-1] == "caught: activity failed"


def test_orchestrator_wrapper_handles_immediate_return_without_yield():
    """An orchestrator that returns without yielding completes immediately.

    ``next(generator)`` raises ``StopIteration`` for such orchestrators; the
    wrapper must surface the return value rather than leaking the exception.
    """

    @app.orchestration_trigger(context_name="context")
    def returns_immediately(context):
        return "done"
        yield  # pragma: no cover - makes the function a generator

    func_call = returns_immediately.build().get_user_function().orchestrator_function

    values = list(orchestrator_generator_wrapper(func_call(Mock())))
    assert values == ["done"]


def test_client_function_is_exposed_for_testing():
    """The documented client test pattern works end to end."""
    func_call = http_start.build().get_user_function().client_function

    req = func.HttpRequest(method="GET", body=b"{}", url="/api/start")

    client = Mock()
    client.start_new = AsyncMock(return_value="test-instance-id")
    client.create_check_status_response = Mock(return_value="check_status_response")

    result = asyncio.run(func_call(req, client))

    assert result == "check_status_response"
    client.start_new.assert_called_once_with("my_orchestrator")
    client.create_check_status_response.assert_called_once_with(
        req, "test-instance-id")
