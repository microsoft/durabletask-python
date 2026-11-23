# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Real-host smoke coverage for two-argument async orchestrators."""

from typing import Any

import pytest

from ._harness import FunctionApp


pytestmark = pytest.mark.functions_e2e


def _completed_output(app: FunctionApp, name: str, body: Any = None) -> Any:
    instance_id = app.start_orchestration(name, body=body)
    status = app.wait_for_completion(instance_id)
    assert status["runtimeStatus"].upper() == "COMPLETED", status
    return status["output"]


@pytest.mark.parametrize("orchestrator", ["async_await_probe", "async_cold_probe"])
def test_async_await_orchestrator(dtask_app: FunctionApp, orchestrator: str) -> None:
    assert _completed_output(dtask_app, orchestrator) == {
        "greeting": "Hello Tokyo!",
        "total": 13,
        "child": ["Hello Tokyo!", "Hello Seattle!", "Hello London!"],
    }


def test_async_fan_out(dtask_app: FunctionApp) -> None:
    assert _completed_output(dtask_app, "async_fan_out", 5) == 55


def test_async_race_returns_winning_handle(dtask_app: FunctionApp) -> None:
    assert _completed_output(dtask_app, "async_race") == {"activity_won": True, "result": "Hello Tokyo!"}


def test_async_call_http(dtask_app: FunctionApp) -> None:
    output = _completed_output(dtask_app, "async_call_http", f"{dtask_app.base_url}/api/ping")
    assert output == {"status_code": 200, "content": "pong"}
