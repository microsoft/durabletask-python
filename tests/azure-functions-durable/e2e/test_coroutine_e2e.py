# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Real-host smoke coverage for both async orchestrator authoring styles."""

import pytest

from ._harness import FunctionApp


pytestmark = pytest.mark.functions_e2e


@pytest.mark.parametrize("app_fixture", ["dtask_app", "v1_app"])
@pytest.mark.parametrize("orchestrator", ["async_await_probe", "async_cold_probe"])
def test_async_await_orchestrator(request: pytest.FixtureRequest, app_fixture: str, orchestrator: str) -> None:
    app: FunctionApp = request.getfixturevalue(app_fixture)
    instance_id = app.start_orchestration(orchestrator)
    status = app.wait_for_completion(instance_id)
    assert status["runtimeStatus"].upper() == "COMPLETED", status
    assert status["output"] == {
        "greeting": "Hello Tokyo!",
        "total": 13,
        "child": ["Hello Tokyo!", "Hello Seattle!", "Hello London!"],
    }
