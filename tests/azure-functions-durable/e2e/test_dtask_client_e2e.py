# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for the durabletask-native client management surface.

Covers ``suspend_orchestration`` / ``resume_orchestration``,
``restart_orchestration``, ``get_all_orchestration_states``,
``get_orchestration_history``, ``wait_for_orchestration_start``, and
``wait_for_orchestration_completion``.
"""

from concurrent.futures import ThreadPoolExecutor
from uuid import UUID

import pytest

from ._harness import http_request

pytestmark = pytest.mark.functions_e2e


@pytest.mark.parametrize("client_type", ["async", "sync"])
def test_invocation_correlation(dtask_app, client_type):
    def invoke(_):
        response = http_request("POST", f"{dtask_app.base_url}/api/correlation/{client_type}")
        assert response.status == 202
        return response.json()

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(invoke, range(2)))
    assert results[0]["invocationId"] != results[1]["invocationId"]
    for result in results:
        assert str(UUID(result["invocationId"])) == result["invocationId"]
        log = dtask_app.wait_for_host_log(
            f"Client operation 'StartOrchestration' received for instance '{result['id']}'.")
        assert f"FunctionInvocationId: {result['invocationId']}." in log
        dtask_app.wait_for_completion(result["id"])


def test_suspend_and_resume(dtask_app):
    instance_id = dtask_app.start_orchestration("wait_for_approval")

    result = http_request("POST", f"{dtask_app.base_url}/api/suspend/{instance_id}")
    assert result.status == 202
    dtask_app.wait_for_status(instance_id, "SUSPENDED")

    result = http_request("POST", f"{dtask_app.base_url}/api/resume/{instance_id}")
    assert result.status == 202
    dtask_app.wait_for_status(instance_id, "RUNNING")

    dtask_app.raise_event(instance_id, "approval", data=True)
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    assert status["output"] == {"approved": True}


def test_restart(dtask_app):
    instance_id = dtask_app.start_orchestration("activity_chain")
    dtask_app.wait_for_completion(instance_id)

    result = http_request("POST", f"{dtask_app.base_url}/api/restart/{instance_id}")
    assert result.status == 202
    new_id = result.json()["id"]

    status = dtask_app.wait_for_completion(new_id)
    assert status["runtimeStatus"] == "COMPLETED"
    assert status["output"] == ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]


def test_get_all_orchestration_states_includes_instance(dtask_app):
    instance_id = dtask_app.start_orchestration("activity_chain")
    dtask_app.wait_for_completion(instance_id)

    result = http_request("GET", f"{dtask_app.base_url}/api/states")
    assert result.status == 200
    assert instance_id in result.json()["ids"]


def test_get_orchestration_history(dtask_app):
    instance_id = dtask_app.start_orchestration("activity_chain")
    dtask_app.wait_for_completion(instance_id)

    result = http_request("GET", f"{dtask_app.base_url}/api/history/{instance_id}")
    assert result.status == 200
    assert result.json()["eventCount"] > 0


def test_wait_for_orchestration_start(dtask_app):
    instance_id = dtask_app.start_orchestration("wait_for_approval")

    result = http_request("GET", f"{dtask_app.base_url}/api/wait_start/{instance_id}")
    assert result.status == 200
    assert result.json()["runtimeStatus"] in ("RUNNING", "PENDING")

    # Clean up the still-running instance.
    dtask_app.raise_event(instance_id, "approval", data=True)
    dtask_app.wait_for_completion(instance_id)


def test_wait_for_orchestration_completion(dtask_app):
    instance_id = dtask_app.start_orchestration("activity_chain")

    result = http_request("GET", f"{dtask_app.base_url}/api/wait_complete/{instance_id}")
    assert result.status == 200
    body = result.json()
    assert body["runtimeStatus"] == "COMPLETED"
    assert body["output"] == ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]
