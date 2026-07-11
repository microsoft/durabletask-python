# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for the deprecated V1 ``DurableOrchestrationClient`` surface.

Covers ``suspend``/``resume``, ``restart``, ``get_status_all``,
``get_status_by``, ``purge_instance_history_by``,
``create_http_management_payload`` / ``get_client_response_links``,
``wait_for_completion_or_create_check_status_response``, and the ``rewind``
NotImplementedError stub.
"""

from datetime import datetime, timedelta, timezone

import pytest

from ._harness import http_request

pytestmark = pytest.mark.functions_e2e


def test_suspend_and_resume(v1_app):
    instance_id = v1_app.start_orchestration("wait_for_approval")

    result = http_request("POST", f"{v1_app.base_url}/api/suspend/{instance_id}")
    assert result.status == 202
    v1_app.wait_for_status(instance_id, "Suspended")

    result = http_request("POST", f"{v1_app.base_url}/api/resume/{instance_id}")
    assert result.status == 202
    v1_app.wait_for_status(instance_id, "Running")

    v1_app.raise_event(instance_id, "approval", data=True)
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    assert status["output"] == {"approved": True}


def test_restart(v1_app):
    instance_id = v1_app.start_orchestration("activity_chain")
    v1_app.wait_for_completion(instance_id)

    result = http_request("POST", f"{v1_app.base_url}/api/restart/{instance_id}")
    assert result.status == 202
    new_id = result.json()["id"]

    status = v1_app.wait_for_completion(new_id)
    assert status["runtimeStatus"] == "Completed"
    assert status["output"] == ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]


def test_get_status_all_includes_instance(v1_app):
    instance_id = v1_app.start_orchestration("activity_chain")
    v1_app.wait_for_completion(instance_id)

    result = http_request("GET", f"{v1_app.base_url}/api/status_all")
    assert result.status == 200
    assert instance_id in result.json()["ids"]


def test_get_status_by_runtime_status(v1_app):
    instance_id = v1_app.start_orchestration("activity_chain")
    v1_app.wait_for_completion(instance_id)

    result = http_request("GET", f"{v1_app.base_url}/api/status_by/Completed")
    assert result.status == 200
    assert instance_id in result.json()["ids"]


def test_purge_instance_history_by(v1_app):
    created_from = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    instance_id = v1_app.start_orchestration("activity_chain")
    v1_app.wait_for_completion(instance_id)

    result = http_request(
        "POST", f"{v1_app.base_url}/api/purge_by",
        data={"from": created_from, "runtimeStatus": "Completed"})
    assert result.status == 200
    assert result.json()["instancesDeleted"] >= 1


def test_create_http_management_payload(v1_app):
    instance_id = v1_app.start_orchestration("activity_chain")
    v1_app.wait_for_completion(instance_id)

    result = http_request("GET", f"{v1_app.base_url}/api/mgmt_payload/{instance_id}")
    assert result.status == 200
    body = result.json()
    payload = body["payload"]
    assert payload["id"] == instance_id
    assert "statusQueryGetUri" in payload
    assert "terminatePostUri" in payload
    # get_client_response_links returns the same links.
    assert body["links"] == payload


def test_wait_for_completion_or_check_status(v1_app):
    instance_id = v1_app.start_orchestration("activity_chain")
    v1_app.wait_for_completion(instance_id)

    # Already complete, so the call returns the output with a 200.
    result = http_request("GET", f"{v1_app.base_url}/api/wait_or_check/{instance_id}")
    assert result.status == 200
    assert result.json() == ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]


def test_rewind(v1_app):
    # The orchestration's activity fails on its first attempt, so it lands in a
    # Failed state; rewinding replays the failed activity, which now succeeds.
    instance_id = v1_app.start_orchestration("rewind_target")
    failed = v1_app.wait_for_completion(instance_id)
    assert failed["runtimeStatus"] == "Failed"

    result = http_request("POST", f"{v1_app.base_url}/api/rewind/{instance_id}")
    assert result.status == 202

    status = v1_app.wait_for_status(instance_id, "Completed")
    assert status["runtimeStatus"] == "Completed"
    assert status["output"] == "succeeded on attempt 2"
