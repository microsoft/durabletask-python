# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""End-to-end tests for the V1-style Durable Functions sample app.

These run against a real Functions host (see ``conftest.py``) and exercise the
classic v1 authoring surface plus the deprecated v1 client management APIs.
"""

import time

import pytest

from ._harness import http_request

pytestmark = pytest.mark.functions_e2e


def _wait_for_entity(app, name, key, predicate, timeout=30):
    """Poll the entity read route until ``predicate(payload)`` is true."""
    deadline = time.time() + timeout
    payload = None
    while time.time() < deadline:
        result = http_request("GET", f"{app.base_url}/api/entity/{name}/{key}")
        assert result.status == 200, f"entity read failed: {result.status} {result.body}"
        payload = result.json()
        if predicate(payload):
            return payload
        time.sleep(0.5)
    raise TimeoutError(f"entity {name}/{key} predicate not met; last: {payload}")


# ---------------------------------------------------------------------------
# Orchestration patterns
# ---------------------------------------------------------------------------

def test_activity_chaining(v1_app):
    instance_id = v1_app.start_orchestration("activity_chain")
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    assert status["output"] == ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]


def test_fan_out_fan_in(v1_app):
    instance_id = v1_app.start_orchestration("fan_out_fan_in", body=4)
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    # 1 + 4 + 9 + 16
    assert status["output"] == 30


def test_sub_orchestration(v1_app):
    instance_id = v1_app.start_orchestration("sub_orchestration_parent")
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    assert status["output"] == {
        "from_child": ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]}


def test_continue_as_new(v1_app):
    instance_id = v1_app.start_orchestration("continue_as_new_counter", body=0)
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    assert status["output"] == 5


# ---------------------------------------------------------------------------
# External events + custom status
# ---------------------------------------------------------------------------

def test_external_event_and_custom_status(v1_app):
    instance_id = v1_app.start_orchestration("wait_for_approval")

    # Raise the awaited event (buffered by the runtime if not yet subscribed).
    result = http_request(
        "POST", f"{v1_app.base_url}/api/raise/{instance_id}/approval", data={"data": True})
    assert result.status == 202

    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    assert status["output"] == {"approved": True}
    assert status.get("customStatus") == "received"


# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------

def test_entity_via_orchestration(v1_app):
    instance_id = v1_app.start_orchestration("counter_orchestration")
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    assert status["output"] == 8


def test_entity_via_client_signal_and_read(v1_app):
    key = f"client-{int(time.time() * 1000)}"

    result = http_request(
        "POST", f"{v1_app.base_url}/api/signal/counter/{key}/add", data={"input": 7})
    assert result.status == 202

    payload = _wait_for_entity(
        v1_app, "counter", key, lambda p: p["exists"] and p["state"] == 7)
    assert payload["state"] == 7


# ---------------------------------------------------------------------------
# Durable HTTP (V1-only feature)
# ---------------------------------------------------------------------------

def test_call_http(v1_app):
    instance_id = v1_app.start_orchestration("http_call", body=f"{v1_app.base_url}/api/ping")
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    assert status["output"]["status_code"] == 200
    assert status["output"]["content"] == "pong"


# ---------------------------------------------------------------------------
# Client management surface
# ---------------------------------------------------------------------------

def test_check_status_response_shape(v1_app):
    result = http_request("POST", f"{v1_app.base_url}/api/start/activity_chain", data={"input": None})
    assert result.status == 202
    payload = result.json()
    assert "id" in payload
    assert "statusQueryGetUri" in payload
    assert "terminatePostUri" in payload


def test_terminate(v1_app):
    # Start a long-waiting orchestration, then terminate it.
    instance_id = v1_app.start_orchestration("wait_for_approval")

    result = http_request("POST", f"{v1_app.base_url}/api/terminate/{instance_id}")
    assert result.status == 202

    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Terminated"


def test_purge(v1_app):
    instance_id = v1_app.start_orchestration("activity_chain")
    v1_app.wait_for_completion(instance_id)

    result = http_request("POST", f"{v1_app.base_url}/api/purge/{instance_id}")
    assert result.status == 200
    assert result.json()["instancesDeleted"] == 1
