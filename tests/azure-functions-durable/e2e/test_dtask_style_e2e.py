# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""End-to-end tests for the durabletask-native-style Durable Functions app.

These run against a real Functions host (see ``conftest.py``) and exercise the
modern two-argument authoring surface plus the durabletask client method names.
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

def test_activity_chaining(dtask_app):
    instance_id = dtask_app.start_orchestration("activity_chain")
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    assert status["output"] == ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]


def test_fan_out_fan_in(dtask_app):
    instance_id = dtask_app.start_orchestration("fan_out_fan_in", body=4)
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    # 1 + 4 + 9 + 16
    assert status["output"] == 30


def test_sub_orchestration(dtask_app):
    instance_id = dtask_app.start_orchestration("sub_orchestration_parent")
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    assert status["output"] == {
        "from_child": ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]}


def test_continue_as_new(dtask_app):
    instance_id = dtask_app.start_orchestration("continue_as_new_counter", body=0)
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    assert status["output"] == 5


# ---------------------------------------------------------------------------
# External events + custom status
# ---------------------------------------------------------------------------

def test_external_event_and_custom_status(dtask_app):
    instance_id = dtask_app.start_orchestration("wait_for_approval")

    result = http_request(
        "POST", f"{dtask_app.base_url}/api/raise/{instance_id}/approval", data={"data": True})
    assert result.status == 202

    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    assert status["output"] == {"approved": True}
    assert status.get("customStatus") == "received"


# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------

def test_entity_via_orchestration(dtask_app):
    instance_id = dtask_app.start_orchestration("counter_orchestration")
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    assert status["output"] == 8


def test_entity_via_client_signal_and_read(dtask_app):
    key = f"client-{int(time.time() * 1000)}"

    result = http_request(
        "POST", f"{dtask_app.base_url}/api/signal/counter/{key}/add", data={"input": 7})
    assert result.status == 202

    payload = _wait_for_entity(
        dtask_app, "counter", key, lambda p: p["exists"] and p["state"] == 7)
    assert payload["state"] == 7


# ---------------------------------------------------------------------------
# Client management surface
# ---------------------------------------------------------------------------

def test_terminate(dtask_app):
    instance_id = dtask_app.start_orchestration("wait_for_approval")

    result = http_request("POST", f"{dtask_app.base_url}/api/terminate/{instance_id}")
    assert result.status == 202

    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "TERMINATED"


def test_purge(dtask_app):
    instance_id = dtask_app.start_orchestration("activity_chain")
    dtask_app.wait_for_completion(instance_id)

    result = http_request("POST", f"{dtask_app.base_url}/api/purge/{instance_id}")
    assert result.status == 200
    assert result.json()["instancesDeleted"] == 1
