# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for the V1-style entity surface.

Covers the full ``DurableEntityContext`` API: state get/set/result, identity
(``entity_name`` / ``entity_key`` / ``operation_name`` / ``is_newly_constructed``
via the ``describe`` operation), ``destruct_on_exit`` (delete), and signalling an
entity from within an orchestrator (``context.signal_entity``).
"""

import time

import pytest

pytestmark = pytest.mark.functions_e2e


def test_entity_set_and_read(v1_app):
    key = f"probe-{int(time.time() * 1000)}"
    v1_app.signal_entity("probe", key, "set", input=42)
    payload = v1_app.wait_for_entity(
        "probe", key, lambda p: p["exists"] and p["state"] == 42)
    assert payload["state"] == 42


def test_entity_describe_via_orchestration(v1_app):
    key = f"probe-{int(time.time() * 1000)}"
    # Ensure the entity exists first.
    v1_app.signal_entity("probe", key, "set", input=1)
    v1_app.wait_for_entity("probe", key, lambda p: p["exists"])

    instance_id = v1_app.start_orchestration("describe_entity", body=key)
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    output = status["output"]
    assert output["entity_name"] == "probe"
    assert output["entity_key"] == key
    assert output["operation_name"] == "describe"
    assert output["is_newly_constructed"] is False


def test_entity_destruct_on_exit(v1_app):
    key = f"probe-{int(time.time() * 1000)}"
    v1_app.signal_entity("probe", key, "set", input=7)
    v1_app.wait_for_entity("probe", key, lambda p: p["exists"] and p["state"] == 7)

    v1_app.signal_entity("probe", key, "delete")
    payload = v1_app.wait_for_entity("probe", key, lambda p: not p["exists"])
    assert payload["exists"] is False


def test_signal_entity_from_orchestrator(v1_app):
    key = f"orch-signal-{int(time.time() * 1000)}"
    instance_id = v1_app.start_orchestration("signal_counter", body=key)
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"

    payload = v1_app.wait_for_entity(
        "counter", key, lambda p: p["exists"] and p["state"] == 10)
    assert payload["state"] == 10
