# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for the durabletask-native entity surface.

Covers ``EntityContext`` state get/set, identity (``entity_id.entity`` /
``entity_id.key`` / ``operation`` via the ``describe`` operation), deletion (set
state to ``None``), and signalling an entity from within an orchestrator
(``ctx.signal_entity``).
"""

import time

import pytest

pytestmark = pytest.mark.functions_e2e


def test_entity_set_and_read(dtask_app):
    key = f"probe-{int(time.time() * 1000)}"
    dtask_app.signal_entity("probe", key, "set", input=42)
    payload = dtask_app.wait_for_entity(
        "probe", key, lambda p: p["exists"] and p["state"] == 42)
    assert payload["state"] == 42


def test_entity_describe_via_orchestration(dtask_app):
    key = f"probe-{int(time.time() * 1000)}"
    dtask_app.signal_entity("probe", key, "set", input=1)
    dtask_app.wait_for_entity("probe", key, lambda p: p["exists"])

    instance_id = dtask_app.start_orchestration("describe_entity", body=key)
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    output = status["output"]
    assert output["entity"] == "probe"
    assert output["key"] == key
    assert output["operation"] == "describe"


def test_entity_delete(dtask_app):
    key = f"probe-{int(time.time() * 1000)}"
    dtask_app.signal_entity("probe", key, "set", input=7)
    dtask_app.wait_for_entity("probe", key, lambda p: p["exists"] and p["state"] == 7)

    dtask_app.signal_entity("probe", key, "delete")
    payload = dtask_app.wait_for_entity("probe", key, lambda p: not p["exists"])
    assert payload["exists"] is False


def test_signal_entity_from_orchestrator(dtask_app):
    key = f"orch-signal-{int(time.time() * 1000)}"
    instance_id = dtask_app.start_orchestration("signal_counter", body=key)
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"

    payload = dtask_app.wait_for_entity(
        "counter", key, lambda p: p["exists"] and p["state"] == 10)
    assert payload["state"] == 10
