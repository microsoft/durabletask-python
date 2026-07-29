# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for the durabletask-native orchestration-context surface.

Covers timers (``ctx.create_timer``), ``task.when_any`` selection, deterministic
IDs (``ctx.new_uuid``), context properties (``is_replaying``, ``version``,
``parent_instance_id``, ``current_utc_datetime``), and parent/child
``parent_instance_id`` propagation.
"""

from uuid import UUID

import pytest

from ._markers import azurite_delayed_visibility

pytestmark = pytest.mark.functions_e2e


@azurite_delayed_visibility
def test_timer(dtask_app):
    instance_id = dtask_app.start_orchestration("timer_wait")
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    assert status["output"] == "fired"


def test_when_any_event_wins(dtask_app):
    instance_id = dtask_app.start_orchestration("event_or_timeout")
    dtask_app.raise_event(instance_id, "go", data="hello")
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    assert status["output"] == {"result": "event", "data": "hello"}


def test_deterministic_ids(dtask_app):
    instance_id = dtask_app.start_orchestration("deterministic_ids")
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    output = status["output"]
    assert str(UUID(output["uuid1"])) == output["uuid1"]
    assert str(UUID(output["uuid2"])) == output["uuid2"]
    assert output["uuid1"] != output["uuid2"]


def test_context_properties(dtask_app):
    instance_id = dtask_app.start_orchestration("context_properties")
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    output = status["output"]
    assert output["instance_id"] == instance_id
    assert isinstance(output["is_replaying"], bool)
    assert output["version"] is None
    assert output["parent_instance_id"] is None
    assert output["has_current_utc_datetime"] is True


def test_parent_instance_id_propagation(dtask_app):
    instance_id = dtask_app.start_orchestration("parent_with_child")
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    output = status["output"]
    assert output["parent_seen_by_child"] == output["my_instance"]
    assert output["my_instance"] == instance_id
