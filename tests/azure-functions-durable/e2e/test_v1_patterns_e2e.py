# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for the V1-style orchestration-context surface.

Covers timers, ``task_any`` selection, deterministic IDs (``new_uuid`` /
``new_guid``), context properties (``is_replaying``, ``version``,
``parent_instance_id``, ``current_utc_datetime``, ``will_continue_as_new``,
``function_context``), and parent/child ``parent_instance_id`` propagation.
"""

from uuid import UUID

import pytest

from ._markers import azurite_delayed_visibility

pytestmark = pytest.mark.functions_e2e


@azurite_delayed_visibility
def test_timer(v1_app):
    instance_id = v1_app.start_orchestration("timer_wait")
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    assert status["output"] == "fired"


def test_task_any_event_wins(v1_app):
    instance_id = v1_app.start_orchestration("event_or_timeout")
    v1_app.raise_event(instance_id, "go", data="hello")
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    assert status["output"] == {"result": "event", "data": "hello"}


def test_deterministic_ids(v1_app):
    instance_id = v1_app.start_orchestration("deterministic_ids")
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    output = status["output"]
    # Both must be parseable UUID strings.
    assert str(UUID(output["uuid"])) == output["uuid"]
    assert str(UUID(output["guid"])) == output["guid"]


def test_context_properties(v1_app):
    instance_id = v1_app.start_orchestration("context_properties")
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    output = status["output"]
    assert output["instance_id"] == instance_id
    assert isinstance(output["is_replaying"], bool)
    assert output["version"] is None
    assert output["parent_instance_id"] is None
    assert output["has_current_utc_datetime"] is True
    assert output["will_continue_as_new"] is False
    assert output["has_function_context"] is True


def test_parent_instance_id_propagation(v1_app):
    instance_id = v1_app.start_orchestration("parent_with_child")
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    output = status["output"]
    # The child must observe its parent's instance ID.
    assert output["parent_seen_by_child"] == output["my_instance"]
    assert output["my_instance"] == instance_id
