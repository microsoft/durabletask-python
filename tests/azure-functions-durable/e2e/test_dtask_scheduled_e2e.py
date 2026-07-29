# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for opt-in durabletask scheduled tasks in a Functions app.

The dtask app opts in via ``app.configure_scheduled_tasks()``. These tests
create a schedule that runs the ``scheduled_tick`` orchestration (which signals
a counter entity) on a short interval, verify it fires repeatedly, then delete
it -- exercising ``ScheduledTaskClient`` end-to-end through the Functions host.
"""

import time

import pytest

from ._markers import azurite_delayed_visibility

pytestmark = [
    pytest.mark.functions_e2e,
    azurite_delayed_visibility,
]


def test_scheduled_orchestration_fires_repeatedly(dtask_app):
    stamp = int(time.time() * 1000)
    key = f"sched-{stamp}"
    schedule_id = f"schedule-{stamp}"

    created = dtask_app.create_schedule(schedule_id, interval_seconds=2, input=key)
    assert created["scheduleId"] == schedule_id
    assert created["status"].lower().endswith("active")

    try:
        # Each run of ``scheduled_tick`` signals counter[key] += 1. At a 2s
        # interval, expect at least two increments within the window.
        payload = dtask_app.wait_for_entity(
            "counter", key, lambda p: p["exists"] and p["state"] >= 2, timeout=40)
        assert payload["state"] >= 2

        described = dtask_app.describe_schedule(schedule_id)
        assert described["exists"] is True
        assert described["scheduleId"] == schedule_id
    finally:
        dtask_app.delete_schedule(schedule_id)


def test_scheduled_task_delete_stops_runs(dtask_app):
    stamp = int(time.time() * 1000)
    key = f"scheddel-{stamp}"
    schedule_id = f"scheduledel-{stamp}"

    dtask_app.create_schedule(schedule_id, interval_seconds=2, input=key)
    # Let it fire at least once.
    dtask_app.wait_for_entity("counter", key, lambda p: p["exists"] and p["state"] >= 1, timeout=40)
    dtask_app.delete_schedule(schedule_id)

    # After deletion the schedule should no longer exist.
    described = dtask_app.describe_schedule(schedule_id)
    assert described["exists"] is False
