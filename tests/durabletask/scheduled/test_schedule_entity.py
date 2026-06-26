# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the Schedule entity behavior, driven through _EntityExecutor."""

import logging
from datetime import datetime, timedelta, timezone

import pytest

from durabletask.entities import EntityInstanceId
from durabletask.internal import shared
from durabletask.internal.entity_state_shim import StateShim
from durabletask.scheduled.exceptions import ScheduleInvalidTransitionError
from durabletask.scheduled.models import (ScheduleCreationOptions,
                                          ScheduleUpdateOptions)
from durabletask.scheduled.schedule_entity import (ENTITY_NAME, Schedule)
from durabletask.scheduled.schedule_status import ScheduleStatus
from durabletask.worker import _EntityExecutor, _Registry

SCHEDULE_ID = "sched-1"


class Harness:
    """Drives Schedule entity operations against a persistent in-memory state."""

    def __init__(self):
        registry = _Registry()
        registry.add_entity(Schedule, ENTITY_NAME)
        self.executor = _EntityExecutor(registry, logging.getLogger("test"))
        self.state = StateShim(None)
        self.entity_id = EntityInstanceId(ENTITY_NAME, SCHEDULE_ID)

    def run(self, operation, input=None):
        before = len(self.state.get_operation_actions())
        encoded = shared.to_json(input) if input is not None else None
        result = self.executor.execute("orch-1", self.entity_id, operation, self.state, encoded)
        self.state.commit()
        # Mimic the wire round-trip: the worker serializes the entity state at
        # the end of each batch, and the next batch receives it as deserialized
        # JSON (a plain dict). This exercises the state ``to_json``/``from_json``
        # hooks between operations and keeps assertions dict-based.
        current = self.state._current_state  # pyright: ignore[reportPrivateUsage]
        if current is not None:
            self.state._current_state = shared.from_json(shared.to_json(current))  # pyright: ignore[reportPrivateUsage]
        actions = self.state.get_operation_actions()[before:]
        return result, actions

    @property
    def state_dict(self):
        return self.state._current_state  # pyright: ignore[reportPrivateUsage]

    @property
    def token(self):
        return self.state_dict["execution_token"]


def _signal_actions(actions):
    return [a for a in actions if a.HasField("sendSignal")]


def _start_actions(actions):
    return [a for a in actions if a.HasField("startNewOrchestration")]


def _creation_options(**kwargs):
    base = dict(schedule_id=SCHEDULE_ID, orchestration_name="my_orch", interval=timedelta(seconds=30))
    base.update(kwargs)
    return ScheduleCreationOptions(**base)


class TestCreate:
    def test_create_activates_and_signals_run(self):
        h = Harness()
        _, actions = h.run("create_schedule", _creation_options())

        assert h.state_dict["status"] == ScheduleStatus.ACTIVE.value
        assert h.state_dict["schedule_created_at"] is not None
        signals = _signal_actions(actions)
        assert len(signals) == 1
        assert signals[0].sendSignal.name == "run_schedule"
        assert signals[0].sendSignal.instanceId == f"@{ENTITY_NAME}@{SCHEDULE_ID}"

    def test_create_twice_updates_in_place(self):
        h = Harness()
        h.run("create_schedule", _creation_options())
        first_token = h.token
        h.run("create_schedule", _creation_options(interval=timedelta(seconds=60)))
        # Re-creation refreshes the execution token.
        assert h.token != first_token
        assert h.state_dict["status"] == ScheduleStatus.ACTIVE.value


class TestPauseResume:
    def test_pause_then_resume(self):
        h = Harness()
        h.run("create_schedule", _creation_options())

        h.run("pause_schedule")
        assert h.state_dict["status"] == ScheduleStatus.PAUSED.value
        assert h.state_dict["next_run_at"] is None

        _, actions = h.run("resume_schedule")
        assert h.state_dict["status"] == ScheduleStatus.ACTIVE.value
        assert len(_signal_actions(actions)) == 1

    def test_pause_when_not_active_raises(self):
        h = Harness()
        h.run("create_schedule", _creation_options())
        h.run("pause_schedule")
        with pytest.raises(ScheduleInvalidTransitionError):
            h.run("pause_schedule")


class TestUpdate:
    def test_update_changes_config_and_resignals(self):
        h = Harness()
        h.run("create_schedule", _creation_options())
        _, actions = h.run("update_schedule",
                           ScheduleUpdateOptions(interval=timedelta(seconds=120)))
        assert abs(h.state_dict["schedule_configuration"]["interval_seconds"] - 120) < 0.001
        assert len(_signal_actions(actions)) == 1

    def test_update_no_change_does_not_signal(self):
        h = Harness()
        h.run("create_schedule", _creation_options())
        _, actions = h.run("update_schedule",
                           ScheduleUpdateOptions(orchestration_name="my_orch"))
        assert len(_signal_actions(actions)) == 0


class TestRunSchedule:
    def test_runs_orchestration_when_due_and_rearms(self):
        h = Harness()
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        h.run("create_schedule", _creation_options(start_at=past, start_immediately_if_late=True))

        _, actions = h.run("run_schedule", h.token)

        starts = _start_actions(actions)
        assert len(starts) == 1
        assert starts[0].startNewOrchestration.name == "my_orch"
        assert h.state_dict["last_run_at"] is not None

        # Re-arm signal should carry a future scheduled time.
        signals = _signal_actions(actions)
        assert len(signals) == 1
        assert signals[0].sendSignal.HasField("scheduledTime")

    def test_ignores_stale_token(self):
        h = Harness()
        h.run("create_schedule", _creation_options())
        _, actions = h.run("run_schedule", "stale-token")
        assert len(_start_actions(actions)) == 0
        assert len(_signal_actions(actions)) == 0

    def test_future_start_does_not_run_yet(self):
        h = Harness()
        future = datetime.now(timezone.utc) + timedelta(days=1)
        h.run("create_schedule", _creation_options(start_at=future))
        _, actions = h.run("run_schedule", h.token)
        assert len(_start_actions(actions)) == 0
        # Still re-arms with a future scheduled signal.
        signals = _signal_actions(actions)
        assert len(signals) == 1
        assert signals[0].sendSignal.HasField("scheduledTime")

    def test_past_end_time_deletes(self):
        h = Harness()
        start = datetime.now(timezone.utc) - timedelta(hours=2)
        end = datetime.now(timezone.utc) - timedelta(hours=1)
        h.run("create_schedule", _creation_options(start_at=start, end_at=end))
        _, actions = h.run("run_schedule", h.token)
        delete_signals = [a for a in _signal_actions(actions) if a.sendSignal.name == "delete"]
        assert len(delete_signals) == 1


class TestDelete:
    def test_delete_clears_state(self):
        h = Harness()
        h.run("create_schedule", _creation_options())
        h.run("delete")
        assert h.state_dict is None
