# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for schedule state transition rules."""

from durabletask.scheduled import transitions
from durabletask.scheduled.schedule_status import ScheduleStatus


class TestCreateTransitions:
    def test_create_from_uninitialized_to_active_is_valid(self):
        assert transitions.is_valid_transition(
            transitions.CREATE_SCHEDULE, ScheduleStatus.UNINITIALIZED, ScheduleStatus.ACTIVE)

    def test_create_from_active_to_active_is_valid(self):
        assert transitions.is_valid_transition(
            transitions.CREATE_SCHEDULE, ScheduleStatus.ACTIVE, ScheduleStatus.ACTIVE)

    def test_create_from_paused_to_active_is_valid(self):
        assert transitions.is_valid_transition(
            transitions.CREATE_SCHEDULE, ScheduleStatus.PAUSED, ScheduleStatus.ACTIVE)


class TestUpdateTransitions:
    def test_update_active_to_active_is_valid(self):
        assert transitions.is_valid_transition(
            transitions.UPDATE_SCHEDULE, ScheduleStatus.ACTIVE, ScheduleStatus.ACTIVE)

    def test_update_paused_to_paused_is_valid(self):
        assert transitions.is_valid_transition(
            transitions.UPDATE_SCHEDULE, ScheduleStatus.PAUSED, ScheduleStatus.PAUSED)

    def test_update_from_uninitialized_is_invalid(self):
        assert not transitions.is_valid_transition(
            transitions.UPDATE_SCHEDULE, ScheduleStatus.UNINITIALIZED, ScheduleStatus.UNINITIALIZED)


class TestPauseResumeTransitions:
    def test_pause_active_is_valid(self):
        assert transitions.is_valid_transition(
            transitions.PAUSE_SCHEDULE, ScheduleStatus.ACTIVE, ScheduleStatus.PAUSED)

    def test_pause_paused_is_invalid(self):
        assert not transitions.is_valid_transition(
            transitions.PAUSE_SCHEDULE, ScheduleStatus.PAUSED, ScheduleStatus.PAUSED)

    def test_resume_paused_is_valid(self):
        assert transitions.is_valid_transition(
            transitions.RESUME_SCHEDULE, ScheduleStatus.PAUSED, ScheduleStatus.ACTIVE)

    def test_resume_active_is_invalid(self):
        assert not transitions.is_valid_transition(
            transitions.RESUME_SCHEDULE, ScheduleStatus.ACTIVE, ScheduleStatus.ACTIVE)


def test_unknown_operation_is_invalid():
    assert not transitions.is_valid_transition(
        "unknown_op", ScheduleStatus.ACTIVE, ScheduleStatus.ACTIVE)
