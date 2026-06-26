# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for scheduled tasks models, validation, and serialization."""

from datetime import datetime, timedelta, timezone

import pytest

from durabletask.internal import shared
from durabletask.scheduled.models import (ScheduleConfiguration,
                                          ScheduleCreationOptions,
                                          ScheduleState, ScheduleUpdateOptions)
from durabletask.scheduled.schedule_status import ScheduleStatus


class TestCreationOptionsValidation:
    def test_requires_schedule_id(self):
        with pytest.raises(ValueError):
            ScheduleCreationOptions(schedule_id="", orchestration_name="orch",
                                    interval=timedelta(seconds=5))

    def test_requires_orchestration_name(self):
        with pytest.raises(ValueError):
            ScheduleCreationOptions(schedule_id="s1", orchestration_name="",
                                    interval=timedelta(seconds=5))

    def test_interval_must_be_at_least_one_second(self):
        with pytest.raises(ValueError):
            ScheduleCreationOptions(schedule_id="s1", orchestration_name="orch",
                                    interval=timedelta(milliseconds=500))

    def test_interval_must_be_positive(self):
        with pytest.raises(ValueError):
            ScheduleCreationOptions(schedule_id="s1", orchestration_name="orch",
                                    interval=timedelta(seconds=-1))

    def test_valid_options(self):
        options = ScheduleCreationOptions(schedule_id="s1", orchestration_name="orch",
                                          interval=timedelta(seconds=30))
        assert options.schedule_id == "s1"
        assert options.interval == timedelta(seconds=30)


class TestCreationOptionsSerialization:
    def test_round_trip_through_json(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 2, 1, tzinfo=timezone.utc)
        options = ScheduleCreationOptions(
            schedule_id="s1", orchestration_name="orch", interval=timedelta(minutes=5),
            orchestration_input={"key": "value"}, orchestration_instance_id="inst-1",
            start_at=start, end_at=end, start_immediately_if_late=True)

        encoded = shared.to_json(options.to_dict())
        decoded = ScheduleCreationOptions.from_dict(shared.from_json(encoded))

        assert decoded.schedule_id == "s1"
        assert decoded.orchestration_name == "orch"
        assert decoded.interval == timedelta(minutes=5)
        assert decoded.orchestration_input == {"key": "value"}
        assert decoded.orchestration_instance_id == "inst-1"
        assert decoded.start_at == start
        assert decoded.end_at == end
        assert decoded.start_immediately_if_late is True


class TestUpdateOptions:
    def test_interval_validation(self):
        with pytest.raises(ValueError):
            ScheduleUpdateOptions(interval=timedelta(milliseconds=100))

    def test_round_trip_through_json(self):
        options = ScheduleUpdateOptions(orchestration_name="orch2", interval=timedelta(seconds=10))
        decoded = ScheduleUpdateOptions.from_dict(shared.from_json(shared.to_json(options.to_dict())))
        assert decoded.orchestration_name == "orch2"
        assert decoded.interval == timedelta(seconds=10)
        assert decoded.start_at is None


class TestScheduleConfiguration:
    def test_from_create_options_rejects_start_after_end(self):
        options = ScheduleCreationOptions(
            schedule_id="s1", orchestration_name="orch", interval=timedelta(seconds=5),
            start_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
            end_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
        with pytest.raises(ValueError):
            ScheduleConfiguration.from_create_options(options)

    def test_update_returns_changed_fields(self):
        config = ScheduleConfiguration.from_create_options(
            ScheduleCreationOptions(schedule_id="s1", orchestration_name="orch",
                                    interval=timedelta(seconds=5)))
        changed = config.update(ScheduleUpdateOptions(interval=timedelta(seconds=10),
                                                      orchestration_name="orch2"))
        assert changed == {"interval", "orchestration_name"}
        assert config.interval == timedelta(seconds=10)
        assert config.orchestration_name == "orch2"

    def test_update_no_changes_returns_empty(self):
        config = ScheduleConfiguration.from_create_options(
            ScheduleCreationOptions(schedule_id="s1", orchestration_name="orch",
                                    interval=timedelta(seconds=5)))
        changed = config.update(ScheduleUpdateOptions(orchestration_name="orch"))
        assert changed == set()

    def test_config_round_trip(self):
        config = ScheduleConfiguration.from_create_options(
            ScheduleCreationOptions(schedule_id="s1", orchestration_name="orch",
                                    interval=timedelta(seconds=5),
                                    start_at=datetime(2026, 1, 1, tzinfo=timezone.utc)))
        restored = ScheduleConfiguration.from_dict(shared.from_json(shared.to_json(config.to_dict())))
        assert restored.schedule_id == "s1"
        assert restored.interval == timedelta(seconds=5)
        assert restored.start_at == datetime(2026, 1, 1, tzinfo=timezone.utc)


class TestScheduleState:
    def test_round_trip_and_description(self):
        state = ScheduleState()
        state.status = ScheduleStatus.ACTIVE
        state.schedule_created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        state.schedule_configuration = ScheduleConfiguration.from_create_options(
            ScheduleCreationOptions(schedule_id="s1", orchestration_name="orch",
                                    interval=timedelta(seconds=5)))

        restored = ScheduleState.from_dict(shared.from_json(shared.to_json(state.to_dict())))
        assert restored.status == ScheduleStatus.ACTIVE
        assert restored.schedule_created_at == datetime(2026, 1, 1, tzinfo=timezone.utc)

        description = restored.to_description()
        assert description.schedule_id == "s1"
        assert description.status == ScheduleStatus.ACTIVE
        assert description.interval == timedelta(seconds=5)

    def test_refresh_execution_token_changes_token(self):
        state = ScheduleState()
        original = state.execution_token
        state.refresh_execution_token()
        assert state.execution_token != original
