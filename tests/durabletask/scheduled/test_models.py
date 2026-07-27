# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for scheduled tasks models, validation, and serialization."""

from datetime import datetime, timedelta, timezone

import pytest

from durabletask.serialization import JsonDataConverter
from durabletask.scheduled.models import (ScheduleConfiguration,
                                          ScheduleCreationOptions, ScheduleQuery,
                                          ScheduleState, ScheduleUpdateOptions)
from durabletask.scheduled.schedule_status import ScheduleStatus

converter = JsonDataConverter()


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

        encoded = converter.serialize(options)
        decoded = converter.deserialize(encoded, ScheduleCreationOptions)

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
        decoded = converter.deserialize(converter.serialize(options), ScheduleUpdateOptions)
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
        restored = converter.deserialize(converter.serialize(config), ScheduleConfiguration)
        assert restored.schedule_id == "s1"
        assert restored.interval == timedelta(seconds=5)
        assert restored.start_at == datetime(2026, 1, 1, tzinfo=timezone.utc)

    def test_to_json_uses_dotnet_compatible_shape(self):
        # The Durable Task Scheduler dashboard deserializes the raw entity state
        # into the .NET types, so the persisted JSON must use PascalCase keys and
        # a .NET ``TimeSpan`` string for the interval.
        config = ScheduleConfiguration.from_create_options(
            ScheduleCreationOptions(schedule_id="s1", orchestration_name="orch",
                                    interval=timedelta(hours=1)))
        payload = config.to_json()
        assert payload["ScheduleId"] == "s1"
        assert payload["OrchestrationName"] == "orch"
        assert payload["Interval"] == "01:00:00"
        assert payload["StartImmediatelyIfLate"] is False
        assert "interval_seconds" not in payload

    @pytest.mark.parametrize("interval,expected", [
        (timedelta(seconds=1), "00:00:01"),
        (timedelta(hours=1), "01:00:00"),
        (timedelta(days=1, hours=2, minutes=3, seconds=4), "1.02:03:04"),
        (timedelta(seconds=1, milliseconds=500), "00:00:01.5000000"),
    ])
    def test_interval_timespan_round_trip(self, interval, expected):
        config = ScheduleConfiguration.from_create_options(
            ScheduleCreationOptions(schedule_id="s1", orchestration_name="orch",
                                    interval=interval))
        payload = config.to_json()
        assert payload["Interval"] == expected
        restored = ScheduleConfiguration.from_json(payload)
        assert restored.interval == interval

    def test_from_json_accepts_legacy_snake_case(self):
        legacy = {
            "schedule_id": "s1",
            "orchestration_name": "orch",
            "interval_seconds": 5.0,
            "start_immediately_if_late": True,
        }
        restored = ScheduleConfiguration.from_json(legacy)
        assert restored.schedule_id == "s1"
        assert restored.interval == timedelta(seconds=5)
        assert restored.start_immediately_if_late is True

    def test_orchestration_input_persisted_as_json_string(self):
        # .NET / the DTS dashboard model OrchestrationInput as a string, so the
        # raw input must be serialized to a JSON string in the persisted state.
        config = ScheduleConfiguration.from_create_options(
            ScheduleCreationOptions(schedule_id="s1", orchestration_name="orch",
                                    interval=timedelta(seconds=5),
                                    orchestration_input="hello"))
        payload = config.to_json()
        assert payload["OrchestrationInput"] == '"hello"'
        restored = ScheduleConfiguration.from_json(payload)
        assert restored.orchestration_input == "hello"

    def test_orchestration_input_dict_round_trips(self):
        config = ScheduleConfiguration.from_create_options(
            ScheduleCreationOptions(schedule_id="s1", orchestration_name="orch",
                                    interval=timedelta(seconds=5),
                                    orchestration_input={"key": "value"}))
        payload = config.to_json()
        assert isinstance(payload["OrchestrationInput"], str)
        restored = ScheduleConfiguration.from_json(payload)
        assert restored.orchestration_input == {"key": "value"}

    def test_orchestration_input_none_stays_none(self):
        config = ScheduleConfiguration.from_create_options(
            ScheduleCreationOptions(schedule_id="s1", orchestration_name="orch",
                                    interval=timedelta(seconds=5)))
        payload = config.to_json()
        assert payload["OrchestrationInput"] is None
        restored = ScheduleConfiguration.from_json(payload)
        assert restored.orchestration_input is None


class TestScheduleState:
    def test_round_trip_and_description(self):
        state = ScheduleState()
        state.status = ScheduleStatus.ACTIVE
        state.schedule_created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        state.schedule_configuration = ScheduleConfiguration.from_create_options(
            ScheduleCreationOptions(schedule_id="s1", orchestration_name="orch",
                                    interval=timedelta(seconds=5)))

        # The nested ``ScheduleConfiguration`` round-trips automatically.
        restored = converter.deserialize(converter.serialize(state), ScheduleState)
        assert restored.status == ScheduleStatus.ACTIVE
        assert restored.schedule_created_at == datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert restored.schedule_configuration is not None
        assert restored.schedule_configuration.interval == timedelta(seconds=5)

        description = restored.to_description()
        assert description.schedule_id == "s1"
        assert description.status == ScheduleStatus.ACTIVE
        assert description.interval == timedelta(seconds=5)

    def test_to_json_serializes_status_as_dotnet_ordinal(self):
        # System.Text.Json (Web defaults) reads the schedule status as a numeric
        # enum, so the persisted status must be its ordinal, not its name.
        state = ScheduleState()
        state.status = ScheduleStatus.ACTIVE
        state.schedule_configuration = ScheduleConfiguration.from_create_options(
            ScheduleCreationOptions(schedule_id="s1", orchestration_name="orch",
                                    interval=timedelta(seconds=5)))
        payload = state.to_json()
        assert payload["Status"] == 1
        assert payload["ExecutionToken"] == state.execution_token
        assert "status" not in payload

    def test_serialized_schedule_configuration_is_a_plain_mapping(self):
        # The persisted entity state the Durable Task Scheduler dashboard reads
        # must carry ``ScheduleConfiguration`` as a plain, .NET-shaped mapping --
        # never a Python-specific ``{"__class__": ..., "__data__": ...}``
        # envelope. ``to_json`` inlines ``ScheduleConfiguration.to_json()`` so
        # the default converter emits the mapping directly. This pins that wire
        # shape so it cannot silently regress to an object/envelope form.
        state = ScheduleState()
        state.schedule_configuration = ScheduleConfiguration.from_create_options(
            ScheduleCreationOptions(schedule_id="s1", orchestration_name="orch",
                                    interval=timedelta(seconds=5)))

        payload = state.to_json()
        nested = payload["ScheduleConfiguration"]
        assert isinstance(nested, dict)
        assert nested["ScheduleId"] == "s1"
        assert nested["OrchestrationName"] == "orch"

        wire = converter.serialize(state)
        assert wire is not None
        # No Python type metadata may leak into the persisted state.
        assert "__class__" not in wire
        assert "__module__" not in wire
        assert "__data__" not in wire

    def test_from_json_accepts_legacy_string_status(self):
        legacy = {
            "status": "Active",
            "execution_token": "token-abc",
            "schedule_configuration": {
                "schedule_id": "s1",
                "orchestration_name": "orch",
                "interval_seconds": 5.0,
            },
        }
        restored = ScheduleState.from_json(legacy)
        assert restored.status == ScheduleStatus.ACTIVE
        assert restored.execution_token == "token-abc"
        assert restored.schedule_configuration is not None
        assert restored.schedule_configuration.interval == timedelta(seconds=5)

    def test_refresh_execution_token_changes_token(self):
        state = ScheduleState()
        original = state.execution_token
        state.refresh_execution_token()
        assert state.execution_token != original

    def test_from_json_preserves_default_token_when_missing(self):
        # A payload without an execution token must not clobber the token
        # generated by ``__init__``; otherwise every run signal looks stale.
        restored = ScheduleState.from_json({"Status": 1})
        assert restored.execution_token


class TestScheduleQueryNormalization:
    def test_naive_bounds_are_coerced_to_aware_utc(self):
        q = ScheduleQuery(
            created_from=datetime(2026, 1, 1, 0, 0, 0),
            created_to=datetime(2026, 2, 1, 0, 0, 0),
        )
        assert q.created_from is not None and q.created_to is not None
        assert q.created_from == datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert q.created_to == datetime(2026, 2, 1, tzinfo=timezone.utc)
        assert q.created_from.tzinfo is timezone.utc
        assert q.created_to.tzinfo is timezone.utc

    def test_aware_bounds_are_preserved(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        q = ScheduleQuery(created_from=start)
        assert q.created_from == start

    def test_none_bounds_stay_none(self):
        q = ScheduleQuery()
        assert q.created_from is None
        assert q.created_to is None
