# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json

import pytest

from durabletask.scheduled import ScheduleInvalidTransitionError, ScheduleStatus


@pytest.mark.parametrize(("status", "name", "value", "ordinal"), [
    (ScheduleStatus.UNINITIALIZED, "UNINITIALIZED", "Uninitialized", 0),
    (ScheduleStatus.ACTIVE, "ACTIVE", "Active", 1),
    (ScheduleStatus.PAUSED, "PAUSED", "Paused", 2),
])
def test_status_preserves_string_formatting_and_conversions(
    status: ScheduleStatus, name: str, value: str, ordinal: int
) -> None:
    formatted = f"ScheduleStatus.{name}"
    assert isinstance(status, str)
    assert status == value
    assert str(status) == formatted
    assert f"{status}" == formatted
    assert format(status, ">30") == formatted.rjust(30)
    assert json.dumps(status) == json.dumps(value)
    assert ScheduleStatus(value) is status
    assert status.to_dotnet_ordinal() == ordinal
    assert ScheduleStatus.from_dotnet(ordinal) is status
    assert ScheduleStatus.from_dotnet(value) is status


def test_transition_error_preserves_status_formatting() -> None:
    error = ScheduleInvalidTransitionError(
        "schedule", ScheduleStatus.PAUSED, ScheduleStatus.PAUSED, "pause_schedule")

    assert str(error) == (
        "Invalid state transition for schedule 'schedule': operation "
        "'pause_schedule' cannot transition from 'ScheduleStatus.PAUSED' "
        "to 'ScheduleStatus.PAUSED'."
    )
