# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for ScheduleClient filtering and scheduled-tasks worker capability."""

from datetime import datetime, timedelta, timezone

import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask.scheduled.client import ScheduledTaskClient
from durabletask.scheduled.models import (ScheduleConfiguration,
                                          ScheduleCreationOptions, ScheduleQuery,
                                          ScheduleState)
from durabletask.scheduled.schedule_status import ScheduleStatus
from durabletask.worker import TaskHubGrpcWorker


def _state_created_at(created_at: datetime) -> ScheduleState:
    state = ScheduleState()
    state.status = ScheduleStatus.ACTIVE
    state.schedule_created_at = created_at
    state.schedule_configuration = ScheduleConfiguration.from_create_options(
        ScheduleCreationOptions(schedule_id="s1", orchestration_name="orch",
                                interval=timedelta(seconds=5)))
    return state


class TestMatchesFilter:
    def test_naive_bound_does_not_crash_against_aware_created_at(self):
        state = _state_created_at(datetime(2026, 1, 15, tzinfo=timezone.utc))
        # A naive bound is normalized by ScheduleQuery, so the comparison must
        # not raise "can't compare offset-naive and offset-aware".
        q = ScheduleQuery(created_from=datetime(2026, 1, 1, 0, 0, 0))
        assert ScheduledTaskClient._matches_filter(state, q) is True

    def test_created_from_is_exclusive(self):
        # Bounds match .NET's exclusive semantics: a schedule created exactly at
        # the bound is excluded.
        boundary = datetime(2026, 1, 1, tzinfo=timezone.utc)
        state = _state_created_at(boundary)
        q = ScheduleQuery(created_from=boundary)
        assert ScheduledTaskClient._matches_filter(state, q) is False

    def test_created_to_is_exclusive(self):
        boundary = datetime(2026, 1, 1, tzinfo=timezone.utc)
        state = _state_created_at(boundary)
        q = ScheduleQuery(created_to=boundary)
        assert ScheduledTaskClient._matches_filter(state, q) is False

    def test_inside_window_matches(self):
        state = _state_created_at(datetime(2026, 1, 15, tzinfo=timezone.utc))
        q = ScheduleQuery(
            created_from=datetime(2026, 1, 1, tzinfo=timezone.utc),
            created_to=datetime(2026, 2, 1, tzinfo=timezone.utc),
        )
        assert ScheduledTaskClient._matches_filter(state, q) is True

    def test_outside_window_is_excluded(self):
        state = _state_created_at(datetime(2026, 3, 1, tzinfo=timezone.utc))
        q = ScheduleQuery(created_to=datetime(2026, 2, 1, tzinfo=timezone.utc))
        assert ScheduledTaskClient._matches_filter(state, q) is False

    def test_status_filter(self):
        state = _state_created_at(datetime(2026, 1, 1, tzinfo=timezone.utc))
        assert ScheduledTaskClient._matches_filter(state, ScheduleQuery(status=ScheduleStatus.ACTIVE)) is True
        assert ScheduledTaskClient._matches_filter(state, ScheduleQuery(status=ScheduleStatus.PAUSED)) is False


class TestScheduledTasksCapability:
    def test_configure_advertises_scheduled_tasks_capability(self):
        worker = TaskHubGrpcWorker()
        worker.configure_scheduled_tasks()
        assert pb.WORKER_CAPABILITY_SCHEDULED_TASKS in worker._capabilities  # pyright: ignore[reportPrivateUsage]

    def test_capability_absent_by_default(self):
        worker = TaskHubGrpcWorker()
        assert pb.WORKER_CAPABILITY_SCHEDULED_TASKS not in worker._capabilities  # pyright: ignore[reportPrivateUsage]

    def test_add_capability_rejected_while_running(self):
        import pytest
        worker = TaskHubGrpcWorker()
        worker._is_running = True  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(RuntimeError):
            worker.add_capability(pb.WORKER_CAPABILITY_SCHEDULED_TASKS)
