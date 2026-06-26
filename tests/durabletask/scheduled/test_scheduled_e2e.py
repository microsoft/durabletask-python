# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""End-to-end tests for scheduled tasks against the in-memory backend."""

import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from durabletask import client, task, worker
from durabletask.scheduled import (ScheduledTaskClient, ScheduleCreationOptions,
                                   ScheduleQuery, ScheduleStatus,
                                   ScheduleUpdateOptions,
                                   configure_scheduled_tasks)
from durabletask.testing import create_test_backend

from tests.durabletask._port_utils import find_free_port

PORT = find_free_port()
HOST = f"localhost:{PORT}"


@pytest.fixture(autouse=True)
def backend():
    """Create an in-memory backend for each test."""
    b = create_test_backend(port=PORT)
    yield b
    b.stop()
    b.reset()


class _RunTracker:
    """Thread-safe tracker that records each target orchestration run."""

    def __init__(self):
        self._lock = threading.Lock()
        self.inputs: list[object] = []

    def record(self, value: object) -> None:
        with self._lock:
            self.inputs.append(value)

    @property
    def count(self) -> int:
        with self._lock:
            return len(self.inputs)


# A module-level tracker is used because orchestrators must be registered by
# reference and run on worker threads.
tracker = _RunTracker()


def target_orchestrator(ctx: task.OrchestrationContext, value):
    """The orchestration started on each schedule run."""
    if not ctx.is_replaying:
        tracker.record(value)
    return value


def _make_worker() -> worker.TaskHubGrpcWorker:
    w = worker.TaskHubGrpcWorker(host_address=HOST)
    w.add_orchestrator(target_orchestrator)
    configure_scheduled_tasks(w)
    return w


def _wait_until(predicate, timeout: float = 15, interval: float = 0.2) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def setup_function(_):
    # Reset the shared tracker before each test.
    tracker.inputs.clear()


def test_create_describe_and_run():
    with _make_worker() as w:
        w.start()
        with client.TaskHubGrpcClient(host_address=HOST) as c:
            scheduled = ScheduledTaskClient(c)
            schedule = scheduled.create_schedule(ScheduleCreationOptions(
                schedule_id="sched-run",
                orchestration_name=task.get_name(target_orchestrator),
                interval=timedelta(seconds=1),
                orchestration_input="hello",
                start_at=datetime.now(timezone.utc),
                start_immediately_if_late=True,
            ))

            assert _wait_until(lambda: tracker.count >= 1), "target orchestration did not run"

            description = schedule.describe()
            assert description.schedule_id == "sched-run"
            assert description.status == ScheduleStatus.ACTIVE
            assert description.orchestration_name == task.get_name(target_orchestrator)
            assert description.last_run_at is not None

            schedule.delete()


def test_get_nonexistent_returns_none():
    with _make_worker() as w:
        w.start()
        with client.TaskHubGrpcClient(host_address=HOST) as c:
            scheduled = ScheduledTaskClient(c)
            assert scheduled.get_schedule("does-not-exist") is None


def test_pause_and_resume():
    with _make_worker() as w:
        w.start()
        with client.TaskHubGrpcClient(host_address=HOST) as c:
            scheduled = ScheduledTaskClient(c)
            schedule = scheduled.create_schedule(ScheduleCreationOptions(
                schedule_id="sched-pause",
                orchestration_name=task.get_name(target_orchestrator),
                interval=timedelta(seconds=1),
                start_at=datetime.now(timezone.utc),
                start_immediately_if_late=True,
            ))

            assert _wait_until(lambda: tracker.count >= 1)

            schedule.pause()
            assert schedule.describe().status == ScheduleStatus.PAUSED

            # After pausing, no further runs should occur.
            time.sleep(2)
            count_after_pause = tracker.count
            time.sleep(2)
            assert tracker.count == count_after_pause, "schedule kept running while paused"

            schedule.resume()
            assert schedule.describe().status == ScheduleStatus.ACTIVE
            assert _wait_until(lambda: tracker.count > count_after_pause)

            schedule.delete()


def test_update_interval_and_input():
    with _make_worker() as w:
        w.start()
        with client.TaskHubGrpcClient(host_address=HOST) as c:
            scheduled = ScheduledTaskClient(c)
            schedule = scheduled.create_schedule(ScheduleCreationOptions(
                schedule_id="sched-update",
                orchestration_name=task.get_name(target_orchestrator),
                interval=timedelta(seconds=30),
                orchestration_input="before",
                start_at=datetime.now(timezone.utc) + timedelta(hours=1),
            ))

            schedule.update(ScheduleUpdateOptions(
                orchestration_input="after",
                interval=timedelta(seconds=2),
            ))

            description = schedule.describe()
            assert description.interval == timedelta(seconds=2)
            assert description.orchestration_input == "after"

            schedule.delete()


def test_list_schedules_with_prefix_and_status():
    with _make_worker() as w:
        w.start()
        with client.TaskHubGrpcClient(host_address=HOST) as c:
            scheduled = ScheduledTaskClient(c)
            for i in range(3):
                scheduled.create_schedule(ScheduleCreationOptions(
                    schedule_id=f"group-a-{i}",
                    orchestration_name=task.get_name(target_orchestrator),
                    interval=timedelta(seconds=30),
                    start_at=datetime.now(timezone.utc) + timedelta(hours=1),
                ))
            scheduled.create_schedule(ScheduleCreationOptions(
                schedule_id="group-b-0",
                orchestration_name=task.get_name(target_orchestrator),
                interval=timedelta(seconds=30),
                start_at=datetime.now(timezone.utc) + timedelta(hours=1),
            ))

            all_schedules = scheduled.list_schedules()
            ids = {s.schedule_id for s in all_schedules}
            assert {"group-a-0", "group-a-1", "group-a-2", "group-b-0"}.issubset(ids)

            group_a = scheduled.list_schedules(ScheduleQuery(schedule_id_prefix="group-a-"))
            assert {s.schedule_id for s in group_a} == {"group-a-0", "group-a-1", "group-a-2"}

            active = scheduled.list_schedules(ScheduleQuery(status=ScheduleStatus.ACTIVE))
            assert all(s.status == ScheduleStatus.ACTIVE for s in active)

            for s in all_schedules:
                scheduled.get_schedule_client(s.schedule_id).delete()


def test_delete_removes_schedule():
    with _make_worker() as w:
        w.start()
        with client.TaskHubGrpcClient(host_address=HOST) as c:
            scheduled = ScheduledTaskClient(c)
            schedule = scheduled.create_schedule(ScheduleCreationOptions(
                schedule_id="sched-delete",
                orchestration_name=task.get_name(target_orchestrator),
                interval=timedelta(seconds=30),
                start_at=datetime.now(timezone.utc) + timedelta(hours=1),
            ))

            assert scheduled.get_schedule("sched-delete") is not None

            schedule.delete()
            assert _wait_until(lambda: scheduled.get_schedule("sched-delete") is None)
