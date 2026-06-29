# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""End-to-end tests for scheduled tasks against a live Durable Task Scheduler.

These tests assume a sidecar/emulator is running. Example command:
    docker run -i -p 8080:8080 -p 8082:8082 -d mcr.microsoft.com/dts/dts-emulator:latest
"""

import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from durabletask import task
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from durabletask.scheduled import (ScheduledTaskClient, ScheduleCreationOptions,
                                   ScheduleQuery, ScheduleStatus,
                                   ScheduleUpdateOptions)

import os

# NOTE: These tests assume a sidecar process is running. Example command:
#       docker run -i -p 8080:8080 -p 8082:8082 -d mcr.microsoft.com/dts/dts-emulator:latest
pytestmark = pytest.mark.dts

taskhub_name = os.getenv("TASKHUB", "default")
endpoint = os.getenv("ENDPOINT", "http://localhost:8080")
secure_channel = endpoint.startswith("https://")


class _RunTracker:
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


tracker = _RunTracker()


def target_orchestrator(ctx: task.OrchestrationContext, value):
    if not ctx.is_replaying:
        tracker.record(value)
    return value


def _make_worker() -> DurableTaskSchedulerWorker:
    w = DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=secure_channel,
                                   taskhub=taskhub_name, token_credential=None)
    w.add_orchestrator(target_orchestrator)
    w.configure_scheduled_tasks()
    return w


def _make_client() -> DurableTaskSchedulerClient:
    return DurableTaskSchedulerClient(host_address=endpoint, secure_channel=secure_channel,
                                      taskhub=taskhub_name, token_credential=None)


def _wait_until(predicate, timeout: float = 30, interval: float = 0.5) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def setup_function(_):
    tracker.inputs.clear()


def test_create_describe_and_run():
    schedule_id = f"sched-run-{uuid.uuid4().hex[:8]}"
    with _make_worker() as w:
        w.start()
        c = _make_client()
        scheduled = ScheduledTaskClient(c)
        schedule = scheduled.create_schedule(ScheduleCreationOptions(
            schedule_id=schedule_id,
            orchestration_name=task.get_name(target_orchestrator),
            interval=timedelta(seconds=1),
            orchestration_input="hello",
            start_at=datetime.now(timezone.utc),
            start_immediately_if_late=True,
        ))
        try:
            assert _wait_until(lambda: tracker.count >= 1), "target orchestration did not run"

            description = schedule.describe()
            assert description.schedule_id == schedule_id
            assert description.status == ScheduleStatus.ACTIVE
            assert description.last_run_at is not None
        finally:
            schedule.delete()


def test_pause_and_resume():
    schedule_id = f"sched-pause-{uuid.uuid4().hex[:8]}"
    with _make_worker() as w:
        w.start()
        c = _make_client()
        scheduled = ScheduledTaskClient(c)
        schedule = scheduled.create_schedule(ScheduleCreationOptions(
            schedule_id=schedule_id,
            orchestration_name=task.get_name(target_orchestrator),
            interval=timedelta(seconds=1),
            start_at=datetime.now(timezone.utc),
            start_immediately_if_late=True,
        ))
        try:
            assert _wait_until(lambda: tracker.count >= 1)

            schedule.pause()
            assert schedule.describe().status == ScheduleStatus.PAUSED

            time.sleep(3)
            count_after_pause = tracker.count
            time.sleep(3)
            assert tracker.count == count_after_pause, "schedule kept running while paused"

            schedule.resume()
            assert schedule.describe().status == ScheduleStatus.ACTIVE
            assert _wait_until(lambda: tracker.count > count_after_pause)
        finally:
            schedule.delete()


def test_update_interval_and_input():
    schedule_id = f"sched-update-{uuid.uuid4().hex[:8]}"
    with _make_worker() as w:
        w.start()
        c = _make_client()
        scheduled = ScheduledTaskClient(c)
        schedule = scheduled.create_schedule(ScheduleCreationOptions(
            schedule_id=schedule_id,
            orchestration_name=task.get_name(target_orchestrator),
            interval=timedelta(seconds=30),
            orchestration_input="before",
            start_at=datetime.now(timezone.utc) + timedelta(hours=1),
        ))
        try:
            schedule.update(ScheduleUpdateOptions(
                orchestration_input="after",
                interval=timedelta(seconds=2),
            ))

            description = schedule.describe()
            assert description.interval == timedelta(seconds=2)
            assert description.orchestration_input == "after"
        finally:
            schedule.delete()


def test_list_and_delete():
    prefix = f"grp-{uuid.uuid4().hex[:8]}-"
    with _make_worker() as w:
        w.start()
        c = _make_client()
        scheduled = ScheduledTaskClient(c)
        created = []
        for i in range(3):
            schedule_id = f"{prefix}{i}"
            scheduled.create_schedule(ScheduleCreationOptions(
                schedule_id=schedule_id,
                orchestration_name=task.get_name(target_orchestrator),
                interval=timedelta(seconds=30),
                start_at=datetime.now(timezone.utc) + timedelta(hours=1),
            ))
            created.append(schedule_id)

        try:
            listed = scheduled.list_schedules(ScheduleQuery(schedule_id_prefix=prefix))
            assert {s.schedule_id for s in listed} == set(created)
        finally:
            for schedule_id in created:
                scheduled.get_schedule_client(schedule_id).delete()

        assert _wait_until(lambda: scheduled.get_schedule(created[0]) is None)
