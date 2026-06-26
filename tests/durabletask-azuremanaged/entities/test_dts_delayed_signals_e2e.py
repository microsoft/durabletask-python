# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Live DTS E2E tests for delayed (scheduled) entity signals.

These tests assume a sidecar/emulator is running. Example command:
    docker run -i -p 8080:8080 -p 8082:8082 -d mcr.microsoft.com/dts/dts-emulator:latest
"""

import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from durabletask import entities, task
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker

pytestmark = pytest.mark.dts

taskhub_name = os.getenv("TASKHUB", "default")
endpoint = os.getenv("ENDPOINT", "http://localhost:8080")
secure_channel = endpoint.startswith("https://")


class _Recorder:
    def __init__(self):
        self._lock = threading.Lock()
        self.times: list[datetime] = []

    def record(self) -> None:
        with self._lock:
            self.times.append(datetime.now(timezone.utc))

    @property
    def count(self) -> int:
        with self._lock:
            return len(self.times)


recorder = _Recorder()


class Recorder(entities.DurableEntity):
    def ping(self, _=None):
        recorder.record()


def setup_function(_):
    recorder.times.clear()


def _wait_until(predicate, timeout: float = 30, interval: float = 0.5) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_client_delayed_signal_is_deferred():
    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=secure_channel,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_entity(Recorder)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=secure_channel,
                                       taskhub=taskhub_name, token_credential=None)
        entity_id = entities.EntityInstanceId("Recorder", f"client-delay-{uuid.uuid4().hex[:8]}")
        sent_at = datetime.now(timezone.utc)
        c.signal_entity(entity_id, "ping", signal_time=sent_at + timedelta(seconds=4))

        time.sleep(1)
        assert recorder.count == 0, "delayed signal fired too early"

        assert _wait_until(lambda: recorder.count >= 1)
        elapsed = (recorder.times[0] - sent_at).total_seconds()
        assert elapsed >= 3, f"signal fired too early ({elapsed}s)"


def test_orchestration_delayed_signal_is_deferred():
    def signaling_orchestrator(ctx: task.OrchestrationContext, key: str):
        entity_id = entities.EntityInstanceId("Recorder", key)
        ctx.signal_entity(entity_id, "ping", signal_time=ctx.current_utc_datetime + timedelta(seconds=4))

    with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=secure_channel,
                                    taskhub=taskhub_name, token_credential=None) as w:
        w.add_orchestrator(signaling_orchestrator)
        w.add_entity(Recorder)
        w.start()

        c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=secure_channel,
                                       taskhub=taskhub_name, token_credential=None)
        key = f"orch-delay-{uuid.uuid4().hex[:8]}"
        sent_at = datetime.now(timezone.utc)
        instance_id = c.schedule_new_orchestration(signaling_orchestrator, input=key)
        c.wait_for_orchestration_completion(instance_id, timeout=30)

        time.sleep(1)
        assert recorder.count == 0, "delayed signal fired too early"

        assert _wait_until(lambda: recorder.count >= 1)
        elapsed = (recorder.times[0] - sent_at).total_seconds()
        assert elapsed >= 3, f"signal fired too early ({elapsed}s)"
