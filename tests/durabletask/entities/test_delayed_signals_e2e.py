# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for delayed (scheduled) entity signals via the client and orchestrator."""

import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from durabletask import client, entities, task, worker
from durabletask.testing import create_test_backend

from tests.durabletask._port_utils import find_free_port

PORT = find_free_port()
HOST = f"localhost:{PORT}"


@pytest.fixture(autouse=True)
def backend():
    b = create_test_backend(port=PORT)
    yield b
    b.stop()
    b.reset()


class _Recorder:
    """Thread-safe recorder of operation invocation times."""

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


def _wait_until(predicate, timeout: float = 10, interval: float = 0.1) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_client_delayed_signal_is_deferred():
    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_entity(Recorder)
        w.start()

        with client.TaskHubGrpcClient(host_address=HOST) as c:
            entity_id = entities.EntityInstanceId("Recorder", "client-delay")
            signal_at = datetime.now(timezone.utc) + timedelta(seconds=2)
            sent_at = datetime.now(timezone.utc)
            c.signal_entity(entity_id, "ping", signal_time=signal_at)

            # Should not have fired immediately.
            time.sleep(0.5)
            assert recorder.count == 0, "delayed signal fired too early"

            # Should fire around the scheduled time.
            assert _wait_until(lambda: recorder.count >= 1)
            elapsed = (recorder.times[0] - sent_at).total_seconds()
            assert elapsed >= 1.5, f"signal fired too early ({elapsed}s)"


def test_client_immediate_signal_still_works():
    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_entity(Recorder)
        w.start()

        with client.TaskHubGrpcClient(host_address=HOST) as c:
            entity_id = entities.EntityInstanceId("Recorder", "client-now")
            c.signal_entity(entity_id, "ping")
            assert _wait_until(lambda: recorder.count >= 1)


def test_orchestration_delayed_signal_is_deferred():
    def signaling_orchestrator(ctx: task.OrchestrationContext, _):
        entity_id = entities.EntityInstanceId("Recorder", "orch-delay")
        signal_at = ctx.current_utc_datetime + timedelta(seconds=2)
        ctx.signal_entity(entity_id, "ping", signal_time=signal_at)

    with worker.TaskHubGrpcWorker(host_address=HOST) as w:
        w.add_orchestrator(signaling_orchestrator)
        w.add_entity(Recorder)
        w.start()

        with client.TaskHubGrpcClient(host_address=HOST) as c:
            sent_at = datetime.now(timezone.utc)
            instance_id = c.schedule_new_orchestration(signaling_orchestrator)
            c.wait_for_orchestration_completion(instance_id, timeout=30)

            # Orchestration completes immediately, but the signal should be deferred.
            time.sleep(0.5)
            assert recorder.count == 0, "delayed signal fired too early"

            assert _wait_until(lambda: recorder.count >= 1)
            elapsed = (recorder.times[0] - sent_at).total_seconds()
            assert elapsed >= 1.5, f"signal fired too early ({elapsed}s)"
