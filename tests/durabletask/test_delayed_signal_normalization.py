# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for delayed-signal datetime normalization and in-memory backend
timer lifecycle (regression coverage for PR #160 review feedback)."""

import time
from datetime import datetime, timedelta, timezone

import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask.entities import EntityInstanceId
from durabletask.internal import helpers
from durabletask.internal.client_helpers import build_signal_entity_req
from durabletask.testing import create_test_backend

from tests.durabletask._port_utils import find_free_port


class TestSignalTimeNormalization:
    def test_build_signal_entity_req_naive_matches_aware(self):
        entity_id = EntityInstanceId("Recorder", "k")
        naive = datetime(2030, 1, 1, 12, 0, 0)
        aware = datetime(2030, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        req_naive = build_signal_entity_req(entity_id, "ping", signal_time=naive)
        req_aware = build_signal_entity_req(entity_id, "ping", signal_time=aware)

        assert req_naive.scheduledTime.seconds == req_aware.scheduledTime.seconds

    def test_new_signal_entity_action_naive_matches_aware(self):
        entity_id = EntityInstanceId("Recorder", "k")
        naive = datetime(2030, 1, 1, 12, 0, 0)
        aware = datetime(2030, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        a_naive = helpers.new_signal_entity_action(1, entity_id, "ping", None, "r1", naive)
        a_aware = helpers.new_signal_entity_action(1, entity_id, "ping", None, "r2", aware)

        naive_ts = a_naive.sendEntityMessage.entityOperationSignaled.scheduledTime
        aware_ts = a_aware.sendEntityMessage.entityOperationSignaled.scheduledTime
        assert naive_ts.seconds == aware_ts.seconds


class TestDelayedTimerLifecycle:
    def test_delayed_operation_does_not_fire_into_reset_backend(self):
        backend = create_test_backend(port=find_free_port())
        try:
            entity_id = "@recorder@k"
            event = pb.HistoryEvent(
                eventId=-1,
                entityOperationSignaled=pb.EntityOperationSignaledEvent(operation="ping"),
            )
            # Schedule the op to fire shortly in the future, then reset before it
            # fires so the timer wakes into a reset (new-generation) backend.
            fire_at = datetime.now(timezone.utc) + timedelta(seconds=0.3)
            backend._schedule_delayed_entity_operation(  # pyright: ignore[reportPrivateUsage]
                entity_id, event, fire_at)
            backend.reset()

            # Give the timer thread time to wake; it must observe the new
            # generation and refuse to recreate state in the reset backend.
            time.sleep(0.6)
            assert backend._entities == {}  # pyright: ignore[reportPrivateUsage]
        finally:
            backend.stop()
            backend.reset()
