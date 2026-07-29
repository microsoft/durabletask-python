# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for advanced durabletask-native entity patterns.

Covers the entity patterns exercised by the durabletask entity e2e suite that
go beyond basic call/signal: entity locking (``ctx.lock_entities``) including
lock release on failure, entity-to-entity signalling, an entity starting a new
orchestration, and entity-operation failure propagation (handled and
unhandled).
"""

import time

import pytest

pytestmark = pytest.mark.functions_e2e


def test_entity_lock_critical_section(dtask_app):
    key = f"lock-{int(time.time() * 1000)}"
    instance_id = dtask_app.start_orchestration("lock_and_increment", body=key)
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    assert status["output"] == 5


def test_entity_lock_released_on_throw(dtask_app):
    key = f"lock-throw-{int(time.time() * 1000)}"

    # First orchestration locks the entity, mutates it (+1), then throws.
    failing_id = dtask_app.start_orchestration("lock_then_throw", body=key)
    failed = dtask_app.wait_for_completion(failing_id)
    assert failed["runtimeStatus"] == "FAILED"

    # If the lock leaked, this second lock+increment would never complete.
    ok_id = dtask_app.start_orchestration("lock_and_increment", body=key)
    ok = dtask_app.wait_for_completion(ok_id)
    assert ok["runtimeStatus"] == "COMPLETED"
    # 1 (from the failed run) + 5 (from this run) == 6.
    assert ok["output"] == 6


def test_entity_signals_entity(dtask_app):
    key = f"relay-signal-{int(time.time() * 1000)}"
    instance_id = dtask_app.start_orchestration("relay_signal", body=key)
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"

    payload = dtask_app.wait_for_entity(
        "counter", key, lambda p: p["exists"] and p["state"] == 7)
    assert payload["state"] == 7


def test_entity_starts_orchestration(dtask_app):
    key = f"relay-start-{int(time.time() * 1000)}"
    instance_id = dtask_app.start_orchestration("relay_start_orch", body=key)
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"

    # The Relay entity scheduled the ``signal_counter`` orchestration, which
    # signals the counter entity to add 10.
    payload = dtask_app.wait_for_entity(
        "counter", key, lambda p: p["exists"] and p["state"] == 10)
    assert payload["state"] == 10


def test_call_failing_entity_fails_orchestration(dtask_app):
    key = f"boom-{int(time.time() * 1000)}"
    instance_id = dtask_app.start_orchestration("call_failing_entity", body=key)
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "FAILED"


def test_call_failing_entity_handled(dtask_app):
    key = f"boom-handled-{int(time.time() * 1000)}"
    instance_id = dtask_app.start_orchestration("call_failing_entity_handled", body=key)
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    assert status["output"]["caught"] is True


def test_client_delayed_signal_is_deferred(dtask_app):
    key = f"client-delay-{int(time.time() * 1000)}"
    # The fractional offset accounts for Azure Queue's one-second visibility
    # precision and keeps dispatch within Core's 100 ms early-delivery window.
    dtask_app.signal_entity("counter", key, "add", input=9, delay_seconds=3.1)

    # It must not be delivered immediately.
    time.sleep(1)
    early = dtask_app.read_entity("counter", key)
    assert not (early["exists"] and early["state"] == 9), (
        f"delayed signal fired too early: {early}")

    # It should be delivered after the delay elapses.
    payload = dtask_app.wait_for_entity(
        "counter", key, lambda p: p["exists"] and p["state"] == 9)
    assert payload["state"] == 9


def test_orchestration_delayed_signal_is_deferred(dtask_app):
    key = f"orch-delay-{int(time.time() * 1000)}"
    instance_id = dtask_app.start_orchestration("signal_counter_delayed", body=key)
    # The orchestration completes immediately; the signal is deferred.
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"

    early = dtask_app.read_entity("counter", key)
    assert not (early["exists"] and early["state"] == 4), (
        f"delayed signal fired too early: {early}")

    payload = dtask_app.wait_for_entity(
        "counter", key, lambda p: p["exists"] and p["state"] == 4)
    assert payload["state"] == 4
