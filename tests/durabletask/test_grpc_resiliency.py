# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import grpc
import pytest

from durabletask.grpc_options import (
    GrpcClientResiliencyOptions,
    GrpcWorkerResiliencyOptions,
)
from durabletask.internal.grpc_resiliency import (
    FailureTracker,
    get_full_jitter_delay_seconds,
    is_client_transport_failure,
    is_worker_transport_failure,
)


def test_worker_resiliency_defaults_are_enabled():
    options = GrpcWorkerResiliencyOptions()

    assert options.hello_timeout_seconds == 30.0
    assert options.silent_disconnect_timeout_seconds == 120.0
    assert options.channel_recreate_failure_threshold == 5
    assert options.reconnect_backoff_base_seconds == 1.0
    assert options.reconnect_backoff_cap_seconds == 30.0


def test_worker_resiliency_allows_disabling_timeout_and_threshold():
    options = GrpcWorkerResiliencyOptions(
        silent_disconnect_timeout_seconds=0.0,
        channel_recreate_failure_threshold=0,
    )

    assert options.silent_disconnect_timeout_seconds == 0.0
    assert options.channel_recreate_failure_threshold == 0


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"hello_timeout_seconds": 0.0}, "hello_timeout_seconds must be > 0"),
        (
            {"silent_disconnect_timeout_seconds": -1.0},
            "silent_disconnect_timeout_seconds must be >= 0",
        ),
        (
            {"channel_recreate_failure_threshold": -1},
            "channel_recreate_failure_threshold must be >= 0",
        ),
        (
            {"reconnect_backoff_base_seconds": 0.0},
            "reconnect_backoff_base_seconds must be > 0",
        ),
        (
            {"reconnect_backoff_cap_seconds": 0.0},
            "reconnect_backoff_cap_seconds must be > 0",
        ),
        (
            {
                "reconnect_backoff_base_seconds": 5.0,
                "reconnect_backoff_cap_seconds": 1.0,
            },
            "reconnect_backoff_cap_seconds must be >= "
            "reconnect_backoff_base_seconds",
        ),
    ],
)
def test_worker_resiliency_rejects_invalid_values(kwargs, message):
    with pytest.raises(ValueError, match=message):
        GrpcWorkerResiliencyOptions(**kwargs)


def test_client_resiliency_defaults_are_enabled():
    options = GrpcClientResiliencyOptions()

    assert options.channel_recreate_failure_threshold == 5
    assert options.min_recreate_interval_seconds == 30.0


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {"channel_recreate_failure_threshold": -1},
            "channel_recreate_failure_threshold must be >= 0",
        ),
        (
            {"min_recreate_interval_seconds": -1.0},
            "min_recreate_interval_seconds must be >= 0",
        ),
    ],
)
def test_client_resiliency_rejects_invalid_values(kwargs, message):
    with pytest.raises(ValueError, match=message):
        GrpcClientResiliencyOptions(**kwargs)


def test_full_jitter_delay_is_capped(monkeypatch):
    monkeypatch.setattr(
        "durabletask.internal.grpc_resiliency.random.random",
        lambda: 1.0,
    )

    delay = get_full_jitter_delay_seconds(
        10,
        base_seconds=1.0,
        cap_seconds=30.0,
    )

    assert delay == 30.0


def test_failure_tracker_trips_at_threshold():
    tracker = FailureTracker(threshold=3)

    assert tracker.record_failure() is False
    assert tracker.record_failure() is False
    assert tracker.record_failure() is True

    tracker.record_success()

    assert tracker.consecutive_failures == 0


def test_client_transport_failure_ignores_long_poll_deadlines():
    assert (
        is_client_transport_failure(
            "WaitForInstanceStart",
            grpc.StatusCode.DEADLINE_EXCEEDED,
        )
        is False
    )
    assert (
        is_client_transport_failure(
            "StartInstance",
            grpc.StatusCode.DEADLINE_EXCEEDED,
        )
        is True
    )
    assert (
        is_client_transport_failure(
            "GetInstance",
            grpc.StatusCode.UNAVAILABLE,
        )
        is True
    )


def test_worker_transport_failure_filters_application_errors():
    assert is_worker_transport_failure(grpc.StatusCode.UNAVAILABLE) is True
    assert is_worker_transport_failure(grpc.StatusCode.DEADLINE_EXCEEDED) is True
    assert is_worker_transport_failure(grpc.StatusCode.UNAUTHENTICATED) is False
    assert is_worker_transport_failure(grpc.StatusCode.NOT_FOUND) is False
