# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import pytest

from durabletask.grpc_options import (
    GrpcClientResiliencyOptions,
    GrpcWorkerResiliencyOptions,
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


def test_worker_resiliency_rejects_invalid_durations():
    with pytest.raises(ValueError, match="hello_timeout_seconds must be > 0"):
        GrpcWorkerResiliencyOptions(hello_timeout_seconds=0.0)

    with pytest.raises(
        ValueError,
        match=(
            "reconnect_backoff_cap_seconds must be >= "
            "reconnect_backoff_base_seconds"
        ),
    ):
        GrpcWorkerResiliencyOptions(
            reconnect_backoff_base_seconds=5.0,
            reconnect_backoff_cap_seconds=1.0,
        )


def test_client_resiliency_defaults_are_enabled():
    options = GrpcClientResiliencyOptions()

    assert options.channel_recreate_failure_threshold == 5
    assert options.min_recreate_interval_seconds == 30.0


def test_client_resiliency_rejects_negative_cooldown():
    with pytest.raises(
        ValueError, match="min_recreate_interval_seconds must be >= 0"
    ):
        GrpcClientResiliencyOptions(min_recreate_interval_seconds=-1.0)
