# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from datetime import datetime, timedelta
from typing import Any

import pytest

from durabletask import task
from durabletask.internal import orchestrator_service_pb2 as pb

START = datetime(2020, 1, 1)


def _delay_at_attempt(policy: task.RetryPolicy, attempt: int) -> timedelta | None:
    retry = task.RetryableTask[Any](policy, pb.OrchestratorAction(), START, is_sub_orch=False)
    for _ in range(attempt - 1):
        retry.increment_attempt_count()
    return retry.compute_next_delay(START)


@pytest.mark.parametrize(("first", "expected_cap"), [
    (timedelta(seconds=1), timedelta(hours=1)),
    (timedelta(hours=6), timedelta(hours=6)),
])
def test_omitted_max_retry_interval_defaults_to_one_hour_or_first_interval(
    first: timedelta, expected_cap: timedelta,
) -> None:
    policy = task.RetryPolicy(first_retry_interval=first, max_number_of_attempts=30, backoff_coefficient=2)

    assert policy.max_retry_interval == expected_cap
    assert _delay_at_attempt(policy, 1) == first
    assert _delay_at_attempt(policy, 20) == expected_cap


def test_explicit_none_max_retry_interval_is_uncapped() -> None:
    policy = task.RetryPolicy(
        first_retry_interval=timedelta(seconds=1),
        max_number_of_attempts=30,
        backoff_coefficient=2,
        max_retry_interval=None,
    )

    assert policy.max_retry_interval is None
    assert _delay_at_attempt(policy, 20) == timedelta(seconds=2 ** 19)


@pytest.mark.parametrize(("coefficient", "attempt"), [(2.0, 1100), (1e200, 3)])
@pytest.mark.parametrize("cap", [timedelta(milliseconds=1), timedelta(0)])
def test_capped_backoff_does_not_overflow(coefficient: float, attempt: int, cap: timedelta) -> None:
    policy = task.RetryPolicy(
        first_retry_interval=timedelta(milliseconds=1),
        max_number_of_attempts=attempt + 1,
        backoff_coefficient=coefficient,
        max_retry_interval=cap,
    )

    assert _delay_at_attempt(policy, attempt) == cap


def test_zero_first_interval_with_overflowing_backoff_is_zero() -> None:
    policy = task.RetryPolicy(
        first_retry_interval=timedelta(0),
        max_number_of_attempts=5,
        backoff_coefficient=1e200,
        max_retry_interval=None,
    )

    assert _delay_at_attempt(policy, 3) == timedelta(0)
