# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
from collections.abc import Generator
from datetime import datetime, timedelta
from typing import Any

import pytest

from durabletask import task, worker
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.serialization import JsonDataConverter

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


def test_explicit_none_replays_recorded_uncapped_split_retry_timers() -> None:
    """Histories recorded with the previous uncapped default replay unchanged with ``None``."""
    hour = timedelta(hours=1)

    def orchestrator(ctx: task.OrchestrationContext, _: Any) -> Generator[task.Task[Any], Any, Any]:
        return (yield ctx.call_activity("flaky", retry_policy=task.RetryPolicy(
            first_retry_interval=hour,
            max_number_of_attempts=3,
            backoff_coefficient=100,
            max_retry_interval=None,
        )))

    registry = worker._Registry()  # pyright: ignore[reportPrivateUsage]
    name = registry.add_orchestrator(orchestrator)
    retried_at = START + hour
    # The second retry waited 100 hours uncapped, split into 72-hour and 28-hour timers.
    history = [
        helpers.new_orchestrator_started_event(START),
        helpers.new_execution_started_event(name, "instance", encoded_input=None),
        helpers.new_task_scheduled_event(1, "flaky"),
        helpers.new_task_failed_event(1, ValueError("transient")),
        helpers.new_timer_created_event(2, retried_at),
        helpers.new_orchestrator_started_event(retried_at),
        helpers.new_timer_fired_event(2, retried_at),
        helpers.new_task_scheduled_event(1, "flaky"),
        helpers.new_task_failed_event(1, ValueError("transient")),
        helpers.new_timer_created_event(3, retried_at + 72 * hour),
    ]

    def execute(new_events: list[pb.HistoryEvent]) -> list[pb.OrchestratorAction]:
        executor = worker._OrchestrationExecutor(  # pyright: ignore[reportPrivateUsage]
            registry, logging.getLogger(__name__), JsonDataConverter())
        return list(executor.execute("instance", history, new_events).actions)

    first_part_fired = [
        helpers.new_orchestrator_started_event(retried_at + 72 * hour),
        helpers.new_timer_fired_event(3, retried_at + 72 * hour),
    ]
    actions = execute(first_part_fired)
    assert [a.WhichOneof("orchestratorActionType") for a in actions] == ["createTimer"]
    assert actions[0].id == 4
    assert actions[0].createTimer.fireAt.ToDatetime() == retried_at + 100 * hour

    history += first_part_fired + [helpers.new_timer_created_event(4, retried_at + 100 * hour)]
    actions = execute([
        helpers.new_orchestrator_started_event(retried_at + 100 * hour),
        helpers.new_timer_fired_event(4, retried_at + 100 * hour),
    ])
    assert [(a.id, a.WhichOneof("orchestratorActionType")) for a in actions] == [(1, "scheduleTask")]


def test_zero_first_interval_with_overflowing_backoff_is_zero() -> None:
    policy = task.RetryPolicy(
        first_retry_interval=timedelta(0),
        max_number_of_attempts=5,
        backoff_coefficient=1e200,
        max_retry_interval=None,
    )

    assert _delay_at_attempt(policy, 3) == timedelta(0)
