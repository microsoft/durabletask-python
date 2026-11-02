# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# pyright: reportPrivateUsage=false

import logging
from collections.abc import Generator
from datetime import UTC, datetime, timedelta, timezone, tzinfo
from typing import Any

import pytest

from durabletask import task, worker
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.serialization import JsonDataConverter


def _set_wall_clock(monkeypatch: pytest.MonkeyPatch, instant: datetime) -> None:
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            return instant.astimezone(tz) if tz is not None else instant.replace(tzinfo=None)

    monkeypatch.setattr(task, "datetime", FrozenDateTime)


@pytest.mark.parametrize("start_zone", [None, UTC, timezone(timedelta(hours=2))])
@pytest.mark.parametrize("current_zone", [None, UTC, timezone(timedelta(hours=-3))])
@pytest.mark.parametrize("elapsed", [9, 10, 11])
def test_retry_timeout_uses_supplied_time(
    start_zone: tzinfo | None, current_zone: tzinfo | None, elapsed: int,
) -> None:
    start = datetime(2020, 1, 1, tzinfo=UTC)
    current = start + timedelta(seconds=elapsed)
    start = start.astimezone(start_zone) if start_zone is not None else start.replace(tzinfo=None)
    current = current.astimezone(current_zone) if current_zone is not None else current.replace(tzinfo=None)
    retry = task.RetryableTask[Any](
        task.RetryPolicy(
            first_retry_interval=timedelta(seconds=1),
            max_number_of_attempts=3,
            retry_timeout=timedelta(seconds=10),
        ),
        pb.OrchestratorAction(),
        start,
        is_sub_orch=False,
    )

    assert retry.compute_next_delay(current) == (timedelta(seconds=1) if elapsed < 10 else None)


@pytest.mark.parametrize("timeout", [None, timedelta(seconds=10)])
@pytest.mark.parametrize("aware_start", [False, True])
@pytest.mark.parametrize("elapsed", [1, 10])
def test_retry_timeout_default_clock(
    monkeypatch: pytest.MonkeyPatch, timeout: timedelta | None, aware_start: bool, elapsed: int,
) -> None:
    start = datetime(2020, 1, 1, tzinfo=UTC)
    _set_wall_clock(monkeypatch, start + timedelta(seconds=elapsed))
    retry = task.RetryableTask[Any](
        task.RetryPolicy(
            first_retry_interval=timedelta(seconds=1),
            max_number_of_attempts=3,
            retry_timeout=timeout,
        ),
        pb.OrchestratorAction(),
        start if aware_start else start.replace(tzinfo=None),
        is_sub_orch=False,
    )

    expected = None if timeout is not None and elapsed >= 10 else timedelta(seconds=1)
    assert retry.compute_next_delay() == expected
    assert retry.compute_next_delay(start + timedelta(seconds=elapsed)) == expected


@pytest.mark.parametrize("timeout", [timedelta(0), timedelta.max])
def test_retry_timeout_extreme_durations(timeout: timedelta) -> None:
    start = datetime(2020, 1, 1)
    retry = task.RetryableTask[Any](
        task.RetryPolicy(
            first_retry_interval=timedelta(seconds=1),
            max_number_of_attempts=3,
            retry_timeout=timeout,
        ),
        pb.OrchestratorAction(),
        start,
        is_sub_orch=False,
    )

    assert retry.compute_next_delay(start) == (None if timeout == timedelta(0) else timedelta(seconds=1))


@pytest.mark.parametrize("is_sub_orch", [False, True], ids=["activity", "sub-orchestration"])
@pytest.mark.parametrize("elapsed", [1, 9, 10, 11])
@pytest.mark.parametrize("wall_clock_year", [2019, 2030])
def test_retry_timeout_replays_history(
    monkeypatch: pytest.MonkeyPatch, is_sub_orch: bool, elapsed: int, wall_clock_year: int,
) -> None:
    _set_wall_clock(monkeypatch, datetime(wall_clock_year, 1, 1, tzinfo=UTC))
    start = datetime(2020, 1, 1)
    failed_at = start + timedelta(seconds=elapsed)

    def orchestrator(ctx: task.OrchestrationContext, _: Any) -> Generator[task.Task[Any], Any, Any]:
        retry_policy = task.RetryPolicy(
            first_retry_interval=timedelta(seconds=1),
            max_number_of_attempts=3,
            retry_timeout=timedelta(seconds=10),
        )
        if is_sub_orch:
            return (yield ctx.call_sub_orchestrator("child", retry_policy=retry_policy))
        return (yield ctx.call_activity("child", retry_policy=retry_policy))

    registry = worker._Registry()
    name = registry.add_orchestrator(orchestrator)
    history = [
        helpers.new_orchestrator_started_event(start),
        helpers.new_execution_started_event(name, "instance", encoded_input=None),
        helpers.new_sub_orchestration_created_event(1, "child", "child-instance")
        if is_sub_orch else helpers.new_task_scheduled_event(1, "child"),
    ]
    failure_events = [
        helpers.new_orchestrator_started_event(failed_at),
        helpers.new_sub_orchestration_failed_event(1, ValueError("transient"))
        if is_sub_orch else helpers.new_task_failed_event(1, ValueError("transient")),
    ]

    def execute(old: list[pb.HistoryEvent], new: list[pb.HistoryEvent]) -> worker.ExecutionResults:
        executor = worker._OrchestrationExecutor(registry, logging.getLogger(__name__), JsonDataConverter())
        return executor.execute("instance", old, new)

    result = execute(history, failure_events)
    replayed = execute(history + failure_events, [helpers.new_orchestrator_started_event(failed_at)])
    assert len(result.actions) == 1
    assert len(replayed.actions) == 1

    if elapsed >= 10:
        for action in (result.actions[0], replayed.actions[0]):
            assert action.HasField("completeOrchestration")
            completion = action.completeOrchestration
            assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
            assert completion.failureDetails.innerFailure.errorMessage == "transient"
        return

    assert replayed.actions == result.actions
    timer = result.actions[0]
    assert timer.HasField("createTimer")
    fire_at = failed_at + timedelta(seconds=1)
    assert timer.createTimer.fireAt.ToDatetime() == fire_at
    history += failure_events + [helpers.new_timer_created_event(timer.id, fire_at)]
    timer_events = [
        helpers.new_orchestrator_started_event(fire_at),
        helpers.new_timer_fired_event(timer.id, fire_at),
    ]
    result = execute(history, timer_events)
    retries = [
        action for action in result.actions
        if action.HasField("createSubOrchestration" if is_sub_orch else "scheduleTask")
    ]
    assert len(retries) == 1
    assert retries[0].id == 1

    history += timer_events + [
        helpers.new_sub_orchestration_created_event(1, "child", "child-instance")
        if is_sub_orch else helpers.new_task_scheduled_event(1, "child"),
    ]
    result = execute(history, [
        helpers.new_orchestrator_started_event(fire_at),
        helpers.new_sub_orchestration_completed_event(1, '"success"')
        if is_sub_orch else helpers.new_task_completed_event(1, '"success"'),
    ])
    assert len(result.actions) == 1
    assert result.actions[0].completeOrchestration.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert result.actions[0].completeOrchestration.result.value == '"success"'
