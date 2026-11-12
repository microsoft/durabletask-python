# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Generator orchestrators must keep replaying histories recorded by earlier SDK releases."""

import json
import logging
from collections.abc import Generator
from datetime import datetime, timedelta
from typing import Any

from durabletask import task, worker
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.serialization import JsonDataConverter

INSTANCE_ID = "generator-compat"
NOW = datetime(2026, 1, 1)


def _orchestrator(ctx: task.OrchestrationContext, values: list[int]) -> Generator[task.Task[Any], Any, Any]:
    doubled = yield task.when_all([ctx.call_activity("double", input=value) for value in values])
    yield ctx.create_timer(timedelta(minutes=5))
    approval = ctx.wait_for_external_event("approval")
    winner = yield task.when_any([approval, ctx.create_timer(timedelta(hours=1))])
    approved = winner == approval and approval.get_result()
    child = yield ctx.call_sub_orchestrator("child", input=sum(doubled), instance_id="child-1")
    return {"doubled": doubled, "approved": approved, "child": child}


def _execute(old: list[pb.HistoryEvent], new: list[pb.HistoryEvent]) -> list[pb.OrchestratorAction]:
    registry = worker._Registry()  # pyright: ignore[reportPrivateUsage]
    registry.add_named_orchestrator("compat", _orchestrator)
    executor = worker._OrchestrationExecutor(  # pyright: ignore[reportPrivateUsage]
        registry, logging.getLogger(__name__), JsonDataConverter())
    return list(executor.execute(INSTANCE_ID, old, new).actions)


def _summary(actions: list[pb.OrchestratorAction]) -> list[tuple[Any, ...]]:
    result: list[tuple[Any, ...]] = []
    for action in actions:
        if action.HasField("scheduleTask"):
            result.append((action.id, "activity", action.scheduleTask.name, action.scheduleTask.input.value))
        elif action.HasField("createTimer"):
            result.append((action.id, "timer", action.createTimer.fireAt.ToDatetime().isoformat()))
        elif action.HasField("createSubOrchestration"):
            sub = action.createSubOrchestration
            result.append((action.id, "sub", sub.name, sub.instanceId, sub.input.value))
        elif action.HasField("completeOrchestration"):
            complete = action.completeOrchestration
            result.append((action.id, "complete", complete.orchestrationStatus, complete.result.value))
        else:
            result.append((action.id, action.WhichOneof("orchestratorActionType")))
    return result


def test_generator_history_from_previous_releases_replays_unchanged() -> None:
    timer_at = NOW + timedelta(minutes=5)
    deadline = timer_at + timedelta(hours=1)
    old: list[pb.HistoryEvent] = []
    steps: list[tuple[list[pb.HistoryEvent], list[tuple[Any, ...]]]] = [
        ([
            helpers.new_orchestrator_started_event(NOW),
            helpers.new_execution_started_event("compat", INSTANCE_ID, json.dumps([1, 2])),
        ], [
            (1, "activity", "double", "1"),
            (2, "activity", "double", "2"),
        ]),
        ([
            helpers.new_orchestrator_started_event(NOW),
            helpers.new_task_scheduled_event(1, "double"),
            helpers.new_task_scheduled_event(2, "double"),
            helpers.new_task_completed_event(2, "4"),
            helpers.new_task_completed_event(1, "2"),
        ], [
            (3, "timer", timer_at.isoformat()),
        ]),
        ([
            helpers.new_orchestrator_started_event(timer_at),
            helpers.new_timer_created_event(3, timer_at),
            helpers.new_timer_fired_event(3, timer_at),
        ], [
            (4, "timer", deadline.isoformat()),
        ]),
        ([
            helpers.new_orchestrator_started_event(timer_at),
            helpers.new_timer_created_event(4, deadline),
            helpers.new_event_raised_event("approval", "true"),
        ], [
            (5, "sub", "child", "child-1", "6"),
        ]),
        ([
            helpers.new_orchestrator_started_event(timer_at),
            helpers.new_sub_orchestration_created_event(5, "child", "child-1", "6"),
            helpers.new_sub_orchestration_completed_event(5, '"done"'),
        ], [
            (6, "complete", pb.ORCHESTRATION_STATUS_COMPLETED,
             json.dumps({"doubled": [2, 4], "approved": True, "child": "done"})),
        ]),
    ]
    for new, expected in steps:
        assert _summary(_execute(old, new)) == expected
        old += new
