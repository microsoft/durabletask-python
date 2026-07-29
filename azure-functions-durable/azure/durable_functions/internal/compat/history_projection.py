# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from typing import Any, cast

from durabletask import history, task
from durabletask.client import OrchestrationStatus

from .orchestration_runtime_status import from_durabletask_status


def project_history(
        events: list[history.HistoryEvent],
        *,
        show_input: bool,
        show_history_output: bool) -> list[dict[str, Any]]:
    """Project Durable Task history events into the v1 status-query shape."""
    projected: list[dict[str, Any]] = []
    scheduled_tasks: dict[int, tuple[int, history.TaskScheduledEvent]] = {}
    scheduled_sub_orchestrations: dict[
        int, tuple[int, history.SubOrchestrationInstanceCreatedEvent]] = {}
    removed_indexes: set[int] = set()

    for event in events:
        if isinstance(
                event,
                (history.OrchestratorStartedEvent,
                 history.OrchestratorCompletedEvent)):
            continue

        item = _project_event(
            event,
            show_input=show_input,
            show_history_output=show_history_output)
        projected_index = len(projected)

        if isinstance(event, history.TaskScheduledEvent):
            scheduled_tasks[event.event_id] = (projected_index, event)
        elif isinstance(
                event,
                (history.TaskCompletedEvent, history.TaskFailedEvent)):
            _add_scheduled_event_data(
                item,
                event.task_scheduled_id,
                scheduled_tasks,
                removed_indexes,
                show_input)
        elif isinstance(event, history.SubOrchestrationInstanceCreatedEvent):
            scheduled_sub_orchestrations[event.event_id] = (
                projected_index, event)
        elif isinstance(
                event,
                (history.SubOrchestrationInstanceCompletedEvent,
                 history.SubOrchestrationInstanceFailedEvent)):
            _add_scheduled_event_data(
                item,
                event.task_scheduled_id,
                scheduled_sub_orchestrations,
                removed_indexes,
                show_input)

        projected.append(item)

    return [
        event for index, event in enumerate(projected)
        if index not in removed_indexes
    ]


def _project_event(
        event: history.HistoryEvent,
        *,
        show_input: bool,
        show_history_output: bool) -> dict[str, Any]:
    item = {
        "EventType": type(event).__name__.removesuffix("Event"),
        "Timestamp": _format_datetime(event.timestamp),
    }
    for field in fields(event):
        if field.name in {"event_id", "timestamp"}:
            continue
        if field.name == "input" and not show_input:
            continue
        if field.name in {"result", "output"} and not show_history_output:
            continue

        value = getattr(event, field.name)
        if value is None:
            continue
        if field.name in {"result", "output"}:
            value = _raw_payload(value)
        item[_field_name(field.name)] = _serialize(value)

    if isinstance(
            event,
            (history.TaskScheduledEvent,
             history.SubOrchestrationInstanceCreatedEvent)):
        item.pop("Version", None)
    elif isinstance(event, history.ExecutionStartedEvent):
        item["FunctionName"] = item.pop("Name")
        for field_name in (
                "OrchestrationInstance",
                "ParentInstance",
                "Version",
                "Tags"):
            item.pop(field_name, None)
    elif isinstance(
            event,
            (history.TaskCompletedEvent,
             history.TaskFailedEvent,
             history.SubOrchestrationInstanceCompletedEvent,
             history.SubOrchestrationInstanceFailedEvent)):
        item.pop("TaskScheduledId", None)
    elif isinstance(event, history.ExecutionCompletedEvent):
        try:
            status = OrchestrationStatus(event.orchestration_status)
        except ValueError:
            pass
        else:
            item["OrchestrationStatus"] = from_durabletask_status(status).name
    elif isinstance(event, history.TimerFiredEvent):
        item.pop("TimerId", None)

    return item


def _add_scheduled_event_data(
        item: dict[str, Any],
        scheduled_id: int,
        scheduled_events: dict[int, tuple[int, Any]],
        removed_indexes: set[int],
        show_input: bool) -> None:
    scheduled = scheduled_events.get(scheduled_id)
    if scheduled is None:
        return

    scheduled_index, scheduled_event = scheduled
    item["ScheduledTime"] = _format_datetime(scheduled_event.timestamp)
    item["FunctionName"] = scheduled_event.name
    if show_input and scheduled_event.input is not None:
        item["Input"] = scheduled_event.input
    removed_indexes.add(scheduled_index)


def _serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return _format_datetime(value)
    if isinstance(value, task.FailureDetails):
        result = {
            "ErrorMessage": value.message,
            "ErrorType": value.error_type,
        }
        if value.stack_trace is not None:
            result["StackTrace"] = value.stack_trace
        return result
    if is_dataclass(value) and not isinstance(value, type):
        return {
            _field_name(field.name): _serialize(getattr(value, field.name))
            for field in fields(value)
            if getattr(value, field.name) is not None
        }
    if isinstance(value, list):
        return [_serialize(item) for item in cast(list[Any], value)]
    if isinstance(value, dict):
        mapping = cast(dict[Any, Any], value)
        return {key: _serialize(item) for key, item in mapping.items()}
    return value


def _field_name(name: str) -> str:
    if name == "scheduled_start_timestamp":
        return "ScheduledStartTime"
    if name in {"orchestration_span_id", "span_id"}:
        return "".join(part.title() for part in name.split("_"))[:-2] + "ID"
    return "".join(part.title() for part in name.split("_"))


def _raw_payload(serialized: str) -> Any:
    try:
        return json.loads(serialized)
    except (TypeError, ValueError):
        return serialized


def _format_datetime(value: datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
