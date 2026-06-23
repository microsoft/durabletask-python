# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, cast

from google.protobuf import json_format
from google.protobuf.message import Message

from durabletask import task
import durabletask.internal.orchestrator_service_pb2 as pb


@dataclass(slots=True)
class OrchestrationInstance:
    instance_id: str
    execution_id: str | None = None


@dataclass(slots=True)
class ParentInstanceInfo:
    task_scheduled_id: int
    name: str | None = None
    version: str | None = None
    orchestration_instance: OrchestrationInstance | None = None


@dataclass(slots=True)
class TraceContext:
    trace_parent: str
    span_id: str
    trace_state: str | None = None


@dataclass(slots=True)
class HistoryEvent:
    event_id: int
    timestamp: datetime

    def to_dict(self) -> dict[str, Any]:
        return _to_serializable(asdict(self))


@dataclass(slots=True)
class ExecutionStartedEvent(HistoryEvent):
    name: str
    version: str | None = None
    input: str | None = None
    orchestration_instance: OrchestrationInstance | None = None
    parent_instance: ParentInstanceInfo | None = None
    scheduled_start_timestamp: datetime | None = None
    parent_trace_context: TraceContext | None = None
    orchestration_span_id: str | None = None
    tags: dict[str, str] | None = None


@dataclass(slots=True)
class ExecutionCompletedEvent(HistoryEvent):
    orchestration_status: int
    result: str | None = None
    failure_details: task.FailureDetails | None = None


@dataclass(slots=True)
class ExecutionTerminatedEvent(HistoryEvent):
    input: str | None = None
    recurse: bool = False


@dataclass(slots=True)
class TaskScheduledEvent(HistoryEvent):
    name: str
    version: str | None = None
    input: str | None = None
    parent_trace_context: TraceContext | None = None
    tags: dict[str, str] | None = None


@dataclass(slots=True)
class TaskCompletedEvent(HistoryEvent):
    task_scheduled_id: int
    result: str | None = None


@dataclass(slots=True)
class TaskFailedEvent(HistoryEvent):
    task_scheduled_id: int
    failure_details: task.FailureDetails | None = None


@dataclass(slots=True)
class SubOrchestrationInstanceCreatedEvent(HistoryEvent):
    instance_id: str
    name: str
    version: str | None = None
    input: str | None = None
    parent_trace_context: TraceContext | None = None
    tags: dict[str, str] | None = None


@dataclass(slots=True)
class SubOrchestrationInstanceCompletedEvent(HistoryEvent):
    task_scheduled_id: int
    result: str | None = None


@dataclass(slots=True)
class SubOrchestrationInstanceFailedEvent(HistoryEvent):
    task_scheduled_id: int
    failure_details: task.FailureDetails | None = None


@dataclass(slots=True)
class TimerCreatedEvent(HistoryEvent):
    fire_at: datetime


@dataclass(slots=True)
class TimerFiredEvent(HistoryEvent):
    fire_at: datetime
    timer_id: int


@dataclass(slots=True)
class OrchestratorStartedEvent(HistoryEvent):
    pass


@dataclass(slots=True)
class OrchestratorCompletedEvent(HistoryEvent):
    pass


@dataclass(slots=True)
class EventSentEvent(HistoryEvent):
    instance_id: str
    name: str
    input: str | None = None


@dataclass(slots=True)
class EventRaisedEvent(HistoryEvent):
    name: str
    input: str | None = None


@dataclass(slots=True)
class GenericEvent(HistoryEvent):
    data: str | None = None


@dataclass(slots=True)
class HistoryStateEvent(HistoryEvent):
    orchestration_state: dict[str, Any]


@dataclass(slots=True)
class ContinueAsNewEvent(HistoryEvent):
    input: str | None = None


@dataclass(slots=True)
class ExecutionSuspendedEvent(HistoryEvent):
    input: str | None = None


@dataclass(slots=True)
class ExecutionResumedEvent(HistoryEvent):
    input: str | None = None


@dataclass(slots=True)
class EntityOperationSignaledEvent(HistoryEvent):
    request_id: str
    operation: str
    scheduled_time: datetime | None = None
    input: str | None = None
    target_instance_id: str | None = None


@dataclass(slots=True)
class EntityOperationCalledEvent(HistoryEvent):
    request_id: str
    operation: str
    scheduled_time: datetime | None = None
    input: str | None = None
    parent_instance_id: str | None = None
    parent_execution_id: str | None = None
    target_instance_id: str | None = None


@dataclass(slots=True)
class EntityOperationCompletedEvent(HistoryEvent):
    request_id: str
    output: str | None = None


@dataclass(slots=True)
class EntityOperationFailedEvent(HistoryEvent):
    request_id: str
    failure_details: task.FailureDetails | None = None


@dataclass(slots=True)
class EntityLockRequestedEvent(HistoryEvent):
    critical_section_id: str
    lock_set: list[str]
    position: int
    parent_instance_id: str | None = None


@dataclass(slots=True)
class EntityLockGrantedEvent(HistoryEvent):
    critical_section_id: str


@dataclass(slots=True)
class EntityUnlockSentEvent(HistoryEvent):
    critical_section_id: str
    parent_instance_id: str | None = None
    target_instance_id: str | None = None


@dataclass(slots=True)
class ExecutionRewoundEvent(HistoryEvent):
    reason: str | None = None
    parent_execution_id: str | None = None
    instance_id: str | None = None
    parent_trace_context: TraceContext | None = None
    name: str | None = None
    version: str | None = None
    input: str | None = None
    parent_instance: ParentInstanceInfo | None = None
    tags: dict[str, str] | None = None


def _from_protobuf(event: pb.HistoryEvent) -> HistoryEvent:  # pyright: ignore[reportUnusedFunction]
    event_type = event.WhichOneof('eventType')
    if event_type is None:
        raise ValueError('History event does not have an eventType set')
    converter = _EVENT_CONVERTERS.get(event_type)
    if converter is None:
        raise ValueError(f'Unsupported history event type: {event_type}')
    return converter(event)


def to_dict(event: HistoryEvent) -> dict[str, Any]:
    return event.to_dict()


def _base_kwargs(event: pb.HistoryEvent) -> dict[str, Any]:
    return {
        'event_id': event.eventId,
        'timestamp': event.timestamp.ToDatetime(timezone.utc),
    }


def _string_value(msg: Message, field_name: str) -> str | None:
    if msg.HasField(field_name):
        return getattr(msg, field_name).value
    return None


def _timestamp_value(msg: Message, field_name: str) -> datetime | None:
    if msg.HasField(field_name):
        return getattr(msg, field_name).ToDatetime(timezone.utc)
    return None


def _failure_details(msg: Message, field_name: str) -> task.FailureDetails | None:
    if not msg.HasField(field_name):
        return None
    details = getattr(msg, field_name)
    return task.FailureDetails(
        details.errorMessage,
        details.errorType,
        details.stackTrace.value if details.HasField('stackTrace') else None,
    )


def _trace_context(msg: Message, field_name: str) -> TraceContext | None:
    if not msg.HasField(field_name):
        return None
    value = getattr(msg, field_name)
    return TraceContext(
        trace_parent=value.traceParent,
        span_id=value.spanID,
        trace_state=value.traceState.value if value.HasField('traceState') else None,
    )


def _orchestration_instance(msg: Message, field_name: str) -> OrchestrationInstance | None:
    if not msg.HasField(field_name):
        return None
    value = getattr(msg, field_name)
    return OrchestrationInstance(
        instance_id=value.instanceId,
        execution_id=value.executionId.value if value.HasField('executionId') else None,
    )


def _parent_instance(msg: Message, field_name: str) -> ParentInstanceInfo | None:
    if not msg.HasField(field_name):
        return None
    value = getattr(msg, field_name)
    orchestration_instance = None
    if value.HasField('orchestrationInstance'):
        orchestration_instance = OrchestrationInstance(
            instance_id=value.orchestrationInstance.instanceId,
            execution_id=value.orchestrationInstance.executionId.value
            if value.orchestrationInstance.HasField('executionId') else None,
        )
    return ParentInstanceInfo(
        task_scheduled_id=value.taskScheduledId,
        name=value.name.value if value.HasField('name') else None,
        version=value.version.value if value.HasField('version') else None,
        orchestration_instance=orchestration_instance,
    )


def _message_to_dict(msg: Message) -> dict[str, Any]:
    return json_format.MessageToDict(msg, preserving_proto_field_name=True)


def _to_serializable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_to_serializable(item) for item in cast(list[Any], value)]
    if isinstance(value, dict):
        return {
            key: _to_serializable(item)
            for key, item in cast(dict[Any, Any], value).items()
        }
    return value


_EVENT_CONVERTERS: dict[str, Callable[[pb.HistoryEvent], HistoryEvent]] = {
    'executionStarted': lambda event: ExecutionStartedEvent(
        **_base_kwargs(event),
        name=event.executionStarted.name,
        version=_string_value(event.executionStarted, 'version'),
        input=_string_value(event.executionStarted, 'input'),
        orchestration_instance=_orchestration_instance(event.executionStarted, 'orchestrationInstance'),
        parent_instance=_parent_instance(event.executionStarted, 'parentInstance'),
        scheduled_start_timestamp=_timestamp_value(event.executionStarted, 'scheduledStartTimestamp'),
        parent_trace_context=_trace_context(event.executionStarted, 'parentTraceContext'),
        orchestration_span_id=_string_value(event.executionStarted, 'orchestrationSpanID'),
        tags=dict(event.executionStarted.tags) if event.executionStarted.tags else None,
    ),
    'executionCompleted': lambda event: ExecutionCompletedEvent(
        **_base_kwargs(event),
        orchestration_status=event.executionCompleted.orchestrationStatus,
        result=_string_value(event.executionCompleted, 'result'),
        failure_details=_failure_details(event.executionCompleted, 'failureDetails'),
    ),
    'executionTerminated': lambda event: ExecutionTerminatedEvent(
        **_base_kwargs(event),
        input=_string_value(event.executionTerminated, 'input'),
        recurse=event.executionTerminated.recurse,
    ),
    'taskScheduled': lambda event: TaskScheduledEvent(
        **_base_kwargs(event),
        name=event.taskScheduled.name,
        version=_string_value(event.taskScheduled, 'version'),
        input=_string_value(event.taskScheduled, 'input'),
        parent_trace_context=_trace_context(event.taskScheduled, 'parentTraceContext'),
        tags=dict(event.taskScheduled.tags) if event.taskScheduled.tags else None,
    ),
    'taskCompleted': lambda event: TaskCompletedEvent(
        **_base_kwargs(event),
        task_scheduled_id=event.taskCompleted.taskScheduledId,
        result=_string_value(event.taskCompleted, 'result'),
    ),
    'taskFailed': lambda event: TaskFailedEvent(
        **_base_kwargs(event),
        task_scheduled_id=event.taskFailed.taskScheduledId,
        failure_details=_failure_details(event.taskFailed, 'failureDetails'),
    ),
    'subOrchestrationInstanceCreated': lambda event: SubOrchestrationInstanceCreatedEvent(
        **_base_kwargs(event),
        instance_id=event.subOrchestrationInstanceCreated.instanceId,
        name=event.subOrchestrationInstanceCreated.name,
        version=_string_value(event.subOrchestrationInstanceCreated, 'version'),
        input=_string_value(event.subOrchestrationInstanceCreated, 'input'),
        parent_trace_context=_trace_context(event.subOrchestrationInstanceCreated, 'parentTraceContext'),
        tags=dict(event.subOrchestrationInstanceCreated.tags) if event.subOrchestrationInstanceCreated.tags else None,
    ),
    'subOrchestrationInstanceCompleted': lambda event: SubOrchestrationInstanceCompletedEvent(
        **_base_kwargs(event),
        task_scheduled_id=event.subOrchestrationInstanceCompleted.taskScheduledId,
        result=_string_value(event.subOrchestrationInstanceCompleted, 'result'),
    ),
    'subOrchestrationInstanceFailed': lambda event: SubOrchestrationInstanceFailedEvent(
        **_base_kwargs(event),
        task_scheduled_id=event.subOrchestrationInstanceFailed.taskScheduledId,
        failure_details=_failure_details(event.subOrchestrationInstanceFailed, 'failureDetails'),
    ),
    'timerCreated': lambda event: TimerCreatedEvent(
        **_base_kwargs(event),
        fire_at=event.timerCreated.fireAt.ToDatetime(timezone.utc),
    ),
    'timerFired': lambda event: TimerFiredEvent(
        **_base_kwargs(event),
        fire_at=event.timerFired.fireAt.ToDatetime(timezone.utc),
        timer_id=event.timerFired.timerId,
    ),
    'orchestratorStarted': lambda event: OrchestratorStartedEvent(**_base_kwargs(event)),
    'orchestratorCompleted': lambda event: OrchestratorCompletedEvent(**_base_kwargs(event)),
    'eventSent': lambda event: EventSentEvent(
        **_base_kwargs(event),
        instance_id=event.eventSent.instanceId,
        name=event.eventSent.name,
        input=_string_value(event.eventSent, 'input'),
    ),
    'eventRaised': lambda event: EventRaisedEvent(
        **_base_kwargs(event),
        name=event.eventRaised.name,
        input=_string_value(event.eventRaised, 'input'),
    ),
    'genericEvent': lambda event: GenericEvent(
        **_base_kwargs(event),
        data=_string_value(event.genericEvent, 'data'),
    ),
    'historyState': lambda event: HistoryStateEvent(
        **_base_kwargs(event),
        orchestration_state=_message_to_dict(event.historyState.orchestrationState),
    ),
    'continueAsNew': lambda event: ContinueAsNewEvent(
        **_base_kwargs(event),
        input=_string_value(event.continueAsNew, 'input'),
    ),
    'executionSuspended': lambda event: ExecutionSuspendedEvent(
        **_base_kwargs(event),
        input=_string_value(event.executionSuspended, 'input'),
    ),
    'executionResumed': lambda event: ExecutionResumedEvent(
        **_base_kwargs(event),
        input=_string_value(event.executionResumed, 'input'),
    ),
    'entityOperationSignaled': lambda event: EntityOperationSignaledEvent(
        **_base_kwargs(event),
        request_id=event.entityOperationSignaled.requestId,
        operation=event.entityOperationSignaled.operation,
        scheduled_time=_timestamp_value(event.entityOperationSignaled, 'scheduledTime'),
        input=_string_value(event.entityOperationSignaled, 'input'),
        target_instance_id=_string_value(event.entityOperationSignaled, 'targetInstanceId'),
    ),
    'entityOperationCalled': lambda event: EntityOperationCalledEvent(
        **_base_kwargs(event),
        request_id=event.entityOperationCalled.requestId,
        operation=event.entityOperationCalled.operation,
        scheduled_time=_timestamp_value(event.entityOperationCalled, 'scheduledTime'),
        input=_string_value(event.entityOperationCalled, 'input'),
        parent_instance_id=_string_value(event.entityOperationCalled, 'parentInstanceId'),
        parent_execution_id=_string_value(event.entityOperationCalled, 'parentExecutionId'),
        target_instance_id=_string_value(event.entityOperationCalled, 'targetInstanceId'),
    ),
    'entityOperationCompleted': lambda event: EntityOperationCompletedEvent(
        **_base_kwargs(event),
        request_id=event.entityOperationCompleted.requestId,
        output=_string_value(event.entityOperationCompleted, 'output'),
    ),
    'entityOperationFailed': lambda event: EntityOperationFailedEvent(
        **_base_kwargs(event),
        request_id=event.entityOperationFailed.requestId,
        failure_details=_failure_details(event.entityOperationFailed, 'failureDetails'),
    ),
    'entityLockRequested': lambda event: EntityLockRequestedEvent(
        **_base_kwargs(event),
        critical_section_id=event.entityLockRequested.criticalSectionId,
        lock_set=list(event.entityLockRequested.lockSet),
        position=event.entityLockRequested.position,
        parent_instance_id=_string_value(event.entityLockRequested, 'parentInstanceId'),
    ),
    'entityLockGranted': lambda event: EntityLockGrantedEvent(
        **_base_kwargs(event),
        critical_section_id=event.entityLockGranted.criticalSectionId,
    ),
    'entityUnlockSent': lambda event: EntityUnlockSentEvent(
        **_base_kwargs(event),
        critical_section_id=event.entityUnlockSent.criticalSectionId,
        parent_instance_id=_string_value(event.entityUnlockSent, 'parentInstanceId'),
        target_instance_id=_string_value(event.entityUnlockSent, 'targetInstanceId'),
    ),
    'executionRewound': lambda event: ExecutionRewoundEvent(
        **_base_kwargs(event),
        reason=_string_value(event.executionRewound, 'reason'),
        parent_execution_id=_string_value(event.executionRewound, 'parentExecutionId'),
        instance_id=_string_value(event.executionRewound, 'instanceId'),
        parent_trace_context=_trace_context(event.executionRewound, 'parentTraceContext'),
        name=_string_value(event.executionRewound, 'name'),
        version=_string_value(event.executionRewound, 'version'),
        input=_string_value(event.executionRewound, 'input'),
        parent_instance=_parent_instance(event.executionRewound, 'parentInstance'),
        tags=dict(event.executionRewound.tags) if event.executionRewound.tags else None,
    ),
}


__all__ = [
    'ContinueAsNewEvent',
    'EntityLockGrantedEvent',
    'EntityLockRequestedEvent',
    'EntityOperationCalledEvent',
    'EntityOperationCompletedEvent',
    'EntityOperationFailedEvent',
    'EntityOperationSignaledEvent',
    'EntityUnlockSentEvent',
    'EventRaisedEvent',
    'EventSentEvent',
    'ExecutionCompletedEvent',
    'ExecutionResumedEvent',
    'ExecutionRewoundEvent',
    'ExecutionStartedEvent',
    'ExecutionSuspendedEvent',
    'ExecutionTerminatedEvent',
    'GenericEvent',
    'HistoryEvent',
    'HistoryStateEvent',
    'OrchestrationInstance',
    'OrchestratorCompletedEvent',
    'OrchestratorStartedEvent',
    'ParentInstanceInfo',
    'SubOrchestrationInstanceCompletedEvent',
    'SubOrchestrationInstanceCreatedEvent',
    'SubOrchestrationInstanceFailedEvent',
    'TaskCompletedEvent',
    'TaskFailedEvent',
    'TaskScheduledEvent',
    'TimerCreatedEvent',
    'TimerFiredEvent',
    'TraceContext',
    'to_dict',
]
