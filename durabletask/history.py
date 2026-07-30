# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from __future__ import annotations

import functools
from collections.abc import Callable
from dataclasses import asdict, dataclass, fields
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
        return {
            name: _to_serializable(getattr(self, name))
            for name in _field_names(type(self))
        }


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


# Field names are looked up once per dataclass type. History export walks
# many events of the same handful of types, so caching avoids repeatedly
# rebuilding the tuple returned by ``dataclasses.fields``. The cache is
# bounded so that dynamically created dataclass types cannot pin an
# unbounded number of entries (and the classes they reference) in memory.
_FIELD_NAMES_CACHE_SIZE = 256

# Values of these exact types are already JSON-native and need no
# conversion. Checking ``type(value)`` against a set is a single hash
# lookup, which short-circuits the common case (most event fields are
# strings, ints, or ``None``) before the type checks below.
_JSON_NATIVE_TYPES: frozenset[type[Any]] = frozenset({bool, float, int, str, type(None)})


@functools.lru_cache(maxsize=_FIELD_NAMES_CACHE_SIZE)
def _field_names(cls: type[Any]) -> tuple[str, ...]:
    return tuple(field.name for field in fields(cast(Any, cls)))


@dataclass
class _LegacyBox:
    """Carrier used to re-enter ``dataclasses.asdict`` for one value."""

    value: Any


def _asdict_only(value: Any) -> Any:
    """Apply ``dataclasses.asdict`` recursion to *value* and nothing else.

    Boxing the value in a throwaway dataclass lets the interpreter's own
    ``asdict`` implementation handle it, so container subclasses,
    namedtuples, ``defaultdict`` and the deep-copy of leaf values all behave
    exactly as they did before this module walked events itself. Delegating
    rather than reimplementing matters because those details have changed
    between Python releases and this package supports several of them.
    """
    return asdict(_LegacyBox(value))['value']


def _legacy_walk(value: Any) -> Any:
    """The pre-optimization conversion pass, applied to an ``asdict`` result."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_legacy_walk(item) for item in cast(list[Any], value)]
    if isinstance(value, dict):
        return {
            key: _legacy_walk(item)
            for key, item in cast(dict[Any, Any], value).items()
        }
    return value


def _legacy_compat(value: Any) -> Any:
    """Reproduce the original ``asdict`` + walk pipeline for one value."""
    return _legacy_walk(_asdict_only(value))


def _to_serializable(value: Any) -> Any:
    """Recursively convert *value* into the form history export writes.

    Values the SDK itself produces convert to JSON-native types. Arbitrary
    values can also arrive through ``dict[str, Any]`` fields, and those keep
    whatever the original pipeline did with them -- which for a few shapes,
    such as a tuple holding a ``datetime``, is not JSON-encodable. That is
    preserved on purpose rather than fixed here; see :func:`_legacy_compat`.

    This walks dataclass instances directly instead of going through
    ``dataclasses.asdict``, which would deep-copy the whole event graph
    into a throwaway intermediate structure that then has to be walked a
    second time.

    The type checks below are deliberately exact rather than
    ``isinstance``. Only the built-in types are handled inline, because
    only for those is walking in place provably identical to what
    ``asdict`` produced. Subclasses, tuples and every other value are
    routed to :func:`_legacy_compat`, which re-enters the real ``asdict``
    so their original semantics -- constructor round-trips, key
    recursion and deep-copied leaves -- are preserved exactly.
    """
    # ``type(value)`` is ``type[Unknown]`` to a type checker because *value*
    # is ``Any``, so the cast is what keeps this module clean under strict
    # checking. Two details are deliberate: the annotation is quoted, since
    # an unquoted ``type[Any]`` is evaluated on every call and builds a
    # throwaway ``types.GenericAlias``; and the lookup is ``type(value)``
    # rather than the cheaper ``value.__class__``, because ``asdict`` used
    # ``type(obj)`` and an object overriding ``__class__`` would otherwise
    # be dispatched differently than it was before.
    value_type = cast('type[Any]', type(value))
    if value_type in _JSON_NATIVE_TYPES:
        return value
    # Mirrors the private ``dataclasses._is_dataclass_instance`` check that
    # ``asdict`` used: dataclass *types* are leaves, only instances recurse.
    if hasattr(value_type, '__dataclass_fields__'):
        return {
            name: _to_serializable(getattr(value, name))
            for name in _field_names(value_type)
        }
    if value_type is datetime:
        return value.isoformat()
    if value_type is list:
        return [_to_serializable(item) for item in value]
    if value_type is dict:
        # ``asdict`` rebuilt keys and collapsed any that compared equal
        # afterwards, and only then did the conversion pass run, so a value
        # whose entry lost a collision was never converted. Converting
        # inline would visit those dropped entries, which is observable if
        # conversion raises or has side effects. Only a key that ``asdict``
        # would rebuild can collide -- native keys pass through untouched
        # and a mapping's keys are already distinct -- so the presence of
        # one is the signal to hand the whole mapping to the legacy path.
        # This is checked before any value is converted, because bailing
        # out partway would already have visited earlier entries.
        for key in value:
            if type(key) not in _JSON_NATIVE_TYPES:
                return _legacy_compat(value)
        return {key: _to_serializable(item) for key, item in value.items()}
    return _legacy_compat(value)


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
