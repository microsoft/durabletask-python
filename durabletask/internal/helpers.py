# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import traceback
from collections.abc import Mapping, Sequence
from decimal import Decimal
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from google.protobuf import struct_pb2, timestamp_pb2, wrappers_pb2

from durabletask.entities import EntityInstanceId
from durabletask.exception_properties import ExceptionPropertiesProvider
import durabletask.internal.orchestrator_service_pb2 as pb

if TYPE_CHECKING:
    from durabletask.task import FailureDetails

# TODO: The new_xxx_event methods are only used by test code and should be moved elsewhere


def new_orchestrator_started_event(timestamp: datetime | None = None) -> pb.HistoryEvent:
    ts = timestamp_pb2.Timestamp()
    if timestamp is not None:
        ts.FromDatetime(timestamp)
    return pb.HistoryEvent(eventId=-1, timestamp=ts, orchestratorStarted=pb.OrchestratorStartedEvent())


def new_orchestrator_completed_event() -> pb.HistoryEvent:
    return pb.HistoryEvent(eventId=-1, timestamp=timestamp_pb2.Timestamp(),
                           orchestratorCompleted=pb.OrchestratorCompletedEvent())


def new_execution_started_event(name: str, instance_id: str, encoded_input: str | None = None,
                                tags: dict[str, str] | None = None,
                                version: str | None = None,
                                parent_trace_context: pb.TraceContext | None = None) -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        executionStarted=pb.ExecutionStartedEvent(
            name=name,
            version=get_string_value(version),
            input=get_string_value(encoded_input),
            orchestrationInstance=pb.OrchestrationInstance(instanceId=instance_id),
            tags=tags,
            parentTraceContext=parent_trace_context))


def new_timer_created_event(timer_id: int, fire_at: datetime) -> pb.HistoryEvent:
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(fire_at)
    return pb.HistoryEvent(
        eventId=timer_id,
        timestamp=timestamp_pb2.Timestamp(),
        timerCreated=pb.TimerCreatedEvent(fireAt=ts)
    )


def new_timer_fired_event(timer_id: int, fire_at: datetime) -> pb.HistoryEvent:
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(fire_at)
    return pb.HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        timerFired=pb.TimerFiredEvent(fireAt=ts, timerId=timer_id)
    )


def new_task_scheduled_event(event_id: int, name: str, encoded_input: str | None = None) -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=event_id,
        timestamp=timestamp_pb2.Timestamp(),
        taskScheduled=pb.TaskScheduledEvent(name=name, input=get_string_value(encoded_input))
    )


def new_task_completed_event(event_id: int, encoded_output: str | None = None) -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        taskCompleted=pb.TaskCompletedEvent(taskScheduledId=event_id, result=get_string_value(encoded_output))
    )


def new_task_failed_event(event_id: int, ex: Exception) -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        taskFailed=pb.TaskFailedEvent(taskScheduledId=event_id, failureDetails=new_failure_details(ex))
    )


def new_sub_orchestration_created_event(
        event_id: int,
        name: str,
        instance_id: str,
        encoded_input: str | None = None,
        version: str | None = None) -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=event_id,
        timestamp=timestamp_pb2.Timestamp(),
        subOrchestrationInstanceCreated=pb.SubOrchestrationInstanceCreatedEvent(
            name=name,
            version=get_string_value(version),
            input=get_string_value(encoded_input),
            instanceId=instance_id)
    )


def new_sub_orchestration_completed_event(event_id: int, encoded_output: str | None = None) -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        subOrchestrationInstanceCompleted=pb.SubOrchestrationInstanceCompletedEvent(
            result=get_string_value(encoded_output),
            taskScheduledId=event_id)
    )


def new_sub_orchestration_failed_event(event_id: int, ex: Exception) -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        subOrchestrationInstanceFailed=pb.SubOrchestrationInstanceFailedEvent(
            failureDetails=new_failure_details(ex),
            taskScheduledId=event_id)
    )


def get_qualified_name(t: type) -> str:
    """Return the fully-qualified ``module.qualname`` name of a type.

    This mirrors the fully-qualified error-type names produced by the other
    Durable Task SDKs (.NET ``Type.ToString()`` and Java ``Class.getName()``),
    so that :attr:`FailureDetails.error_type` values are consistent across SDKs
    and can be matched unambiguously by :meth:`FailureDetails.is_caused_by`.

    Builtin exception types keep their ``builtins.`` prefix (e.g.
    ``builtins.ValueError``), matching how .NET keeps ``System.`` and Java keeps
    ``java.lang.``.
    """
    module = getattr(t, "__module__", None)
    qualname = getattr(t, "__qualname__", None) or getattr(t, "__name__", None) or str(t)
    if not module:
        return qualname
    return f"{module}.{qualname}"


def protobuf_value_from_python(value: Any) -> struct_pb2.Value:
    """Convert a portable Python value to a protobuf ``Value``."""
    if value is None:
        return struct_pb2.Value(null_value=struct_pb2.NullValue.NULL_VALUE)
    if isinstance(value, bool):
        return struct_pb2.Value(bool_value=value)
    if isinstance(value, (int, float, Decimal)):
        return struct_pb2.Value(number_value=float(value))
    if isinstance(value, str):
        return struct_pb2.Value(string_value=value)
    if isinstance(value, Mapping):
        mapping = cast(Mapping[Any, Any], value)
        fields = {
            str(key): protobuf_value_from_python(item)
            for key, item in mapping.items()
        }
        return struct_pb2.Value(struct_value=struct_pb2.Struct(fields=fields))
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        sequence = cast(Sequence[Any], value)
        return struct_pb2.Value(
            list_value=struct_pb2.ListValue(
                values=[protobuf_value_from_python(item) for item in sequence]))
    return struct_pb2.Value(string_value=str(value))


def python_value_from_protobuf(value: struct_pb2.Value) -> Any:
    """Convert a protobuf ``Value`` to a JSON-safe Python value."""
    kind = value.WhichOneof("kind")
    if kind == "null_value":
        return None
    if kind == "bool_value":
        return value.bool_value
    if kind == "number_value":
        return value.number_value
    if kind == "string_value":
        return value.string_value
    if kind == "struct_value":
        return {
            key: python_value_from_protobuf(item)
            for key, item in value.struct_value.fields.items()
        }
    if kind == "list_value":
        return [python_value_from_protobuf(item) for item in value.list_value.values]
    return str(value)


def failure_details_from_protobuf(details: pb.TaskFailureDetails) -> "FailureDetails":
    """Convert protobuf failure details to the public, JSON-safe model."""
    from durabletask.task import FailureDetails

    return FailureDetails(
        message=details.errorMessage,
        error_type=details.errorType,
        stack_trace=details.stackTrace.value if details.HasField("stackTrace") else None,
        inner_failure=(
            failure_details_from_protobuf(details.innerFailure)
            if details.HasField("innerFailure") else None),
        properties=(
            {
                key: python_value_from_protobuf(value)
                for key, value in details.properties.items()
            }
            if details.properties else None),
    )


def new_failure_details(
        ex: Exception,
        exception_properties_provider: ExceptionPropertiesProvider | None = None,
        logger: logging.Logger | None = None,
        _visited: set[int] | None = None) -> pb.TaskFailureDetails:
    if _visited is None:
        _visited = set()
    _visited.add(id(ex))
    inner: BaseException | None = ex.__cause__ or ex.__context__
    if len(_visited) > 10 or (inner and id(inner) in _visited) or not isinstance(inner, Exception):
        inner = None
    properties: dict[str, struct_pb2.Value] | None = None
    if exception_properties_provider is not None:
        try:
            provider_properties = exception_properties_provider.get_exception_properties(ex)
        except Exception:
            if logger is not None:
                logger.warning(
                    "ExceptionPropertiesProvider failed while processing %s.",
                    get_qualified_name(type(ex)),
                    exc_info=True)
        else:
            try:
                untyped_properties: object = cast(object, provider_properties)
                if untyped_properties is not None:
                    if not isinstance(untyped_properties, Mapping):
                        raise TypeError(
                            "ExceptionPropertiesProvider.get_exception_properties() "
                            "must return a mapping or None.")
                    properties = {
                        str(key): protobuf_value_from_python(value)
                        for key, value in cast(Mapping[Any, Any], untyped_properties).items()
                    }
            except Exception:
                if logger is not None:
                    logger.warning(
                        "ExceptionPropertiesProvider returned invalid properties for %s.",
                        get_qualified_name(type(ex)),
                        exc_info=True)

    failure_details = pb.TaskFailureDetails(
        errorType=get_qualified_name(type(ex)),
        errorMessage=str(ex),
        stackTrace=wrappers_pb2.StringValue(value=''.join(traceback.format_tb(ex.__traceback__))),
        innerFailure=(
            new_failure_details(
                inner, exception_properties_provider, logger, _visited)
            if inner else None)
    )
    if properties:
        for key, value in properties.items():
            failure_details.properties[key].CopyFrom(value)
    return failure_details


def _failure_details_from_core_dict(fd: dict[str, Any]) -> pb.TaskFailureDetails:
    """Convert a serialized DurableTask.Core ``FailureDetails`` dict to protobuf."""
    inner = fd.get("InnerFailure")
    stack_trace = fd.get("StackTrace")
    result = pb.TaskFailureDetails(
        errorType=str(fd.get("ErrorType") or ""),
        errorMessage=str(fd.get("ErrorMessage") or ""),
        stackTrace=get_string_value(str(stack_trace) if stack_trace is not None else None),
        innerFailure=_failure_details_from_core_dict(cast(dict[str, Any], inner)) if isinstance(inner, dict) else None,
        isNonRetriable=bool(fd.get("IsNonRetriable", False)),
    )
    properties = fd.get("Properties")
    if isinstance(properties, Mapping):
        for key, value in cast(Mapping[Any, Any], properties).items():
            result.properties[str(key)].CopyFrom(protobuf_value_from_python(value))
    return result


def entity_response_failure_details(
        response: dict[str, Any],
        error_content: Any = None) -> pb.TaskFailureDetails:
    """Build failure details from a failed legacy-protocol entity ``ResponseMessage``.

    Call this only for responses that :func:`is_entity_error_response` reports as
    failures. In the WebJobs "old protocol" ``ResponseMessage`` (see
    ``EntityScheduler/ResponseMessage.cs``), a failed operation serializes the
    human-readable content into ``result`` while ``exceptionType`` carries only
    the exception's type name (a presence marker) -- it is *not* the message.
    This mirrors ``azure-functions-durable-python`` / ``-js``, which read the
    message from ``result`` and ignore ``exceptionType``'s value.

    Parameters
    ----------
    response:
        The deserialized ``ResponseMessage`` dict.
    error_content:
        The already-deserialized ``result`` payload, used as the failure
        message. A structured ``failureDetails`` object, if present, takes
        precedence (current-protocol shape).
    """
    failure_details = response.get("failureDetails")
    if isinstance(failure_details, dict):
        return _failure_details_from_core_dict(cast(dict[str, Any], failure_details))
    error_type = str(response.get("exceptionType") or "")
    error_message = "" if error_content is None else str(error_content)
    return pb.TaskFailureDetails(errorType=error_type, errorMessage=error_message)


def is_entity_error_response(response: dict[str, Any]) -> bool:
    """Return ``True`` if a legacy-protocol entity ``ResponseMessage`` is a failure.

    In the WebJobs "old protocol" a failed operation is marked by the presence of
    an ``exceptionType`` field (successful responses omit it). Current-protocol
    payloads may instead carry a structured ``failureDetails`` object.
    """
    return "exceptionType" in response or isinstance(response.get("failureDetails"), dict)


def new_event_sent_event(
        event_id: int,
        instance_id: str,
        input: str | None,
        *,
        name: str = "") -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=event_id,
        timestamp=timestamp_pb2.Timestamp(),
        eventSent=pb.EventSentEvent(
            name=name,
            input=get_string_value(input),
            instanceId=instance_id
        )
    )


def new_event_raised_event(name: str, encoded_input: str | None = None) -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        eventRaised=pb.EventRaisedEvent(name=name, input=get_string_value(encoded_input))
    )


def new_suspend_event(*, encoded_input: str | None = None) -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        executionSuspended=pb.ExecutionSuspendedEvent(
            input=get_string_value(encoded_input)
        )
    )


def new_resume_event(*, encoded_input: str | None = None) -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        executionResumed=pb.ExecutionResumedEvent(
            input=get_string_value(encoded_input)
        )
    )


def new_terminated_event(*, encoded_output: str | None = None) -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        executionTerminated=pb.ExecutionTerminatedEvent(
            input=get_string_value(encoded_output)
        )
    )


def new_execution_completed_event(
        status: pb.OrchestrationStatus,
        encoded_result: str | None = None,
        failure_details: pb.TaskFailureDetails | None = None) -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        executionCompleted=pb.ExecutionCompletedEvent(
            orchestrationStatus=status,
            result=get_string_value(encoded_result),
            failureDetails=failure_details,
        )
    )


def get_string_value(val: str | None) -> wrappers_pb2.StringValue | None:
    if val is None:
        return None
    else:
        return wrappers_pb2.StringValue(value=val)


def get_int_value(val: int | None) -> wrappers_pb2.Int32Value | None:
    if val is None:
        return None
    else:
        return wrappers_pb2.Int32Value(value=val)


def get_string_value_or_empty(val: str | None) -> wrappers_pb2.StringValue:
    if val is None:
        return wrappers_pb2.StringValue(value="")
    return wrappers_pb2.StringValue(value=val)


def new_complete_orchestration_action(
        id: int,
        status: pb.OrchestrationStatus,
        result: str | None = None,
        failure_details: pb.TaskFailureDetails | None = None,
        carryover_events: list[pb.HistoryEvent] | None = None,
        new_version: str | None = None) -> pb.OrchestratorAction:
    completeOrchestrationAction = pb.CompleteOrchestrationAction(
        orchestrationStatus=status,
        result=get_string_value(result),
        failureDetails=failure_details,
        carryoverEvents=carryover_events,
        newVersion=get_string_value(new_version))

    return pb.OrchestratorAction(id=id, completeOrchestration=completeOrchestrationAction)


def new_create_timer_action(id: int, fire_at: datetime) -> pb.OrchestratorAction:
    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(fire_at)
    return pb.OrchestratorAction(id=id, createTimer=pb.CreateTimerAction(fireAt=timestamp))


def new_schedule_task_action(id: int, name: str, encoded_input: str | None,
                             tags: dict[str, str] | None,
                             parent_trace_context: pb.TraceContext | None = None) -> pb.OrchestratorAction:
    return pb.OrchestratorAction(id=id, scheduleTask=pb.ScheduleTaskAction(
        name=name,
        input=get_string_value(encoded_input),
        tags=tags,
        parentTraceContext=parent_trace_context,
    ))


def new_send_event_action(
        id: int,
        instance_id: str,
        event_name: str,
        encoded_data: str | None) -> pb.OrchestratorAction:
    return pb.OrchestratorAction(
        id=id,
        sendEvent=pb.SendEventAction(
            instance=pb.OrchestrationInstance(instanceId=instance_id),
            name=event_name,
            data=get_string_value(encoded_data),
        ),
    )


def new_call_entity_action(id: int,
                           parent_instance_id: str,
                           entity_id: EntityInstanceId,
                           operation: str,
                           encoded_input: str | None,
                           request_id: str) -> pb.OrchestratorAction:
    return pb.OrchestratorAction(id=id, sendEntityMessage=pb.SendEntityMessageAction(entityOperationCalled=pb.EntityOperationCalledEvent(
        requestId=request_id,
        operation=operation,
        scheduledTime=None,
        input=get_string_value(encoded_input),
        parentInstanceId=get_string_value(parent_instance_id),
        parentExecutionId=None,
        targetInstanceId=get_string_value(str(entity_id)),
    )))


def new_signal_entity_action(id: int,
                             entity_id: EntityInstanceId,
                             operation: str,
                             encoded_input: str | None,
                             request_id: str,
                             signal_time: datetime | None = None) -> pb.OrchestratorAction:
    scheduled_time = new_timestamp(signal_time) if signal_time is not None else None
    return pb.OrchestratorAction(id=id, sendEntityMessage=pb.SendEntityMessageAction(entityOperationSignaled=pb.EntityOperationSignaledEvent(
        requestId=request_id,
        operation=operation,
        scheduledTime=scheduled_time,
        input=get_string_value(encoded_input),
        targetInstanceId=get_string_value(str(entity_id)),
    )))


def new_lock_entities_action(id: int, entity_message: pb.SendEntityMessageAction):
    return pb.OrchestratorAction(id=id, sendEntityMessage=entity_message)


def convert_to_entity_batch_request(req: pb.EntityRequest) -> tuple[pb.EntityBatchRequest, list[pb.OperationInfo]]:
    batch_request = pb.EntityBatchRequest(entityState=req.entityState, instanceId=req.instanceId, operations=[])

    operation_infos: list[pb.OperationInfo] = []

    for op in req.operationRequests:
        if op.HasField("entityOperationSignaled"):
            batch_request.operations.append(pb.OperationRequest(requestId=op.entityOperationSignaled.requestId,
                                                                operation=op.entityOperationSignaled.operation,
                                                                input=op.entityOperationSignaled.input))
            operation_infos.append(pb.OperationInfo(requestId=op.entityOperationSignaled.requestId,
                                                    responseDestination=None))
        elif op.HasField("entityOperationCalled"):
            batch_request.operations.append(pb.OperationRequest(requestId=op.entityOperationCalled.requestId,
                                                                operation=op.entityOperationCalled.operation,
                                                                input=op.entityOperationCalled.input))
            operation_infos.append(pb.OperationInfo(requestId=op.entityOperationCalled.requestId,
                                                    responseDestination=pb.OrchestrationInstance(
                                                        instanceId=op.entityOperationCalled.parentInstanceId.value,
                                                        executionId=op.entityOperationCalled.parentExecutionId
                                                    )))

    return batch_request, operation_infos


def new_timestamp(dt: datetime) -> timestamp_pb2.Timestamp:
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(dt)
    return ts


def ensure_aware(value: datetime | None) -> datetime | None:
    """Return ``value`` as a timezone-aware datetime, assuming UTC when naive.

    A naive datetime is tagged as UTC; an already-aware datetime is returned
    unchanged. Useful before comparing user-supplied datetimes against the
    SDK's always-aware-UTC timestamps to avoid "can't compare offset-naive and
    offset-aware datetimes".
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def new_create_sub_orchestration_action(
        id: int,
        name: str,
        instance_id: str | None,
        encoded_input: str | None,
        version: str | None,
        parent_trace_context: pb.TraceContext | None = None) -> pb.OrchestratorAction:
    return pb.OrchestratorAction(id=id, createSubOrchestration=pb.CreateSubOrchestrationAction(
        name=name,
        instanceId=instance_id,
        input=get_string_value(encoded_input),
        version=get_string_value(version),
        parentTraceContext=parent_trace_context,
    ))


def is_empty(v: wrappers_pb2.StringValue | None) -> bool:
    return v is None or v.value == ''


def get_orchestration_status_str(status: pb.OrchestrationStatus):
    try:
        const_name = pb.OrchestrationStatus.Name(status)
        if const_name.startswith('ORCHESTRATION_STATUS_'):
            return const_name[len('ORCHESTRATION_STATUS_'):]
    except Exception:
        return "UNKNOWN"
