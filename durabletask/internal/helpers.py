# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import traceback
from datetime import datetime

from google.protobuf import timestamp_pb2, wrappers_pb2

from durabletask.entities import EntityInstanceId
import durabletask.internal.orchestrator_service_pb2 as pb

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


def new_failure_details(ex: Exception, _visited: set[int] | None = None) -> pb.TaskFailureDetails:
    if _visited is None:
        _visited = set()
    _visited.add(id(ex))
    inner: BaseException | None = ex.__cause__ or ex.__context__
    if len(_visited) > 10 or (inner and id(inner) in _visited) or not isinstance(inner, Exception):
        inner = None
    return pb.TaskFailureDetails(
        errorType=type(ex).__name__,
        errorMessage=str(ex),
        stackTrace=wrappers_pb2.StringValue(value=''.join(traceback.format_tb(ex.__traceback__))),
        innerFailure=new_failure_details(inner, _visited) if inner else None
    )


def new_event_sent_event(event_id: int, instance_id: str, input: str):
    return pb.HistoryEvent(
        eventId=event_id,
        timestamp=timestamp_pb2.Timestamp(),
        eventSent=pb.EventSentEvent(
            name="",
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


def new_suspend_event() -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        executionSuspended=pb.ExecutionSuspendedEvent()
    )


def new_resume_event() -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        executionResumed=pb.ExecutionResumedEvent()
    )


def new_terminated_event(*, encoded_output: str | None = None) -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        executionTerminated=pb.ExecutionTerminatedEvent(
            input=get_string_value(encoded_output)
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
        carryover_events: list[pb.HistoryEvent] | None = None) -> pb.OrchestratorAction:
    completeOrchestrationAction = pb.CompleteOrchestrationAction(
        orchestrationStatus=status,
        result=get_string_value(result),
        failureDetails=failure_details,
        carryoverEvents=carryover_events)

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
