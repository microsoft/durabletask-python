# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import traceback
from datetime import datetime
from typing import Any

import simplejson as json
from google.protobuf import timestamp_pb2, wrappers_pb2

import durabletask.internal.orchestrator_service_pb2 as pb

# TODO: The new_xxx_event methods are only used by test code and should be moved elsewhere


def new_orchestrator_started_event(timestamp: datetime | None = None) -> pb.HistoryEvent:
    ts = timestamp_pb2.Timestamp()
    if timestamp is not None:
        ts.FromDatetime(timestamp)
    return pb.HistoryEvent(eventId=-1, timestamp=ts, orchestratorStarted=pb.OrchestratorStartedEvent())


def new_execution_started_event(name: str, instance_id: str, encoded_input: str | None = None) -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        executionStarted=pb.ExecutionStartedEvent(
            name=name,
            input=get_string_value(encoded_input),
            orchestrationInstance=pb.OrchestrationInstance(instanceId=instance_id)))


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
        encoded_input: str | None = None) -> pb.HistoryEvent:
    return pb.HistoryEvent(
        eventId=event_id,
        timestamp=timestamp_pb2.Timestamp(),
        subOrchestrationInstanceCreated=pb.SubOrchestrationInstanceCreatedEvent(
            name=name,
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


def new_failure_details(ex: Exception) -> pb.TaskFailureDetails:
    return pb.TaskFailureDetails(
        errorType=type(ex).__name__,
        errorMessage=str(ex),
        stackTrace=wrappers_pb2.StringValue(value=''.join(traceback.format_tb(ex.__traceback__)))
    )


def get_string_value(val: str | None) -> wrappers_pb2.StringValue | None:
    if val is None:
        return None
    else:
        return wrappers_pb2.StringValue(value=val)


def new_complete_orchestration_action(
        id: int,
        status: pb.OrchestrationStatus,
        result: str | None = None,
        failure_details: pb.TaskFailureDetails | None = None) -> pb.OrchestratorAction:

    completeOrchestrationAction = pb.CompleteOrchestrationAction(
        orchestrationStatus=status,
        result=get_string_value(result),
        failureDetails=failure_details)

    # TODO: CarryoverEvents

    return pb.OrchestratorAction(id=id, completeOrchestration=completeOrchestrationAction)


def new_create_timer_action(id: int, fire_at: datetime) -> pb.OrchestratorAction:
    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(fire_at)
    return pb.OrchestratorAction(id=id, createTimer=pb.CreateTimerAction(fireAt=timestamp))


def new_schedule_task_action(id: int, name: str, input: Any) -> pb.OrchestratorAction:
    encoded_input = json.dumps(input) if input is not None else None
    return pb.OrchestratorAction(id=id, scheduleTask=pb.ScheduleTaskAction(
        name=name,
        input=get_string_value(encoded_input)
    ))


def new_timestamp(dt: datetime) -> timestamp_pb2.Timestamp:
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(dt)
    return ts


def new_create_sub_orchestration_action(
        id: int,
        name: str,
        instance_id: str | None,
        input: Any) -> pb.OrchestratorAction:
    encoded_input = json.dumps(input) if input is not None else None
    return pb.OrchestratorAction(id=id, createSubOrchestration=pb.CreateSubOrchestrationAction(
        name=name,
        instanceId=instance_id,
        input=get_string_value(encoded_input)
    ))


def is_empty(v: wrappers_pb2.StringValue):
    return v is None or v.value == ''


def get_orchestration_status_str(status: pb.OrchestrationStatus):
    try:
        const_name = pb.OrchestrationStatus.Name(status)
        if const_name.startswith('ORCHESTRATION_STATUS_'):
            return const_name[len('ORCHESTRATION_STATUS_'):]
    except Exception:
        return "UNKNOWN"
