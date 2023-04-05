import simplejson as json
import traceback

from datetime import datetime
from typing import Any
from google.protobuf import timestamp_pb2, wrappers_pb2

from durabletask.protos.orchestrator_service_pb2 import *

# TODO: The new_xxx_event methods are only used by test code and should be moved elsewhere


def new_orchestrator_started_event(timestamp: datetime | None = None) -> HistoryEvent:
    ts = timestamp_pb2.Timestamp()
    if timestamp is not None:
        ts.FromDatetime(timestamp)
    return HistoryEvent(eventId=-1, timestamp=ts, orchestratorStarted=OrchestratorStartedEvent())


def new_execution_started_event(name: str, instance_id: str, encoded_input: str | None = None) -> HistoryEvent:
    return HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        executionStarted=ExecutionStartedEvent(
            name=name, input=get_string_value(encoded_input), orchestrationInstance=OrchestrationInstance(instanceId=instance_id)))


def new_timer_created_event(timer_id: int, fire_at: datetime) -> HistoryEvent:
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(fire_at)
    return HistoryEvent(
        eventId=timer_id,
        timestamp=timestamp_pb2.Timestamp(),
        timerCreated=TimerCreatedEvent(fireAt=ts)
    )


def new_timer_fired_event(timer_id: int, fire_at: datetime) -> HistoryEvent:
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(fire_at)
    return HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        timerFired=TimerFiredEvent(fireAt=ts, timerId=timer_id)
    )


def new_task_scheduled_event(event_id: int, name: str, encoded_input: str | None = None) -> HistoryEvent:
    return HistoryEvent(
        eventId=event_id,
        timestamp=timestamp_pb2.Timestamp(),
        taskScheduled=TaskScheduledEvent(name=name, input=get_string_value(encoded_input))
    )


def new_task_completed_event(event_id: int, encoded_output: str | None = None) -> HistoryEvent:
    return HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        taskCompleted=TaskCompletedEvent(taskScheduledId=event_id, result=get_string_value(encoded_output))
    )


def new_task_failed_event(event_id: int, ex: Exception) -> HistoryEvent:
    return HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        taskFailed=TaskFailedEvent(taskScheduledId=event_id, failureDetails=new_failure_details(ex))
    )


def new_failure_details(ex: Exception) -> TaskFailureDetails:
    return TaskFailureDetails(
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
        status: OrchestrationStatus,
        result: str | None = None,
        failure_details: TaskFailureDetails | None = None) -> OrchestratorAction:

    completeOrchestrationAction = CompleteOrchestrationAction(
        orchestrationStatus=status,
        result=get_string_value(result),
        failureDetails=failure_details)

    # TODO: CarryoverEvents

    return OrchestratorAction(id=id, completeOrchestration=completeOrchestrationAction)


def new_create_timer_action(id: int, fire_at: datetime) -> OrchestratorAction:
    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(fire_at)
    return OrchestratorAction(id=id, createTimer=CreateTimerAction(fireAt=timestamp))


def new_schedule_task_action(id: int, name: str, input: Any) -> OrchestratorAction:
    encoded_input = json.dumps(input) if input is not None else None
    return OrchestratorAction(id=id, scheduleTask=ScheduleTaskAction(
        name=name,
        input=get_string_value(encoded_input)
    ))


def new_timestamp(dt: datetime) -> timestamp_pb2.Timestamp:
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(dt)
    return ts


def is_empty(v: wrappers_pb2.StringValue):
    return v is None or v.value == ''
