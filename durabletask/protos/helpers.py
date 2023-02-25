import traceback

from datetime import datetime
from google.protobuf import timestamp_pb2, wrappers_pb2

from durabletask.protos.orchestrator_service_pb2 import *


def new_orchestrator_started_event(timestamp: datetime | None = None) -> HistoryEvent:
    ts = timestamp_pb2.Timestamp()
    if timestamp is not None:
        ts.FromDatetime(timestamp)
    return HistoryEvent(eventId=-1, timestamp=ts, orchestratorStarted=OrchestratorStartedEvent())


def new_execution_started_event(name: str, instance_id: str, input: str | None = None) -> HistoryEvent:
    input_: wrappers_pb2.StringValue | None = None
    if input is not None:
        input_ = wrappers_pb2.StringValue(value=input)

    return HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        executionStarted=ExecutionStartedEvent(
            name=name, input=input_, orchestrationInstance=OrchestrationInstance(instanceId=instance_id)))


def new_timer_created_event(timer_id: int, fire_at: datetime):
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(fire_at)
    return HistoryEvent(
        eventId=timer_id,
        timestamp=timestamp_pb2.Timestamp(),
        timerCreated=TimerCreatedEvent(fireAt=ts)
    )


def new_timer_fired_event(timer_id: int, fire_at: datetime):
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(fire_at)
    return HistoryEvent(
        eventId=-1,
        timestamp=timestamp_pb2.Timestamp(),
        timerFired=TimerFiredEvent(fireAt=ts, timerId=timer_id)
    )


def new_failure_details(ex: Exception) -> TaskFailureDetails:
    return TaskFailureDetails(
        errorType=type(ex).__name__,
        errorMessage=str(ex),
        stackTrace=wrappers_pb2.StringValue(value=''.join(traceback.format_tb(ex.__traceback__)))
    )


def new_complete_orchestration_action(
        id: int,
        status: OrchestrationStatus,
        result: str | None = None,
        failureDetails: TaskFailureDetails | None = None) -> OrchestratorAction:
    resultWrapper = None
    if result is not None:
        resultWrapper = wrappers_pb2.StringValue(value=result)
    return OrchestratorAction(id=id, completeOrchestration=CompleteOrchestrationAction(
        orchestrationStatus=status,
        result=resultWrapper,
        carryoverEvents=None,  # TODO: Populate carryoverEvents
        failureDetails=failureDetails,
    ))


def create_timer_action(id: int, fire_at: datetime) -> OrchestratorAction:
    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(fire_at)
    return OrchestratorAction(id=id, createTimer=CreateTimerAction(fireAt=timestamp))
