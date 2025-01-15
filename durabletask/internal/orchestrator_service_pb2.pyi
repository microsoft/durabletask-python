from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import wrappers_pb2 as _wrappers_pb2
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OrchestrationStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ORCHESTRATION_STATUS_RUNNING: _ClassVar[OrchestrationStatus]
    ORCHESTRATION_STATUS_COMPLETED: _ClassVar[OrchestrationStatus]
    ORCHESTRATION_STATUS_CONTINUED_AS_NEW: _ClassVar[OrchestrationStatus]
    ORCHESTRATION_STATUS_FAILED: _ClassVar[OrchestrationStatus]
    ORCHESTRATION_STATUS_CANCELED: _ClassVar[OrchestrationStatus]
    ORCHESTRATION_STATUS_TERMINATED: _ClassVar[OrchestrationStatus]
    ORCHESTRATION_STATUS_PENDING: _ClassVar[OrchestrationStatus]
    ORCHESTRATION_STATUS_SUSPENDED: _ClassVar[OrchestrationStatus]

class CreateOrchestrationAction(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ERROR: _ClassVar[CreateOrchestrationAction]
    IGNORE: _ClassVar[CreateOrchestrationAction]
    TERMINATE: _ClassVar[CreateOrchestrationAction]
ORCHESTRATION_STATUS_RUNNING: OrchestrationStatus
ORCHESTRATION_STATUS_COMPLETED: OrchestrationStatus
ORCHESTRATION_STATUS_CONTINUED_AS_NEW: OrchestrationStatus
ORCHESTRATION_STATUS_FAILED: OrchestrationStatus
ORCHESTRATION_STATUS_CANCELED: OrchestrationStatus
ORCHESTRATION_STATUS_TERMINATED: OrchestrationStatus
ORCHESTRATION_STATUS_PENDING: OrchestrationStatus
ORCHESTRATION_STATUS_SUSPENDED: OrchestrationStatus
ERROR: CreateOrchestrationAction
IGNORE: CreateOrchestrationAction
TERMINATE: CreateOrchestrationAction

class OrchestrationInstance(_message.Message):
    __slots__ = ("instanceId", "executionId")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONID_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    executionId: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., executionId: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class ActivityRequest(_message.Message):
    __slots__ = ("name", "version", "input", "orchestrationInstance", "taskId", "parentTraceContext")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATIONINSTANCE_FIELD_NUMBER: _ClassVar[int]
    TASKID_FIELD_NUMBER: _ClassVar[int]
    PARENTTRACECONTEXT_FIELD_NUMBER: _ClassVar[int]
    name: str
    version: _wrappers_pb2.StringValue
    input: _wrappers_pb2.StringValue
    orchestrationInstance: OrchestrationInstance
    taskId: int
    parentTraceContext: TraceContext
    def __init__(self, name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., orchestrationInstance: _Optional[_Union[OrchestrationInstance, _Mapping]] = ..., taskId: _Optional[int] = ..., parentTraceContext: _Optional[_Union[TraceContext, _Mapping]] = ...) -> None: ...

class ActivityResponse(_message.Message):
    __slots__ = ("instanceId", "taskId", "result", "failureDetails", "completionToken")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    TASKID_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    FAILUREDETAILS_FIELD_NUMBER: _ClassVar[int]
    COMPLETIONTOKEN_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    taskId: int
    result: _wrappers_pb2.StringValue
    failureDetails: TaskFailureDetails
    completionToken: str
    def __init__(self, instanceId: _Optional[str] = ..., taskId: _Optional[int] = ..., result: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., failureDetails: _Optional[_Union[TaskFailureDetails, _Mapping]] = ..., completionToken: _Optional[str] = ...) -> None: ...

class TaskFailureDetails(_message.Message):
    __slots__ = ("errorType", "errorMessage", "stackTrace", "innerFailure", "isNonRetriable")
    ERRORTYPE_FIELD_NUMBER: _ClassVar[int]
    ERRORMESSAGE_FIELD_NUMBER: _ClassVar[int]
    STACKTRACE_FIELD_NUMBER: _ClassVar[int]
    INNERFAILURE_FIELD_NUMBER: _ClassVar[int]
    ISNONRETRIABLE_FIELD_NUMBER: _ClassVar[int]
    errorType: str
    errorMessage: str
    stackTrace: _wrappers_pb2.StringValue
    innerFailure: TaskFailureDetails
    isNonRetriable: bool
    def __init__(self, errorType: _Optional[str] = ..., errorMessage: _Optional[str] = ..., stackTrace: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., innerFailure: _Optional[_Union[TaskFailureDetails, _Mapping]] = ..., isNonRetriable: bool = ...) -> None: ...

class ParentInstanceInfo(_message.Message):
    __slots__ = ("taskScheduledId", "name", "version", "orchestrationInstance")
    TASKSCHEDULEDID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATIONINSTANCE_FIELD_NUMBER: _ClassVar[int]
    taskScheduledId: int
    name: _wrappers_pb2.StringValue
    version: _wrappers_pb2.StringValue
    orchestrationInstance: OrchestrationInstance
    def __init__(self, taskScheduledId: _Optional[int] = ..., name: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., orchestrationInstance: _Optional[_Union[OrchestrationInstance, _Mapping]] = ...) -> None: ...

class TraceContext(_message.Message):
    __slots__ = ("traceParent", "spanID", "traceState")
    TRACEPARENT_FIELD_NUMBER: _ClassVar[int]
    SPANID_FIELD_NUMBER: _ClassVar[int]
    TRACESTATE_FIELD_NUMBER: _ClassVar[int]
    traceParent: str
    spanID: str
    traceState: _wrappers_pb2.StringValue
    def __init__(self, traceParent: _Optional[str] = ..., spanID: _Optional[str] = ..., traceState: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class ExecutionStartedEvent(_message.Message):
    __slots__ = ("name", "version", "input", "orchestrationInstance", "parentInstance", "scheduledStartTimestamp", "parentTraceContext", "orchestrationSpanID")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATIONINSTANCE_FIELD_NUMBER: _ClassVar[int]
    PARENTINSTANCE_FIELD_NUMBER: _ClassVar[int]
    SCHEDULEDSTARTTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    PARENTTRACECONTEXT_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATIONSPANID_FIELD_NUMBER: _ClassVar[int]
    name: str
    version: _wrappers_pb2.StringValue
    input: _wrappers_pb2.StringValue
    orchestrationInstance: OrchestrationInstance
    parentInstance: ParentInstanceInfo
    scheduledStartTimestamp: _timestamp_pb2.Timestamp
    parentTraceContext: TraceContext
    orchestrationSpanID: _wrappers_pb2.StringValue
    def __init__(self, name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., orchestrationInstance: _Optional[_Union[OrchestrationInstance, _Mapping]] = ..., parentInstance: _Optional[_Union[ParentInstanceInfo, _Mapping]] = ..., scheduledStartTimestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., parentTraceContext: _Optional[_Union[TraceContext, _Mapping]] = ..., orchestrationSpanID: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class ExecutionCompletedEvent(_message.Message):
    __slots__ = ("orchestrationStatus", "result", "failureDetails")
    ORCHESTRATIONSTATUS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    FAILUREDETAILS_FIELD_NUMBER: _ClassVar[int]
    orchestrationStatus: OrchestrationStatus
    result: _wrappers_pb2.StringValue
    failureDetails: TaskFailureDetails
    def __init__(self, orchestrationStatus: _Optional[_Union[OrchestrationStatus, str]] = ..., result: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., failureDetails: _Optional[_Union[TaskFailureDetails, _Mapping]] = ...) -> None: ...

class ExecutionTerminatedEvent(_message.Message):
    __slots__ = ("input", "recurse")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    RECURSE_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    recurse: bool
    def __init__(self, input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., recurse: bool = ...) -> None: ...

class TaskScheduledEvent(_message.Message):
    __slots__ = ("name", "version", "input", "parentTraceContext")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    PARENTTRACECONTEXT_FIELD_NUMBER: _ClassVar[int]
    name: str
    version: _wrappers_pb2.StringValue
    input: _wrappers_pb2.StringValue
    parentTraceContext: TraceContext
    def __init__(self, name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., parentTraceContext: _Optional[_Union[TraceContext, _Mapping]] = ...) -> None: ...

class TaskCompletedEvent(_message.Message):
    __slots__ = ("taskScheduledId", "result")
    TASKSCHEDULEDID_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    taskScheduledId: int
    result: _wrappers_pb2.StringValue
    def __init__(self, taskScheduledId: _Optional[int] = ..., result: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class TaskFailedEvent(_message.Message):
    __slots__ = ("taskScheduledId", "failureDetails")
    TASKSCHEDULEDID_FIELD_NUMBER: _ClassVar[int]
    FAILUREDETAILS_FIELD_NUMBER: _ClassVar[int]
    taskScheduledId: int
    failureDetails: TaskFailureDetails
    def __init__(self, taskScheduledId: _Optional[int] = ..., failureDetails: _Optional[_Union[TaskFailureDetails, _Mapping]] = ...) -> None: ...

class SubOrchestrationInstanceCreatedEvent(_message.Message):
    __slots__ = ("instanceId", "name", "version", "input", "parentTraceContext")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    PARENTTRACECONTEXT_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    name: str
    version: _wrappers_pb2.StringValue
    input: _wrappers_pb2.StringValue
    parentTraceContext: TraceContext
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., parentTraceContext: _Optional[_Union[TraceContext, _Mapping]] = ...) -> None: ...

class SubOrchestrationInstanceCompletedEvent(_message.Message):
    __slots__ = ("taskScheduledId", "result")
    TASKSCHEDULEDID_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    taskScheduledId: int
    result: _wrappers_pb2.StringValue
    def __init__(self, taskScheduledId: _Optional[int] = ..., result: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class SubOrchestrationInstanceFailedEvent(_message.Message):
    __slots__ = ("taskScheduledId", "failureDetails")
    TASKSCHEDULEDID_FIELD_NUMBER: _ClassVar[int]
    FAILUREDETAILS_FIELD_NUMBER: _ClassVar[int]
    taskScheduledId: int
    failureDetails: TaskFailureDetails
    def __init__(self, taskScheduledId: _Optional[int] = ..., failureDetails: _Optional[_Union[TaskFailureDetails, _Mapping]] = ...) -> None: ...

class TimerCreatedEvent(_message.Message):
    __slots__ = ("fireAt",)
    FIREAT_FIELD_NUMBER: _ClassVar[int]
    fireAt: _timestamp_pb2.Timestamp
    def __init__(self, fireAt: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class TimerFiredEvent(_message.Message):
    __slots__ = ("fireAt", "timerId")
    FIREAT_FIELD_NUMBER: _ClassVar[int]
    TIMERID_FIELD_NUMBER: _ClassVar[int]
    fireAt: _timestamp_pb2.Timestamp
    timerId: int
    def __init__(self, fireAt: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., timerId: _Optional[int] = ...) -> None: ...

class OrchestratorStartedEvent(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class OrchestratorCompletedEvent(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class EventSentEvent(_message.Message):
    __slots__ = ("instanceId", "name", "input")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    name: str
    input: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class EventRaisedEvent(_message.Message):
    __slots__ = ("name", "input")
    NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    name: str
    input: _wrappers_pb2.StringValue
    def __init__(self, name: _Optional[str] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class GenericEvent(_message.Message):
    __slots__ = ("data",)
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: _wrappers_pb2.StringValue
    def __init__(self, data: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class HistoryStateEvent(_message.Message):
    __slots__ = ("orchestrationState",)
    ORCHESTRATIONSTATE_FIELD_NUMBER: _ClassVar[int]
    orchestrationState: OrchestrationState
    def __init__(self, orchestrationState: _Optional[_Union[OrchestrationState, _Mapping]] = ...) -> None: ...

class ContinueAsNewEvent(_message.Message):
    __slots__ = ("input",)
    INPUT_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    def __init__(self, input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class ExecutionSuspendedEvent(_message.Message):
    __slots__ = ("input",)
    INPUT_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    def __init__(self, input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class ExecutionResumedEvent(_message.Message):
    __slots__ = ("input",)
    INPUT_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    def __init__(self, input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class HistoryEvent(_message.Message):
    __slots__ = ("eventId", "timestamp", "executionStarted", "executionCompleted", "executionTerminated", "taskScheduled", "taskCompleted", "taskFailed", "subOrchestrationInstanceCreated", "subOrchestrationInstanceCompleted", "subOrchestrationInstanceFailed", "timerCreated", "timerFired", "orchestratorStarted", "orchestratorCompleted", "eventSent", "eventRaised", "genericEvent", "historyState", "continueAsNew", "executionSuspended", "executionResumed")
    EVENTID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONSTARTED_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONCOMPLETED_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONTERMINATED_FIELD_NUMBER: _ClassVar[int]
    TASKSCHEDULED_FIELD_NUMBER: _ClassVar[int]
    TASKCOMPLETED_FIELD_NUMBER: _ClassVar[int]
    TASKFAILED_FIELD_NUMBER: _ClassVar[int]
    SUBORCHESTRATIONINSTANCECREATED_FIELD_NUMBER: _ClassVar[int]
    SUBORCHESTRATIONINSTANCECOMPLETED_FIELD_NUMBER: _ClassVar[int]
    SUBORCHESTRATIONINSTANCEFAILED_FIELD_NUMBER: _ClassVar[int]
    TIMERCREATED_FIELD_NUMBER: _ClassVar[int]
    TIMERFIRED_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATORSTARTED_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATORCOMPLETED_FIELD_NUMBER: _ClassVar[int]
    EVENTSENT_FIELD_NUMBER: _ClassVar[int]
    EVENTRAISED_FIELD_NUMBER: _ClassVar[int]
    GENERICEVENT_FIELD_NUMBER: _ClassVar[int]
    HISTORYSTATE_FIELD_NUMBER: _ClassVar[int]
    CONTINUEASNEW_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONSUSPENDED_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONRESUMED_FIELD_NUMBER: _ClassVar[int]
    eventId: int
    timestamp: _timestamp_pb2.Timestamp
    executionStarted: ExecutionStartedEvent
    executionCompleted: ExecutionCompletedEvent
    executionTerminated: ExecutionTerminatedEvent
    taskScheduled: TaskScheduledEvent
    taskCompleted: TaskCompletedEvent
    taskFailed: TaskFailedEvent
    subOrchestrationInstanceCreated: SubOrchestrationInstanceCreatedEvent
    subOrchestrationInstanceCompleted: SubOrchestrationInstanceCompletedEvent
    subOrchestrationInstanceFailed: SubOrchestrationInstanceFailedEvent
    timerCreated: TimerCreatedEvent
    timerFired: TimerFiredEvent
    orchestratorStarted: OrchestratorStartedEvent
    orchestratorCompleted: OrchestratorCompletedEvent
    eventSent: EventSentEvent
    eventRaised: EventRaisedEvent
    genericEvent: GenericEvent
    historyState: HistoryStateEvent
    continueAsNew: ContinueAsNewEvent
    executionSuspended: ExecutionSuspendedEvent
    executionResumed: ExecutionResumedEvent
    def __init__(self, eventId: _Optional[int] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., executionStarted: _Optional[_Union[ExecutionStartedEvent, _Mapping]] = ..., executionCompleted: _Optional[_Union[ExecutionCompletedEvent, _Mapping]] = ..., executionTerminated: _Optional[_Union[ExecutionTerminatedEvent, _Mapping]] = ..., taskScheduled: _Optional[_Union[TaskScheduledEvent, _Mapping]] = ..., taskCompleted: _Optional[_Union[TaskCompletedEvent, _Mapping]] = ..., taskFailed: _Optional[_Union[TaskFailedEvent, _Mapping]] = ..., subOrchestrationInstanceCreated: _Optional[_Union[SubOrchestrationInstanceCreatedEvent, _Mapping]] = ..., subOrchestrationInstanceCompleted: _Optional[_Union[SubOrchestrationInstanceCompletedEvent, _Mapping]] = ..., subOrchestrationInstanceFailed: _Optional[_Union[SubOrchestrationInstanceFailedEvent, _Mapping]] = ..., timerCreated: _Optional[_Union[TimerCreatedEvent, _Mapping]] = ..., timerFired: _Optional[_Union[TimerFiredEvent, _Mapping]] = ..., orchestratorStarted: _Optional[_Union[OrchestratorStartedEvent, _Mapping]] = ..., orchestratorCompleted: _Optional[_Union[OrchestratorCompletedEvent, _Mapping]] = ..., eventSent: _Optional[_Union[EventSentEvent, _Mapping]] = ..., eventRaised: _Optional[_Union[EventRaisedEvent, _Mapping]] = ..., genericEvent: _Optional[_Union[GenericEvent, _Mapping]] = ..., historyState: _Optional[_Union[HistoryStateEvent, _Mapping]] = ..., continueAsNew: _Optional[_Union[ContinueAsNewEvent, _Mapping]] = ..., executionSuspended: _Optional[_Union[ExecutionSuspendedEvent, _Mapping]] = ..., executionResumed: _Optional[_Union[ExecutionResumedEvent, _Mapping]] = ...) -> None: ...

class ScheduleTaskAction(_message.Message):
    __slots__ = ("name", "version", "input")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    name: str
    version: _wrappers_pb2.StringValue
    input: _wrappers_pb2.StringValue
    def __init__(self, name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class CreateSubOrchestrationAction(_message.Message):
    __slots__ = ("instanceId", "name", "version", "input")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    name: str
    version: _wrappers_pb2.StringValue
    input: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class CreateTimerAction(_message.Message):
    __slots__ = ("fireAt",)
    FIREAT_FIELD_NUMBER: _ClassVar[int]
    fireAt: _timestamp_pb2.Timestamp
    def __init__(self, fireAt: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class SendEventAction(_message.Message):
    __slots__ = ("instance", "name", "data")
    INSTANCE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    instance: OrchestrationInstance
    name: str
    data: _wrappers_pb2.StringValue
    def __init__(self, instance: _Optional[_Union[OrchestrationInstance, _Mapping]] = ..., name: _Optional[str] = ..., data: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class CompleteOrchestrationAction(_message.Message):
    __slots__ = ("orchestrationStatus", "result", "details", "newVersion", "carryoverEvents", "failureDetails")
    ORCHESTRATIONSTATUS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    NEWVERSION_FIELD_NUMBER: _ClassVar[int]
    CARRYOVEREVENTS_FIELD_NUMBER: _ClassVar[int]
    FAILUREDETAILS_FIELD_NUMBER: _ClassVar[int]
    orchestrationStatus: OrchestrationStatus
    result: _wrappers_pb2.StringValue
    details: _wrappers_pb2.StringValue
    newVersion: _wrappers_pb2.StringValue
    carryoverEvents: _containers.RepeatedCompositeFieldContainer[HistoryEvent]
    failureDetails: TaskFailureDetails
    def __init__(self, orchestrationStatus: _Optional[_Union[OrchestrationStatus, str]] = ..., result: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., details: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., newVersion: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., carryoverEvents: _Optional[_Iterable[_Union[HistoryEvent, _Mapping]]] = ..., failureDetails: _Optional[_Union[TaskFailureDetails, _Mapping]] = ...) -> None: ...

class TerminateOrchestrationAction(_message.Message):
    __slots__ = ("instanceId", "reason", "recurse")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    RECURSE_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    reason: _wrappers_pb2.StringValue
    recurse: bool
    def __init__(self, instanceId: _Optional[str] = ..., reason: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., recurse: bool = ...) -> None: ...

class OrchestratorAction(_message.Message):
    __slots__ = ("id", "scheduleTask", "createSubOrchestration", "createTimer", "sendEvent", "completeOrchestration", "terminateOrchestration")
    ID_FIELD_NUMBER: _ClassVar[int]
    SCHEDULETASK_FIELD_NUMBER: _ClassVar[int]
    CREATESUBORCHESTRATION_FIELD_NUMBER: _ClassVar[int]
    CREATETIMER_FIELD_NUMBER: _ClassVar[int]
    SENDEVENT_FIELD_NUMBER: _ClassVar[int]
    COMPLETEORCHESTRATION_FIELD_NUMBER: _ClassVar[int]
    TERMINATEORCHESTRATION_FIELD_NUMBER: _ClassVar[int]
    id: int
    scheduleTask: ScheduleTaskAction
    createSubOrchestration: CreateSubOrchestrationAction
    createTimer: CreateTimerAction
    sendEvent: SendEventAction
    completeOrchestration: CompleteOrchestrationAction
    terminateOrchestration: TerminateOrchestrationAction
    def __init__(self, id: _Optional[int] = ..., scheduleTask: _Optional[_Union[ScheduleTaskAction, _Mapping]] = ..., createSubOrchestration: _Optional[_Union[CreateSubOrchestrationAction, _Mapping]] = ..., createTimer: _Optional[_Union[CreateTimerAction, _Mapping]] = ..., sendEvent: _Optional[_Union[SendEventAction, _Mapping]] = ..., completeOrchestration: _Optional[_Union[CompleteOrchestrationAction, _Mapping]] = ..., terminateOrchestration: _Optional[_Union[TerminateOrchestrationAction, _Mapping]] = ...) -> None: ...

class OrchestratorRequest(_message.Message):
    __slots__ = ("instanceId", "executionId", "pastEvents", "newEvents", "entityParameters")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONID_FIELD_NUMBER: _ClassVar[int]
    PASTEVENTS_FIELD_NUMBER: _ClassVar[int]
    NEWEVENTS_FIELD_NUMBER: _ClassVar[int]
    ENTITYPARAMETERS_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    executionId: _wrappers_pb2.StringValue
    pastEvents: _containers.RepeatedCompositeFieldContainer[HistoryEvent]
    newEvents: _containers.RepeatedCompositeFieldContainer[HistoryEvent]
    entityParameters: OrchestratorEntityParameters
    def __init__(self, instanceId: _Optional[str] = ..., executionId: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., pastEvents: _Optional[_Iterable[_Union[HistoryEvent, _Mapping]]] = ..., newEvents: _Optional[_Iterable[_Union[HistoryEvent, _Mapping]]] = ..., entityParameters: _Optional[_Union[OrchestratorEntityParameters, _Mapping]] = ...) -> None: ...

class OrchestratorResponse(_message.Message):
    __slots__ = ("instanceId", "actions", "customStatus", "completionToken")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    ACTIONS_FIELD_NUMBER: _ClassVar[int]
    CUSTOMSTATUS_FIELD_NUMBER: _ClassVar[int]
    COMPLETIONTOKEN_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    actions: _containers.RepeatedCompositeFieldContainer[OrchestratorAction]
    customStatus: _wrappers_pb2.StringValue
    completionToken: str
    def __init__(self, instanceId: _Optional[str] = ..., actions: _Optional[_Iterable[_Union[OrchestratorAction, _Mapping]]] = ..., customStatus: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., completionToken: _Optional[str] = ...) -> None: ...

class CreateInstanceRequest(_message.Message):
    __slots__ = ("instanceId", "name", "version", "input", "scheduledStartTimestamp", "orchestrationIdReusePolicy", "executionId", "tags")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    SCHEDULEDSTARTTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATIONIDREUSEPOLICY_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONID_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    name: str
    version: _wrappers_pb2.StringValue
    input: _wrappers_pb2.StringValue
    scheduledStartTimestamp: _timestamp_pb2.Timestamp
    orchestrationIdReusePolicy: OrchestrationIdReusePolicy
    executionId: _wrappers_pb2.StringValue
    tags: _containers.ScalarMap[str, str]
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., scheduledStartTimestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., orchestrationIdReusePolicy: _Optional[_Union[OrchestrationIdReusePolicy, _Mapping]] = ..., executionId: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., tags: _Optional[_Mapping[str, str]] = ...) -> None: ...

class OrchestrationIdReusePolicy(_message.Message):
    __slots__ = ("operationStatus", "action")
    OPERATIONSTATUS_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    operationStatus: _containers.RepeatedScalarFieldContainer[OrchestrationStatus]
    action: CreateOrchestrationAction
    def __init__(self, operationStatus: _Optional[_Iterable[_Union[OrchestrationStatus, str]]] = ..., action: _Optional[_Union[CreateOrchestrationAction, str]] = ...) -> None: ...

class CreateInstanceResponse(_message.Message):
    __slots__ = ("instanceId",)
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    def __init__(self, instanceId: _Optional[str] = ...) -> None: ...

class GetInstanceRequest(_message.Message):
    __slots__ = ("instanceId", "getInputsAndOutputs")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    GETINPUTSANDOUTPUTS_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    getInputsAndOutputs: bool
    def __init__(self, instanceId: _Optional[str] = ..., getInputsAndOutputs: bool = ...) -> None: ...

class GetInstanceResponse(_message.Message):
    __slots__ = ("exists", "orchestrationState")
    EXISTS_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATIONSTATE_FIELD_NUMBER: _ClassVar[int]
    exists: bool
    orchestrationState: OrchestrationState
    def __init__(self, exists: bool = ..., orchestrationState: _Optional[_Union[OrchestrationState, _Mapping]] = ...) -> None: ...

class RewindInstanceRequest(_message.Message):
    __slots__ = ("instanceId", "reason")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    reason: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., reason: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class RewindInstanceResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class OrchestrationState(_message.Message):
    __slots__ = ("instanceId", "name", "version", "orchestrationStatus", "scheduledStartTimestamp", "createdTimestamp", "lastUpdatedTimestamp", "input", "output", "customStatus", "failureDetails", "executionId", "completedTimestamp", "parentInstanceId")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATIONSTATUS_FIELD_NUMBER: _ClassVar[int]
    SCHEDULEDSTARTTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    CREATEDTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    LASTUPDATEDTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    CUSTOMSTATUS_FIELD_NUMBER: _ClassVar[int]
    FAILUREDETAILS_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONID_FIELD_NUMBER: _ClassVar[int]
    COMPLETEDTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    PARENTINSTANCEID_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    name: str
    version: _wrappers_pb2.StringValue
    orchestrationStatus: OrchestrationStatus
    scheduledStartTimestamp: _timestamp_pb2.Timestamp
    createdTimestamp: _timestamp_pb2.Timestamp
    lastUpdatedTimestamp: _timestamp_pb2.Timestamp
    input: _wrappers_pb2.StringValue
    output: _wrappers_pb2.StringValue
    customStatus: _wrappers_pb2.StringValue
    failureDetails: TaskFailureDetails
    executionId: _wrappers_pb2.StringValue
    completedTimestamp: _timestamp_pb2.Timestamp
    parentInstanceId: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., orchestrationStatus: _Optional[_Union[OrchestrationStatus, str]] = ..., scheduledStartTimestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., createdTimestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., lastUpdatedTimestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., output: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., customStatus: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., failureDetails: _Optional[_Union[TaskFailureDetails, _Mapping]] = ..., executionId: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., completedTimestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., parentInstanceId: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class RaiseEventRequest(_message.Message):
    __slots__ = ("instanceId", "name", "input")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    name: str
    input: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class RaiseEventResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TerminateRequest(_message.Message):
    __slots__ = ("instanceId", "output", "recursive")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    RECURSIVE_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    output: _wrappers_pb2.StringValue
    recursive: bool
    def __init__(self, instanceId: _Optional[str] = ..., output: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., recursive: bool = ...) -> None: ...

class TerminateResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SuspendRequest(_message.Message):
    __slots__ = ("instanceId", "reason")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    reason: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., reason: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class SuspendResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ResumeRequest(_message.Message):
    __slots__ = ("instanceId", "reason")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    reason: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., reason: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class ResumeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class QueryInstancesRequest(_message.Message):
    __slots__ = ("query",)
    QUERY_FIELD_NUMBER: _ClassVar[int]
    query: InstanceQuery
    def __init__(self, query: _Optional[_Union[InstanceQuery, _Mapping]] = ...) -> None: ...

class InstanceQuery(_message.Message):
    __slots__ = ("runtimeStatus", "createdTimeFrom", "createdTimeTo", "taskHubNames", "maxInstanceCount", "continuationToken", "instanceIdPrefix", "fetchInputsAndOutputs")
    RUNTIMESTATUS_FIELD_NUMBER: _ClassVar[int]
    CREATEDTIMEFROM_FIELD_NUMBER: _ClassVar[int]
    CREATEDTIMETO_FIELD_NUMBER: _ClassVar[int]
    TASKHUBNAMES_FIELD_NUMBER: _ClassVar[int]
    MAXINSTANCECOUNT_FIELD_NUMBER: _ClassVar[int]
    CONTINUATIONTOKEN_FIELD_NUMBER: _ClassVar[int]
    INSTANCEIDPREFIX_FIELD_NUMBER: _ClassVar[int]
    FETCHINPUTSANDOUTPUTS_FIELD_NUMBER: _ClassVar[int]
    runtimeStatus: _containers.RepeatedScalarFieldContainer[OrchestrationStatus]
    createdTimeFrom: _timestamp_pb2.Timestamp
    createdTimeTo: _timestamp_pb2.Timestamp
    taskHubNames: _containers.RepeatedCompositeFieldContainer[_wrappers_pb2.StringValue]
    maxInstanceCount: int
    continuationToken: _wrappers_pb2.StringValue
    instanceIdPrefix: _wrappers_pb2.StringValue
    fetchInputsAndOutputs: bool
    def __init__(self, runtimeStatus: _Optional[_Iterable[_Union[OrchestrationStatus, str]]] = ..., createdTimeFrom: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., createdTimeTo: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., taskHubNames: _Optional[_Iterable[_Union[_wrappers_pb2.StringValue, _Mapping]]] = ..., maxInstanceCount: _Optional[int] = ..., continuationToken: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., instanceIdPrefix: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., fetchInputsAndOutputs: bool = ...) -> None: ...

class QueryInstancesResponse(_message.Message):
    __slots__ = ("orchestrationState", "continuationToken")
    ORCHESTRATIONSTATE_FIELD_NUMBER: _ClassVar[int]
    CONTINUATIONTOKEN_FIELD_NUMBER: _ClassVar[int]
    orchestrationState: _containers.RepeatedCompositeFieldContainer[OrchestrationState]
    continuationToken: _wrappers_pb2.StringValue
    def __init__(self, orchestrationState: _Optional[_Iterable[_Union[OrchestrationState, _Mapping]]] = ..., continuationToken: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class PurgeInstancesRequest(_message.Message):
    __slots__ = ("instanceId", "purgeInstanceFilter", "recursive")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    PURGEINSTANCEFILTER_FIELD_NUMBER: _ClassVar[int]
    RECURSIVE_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    purgeInstanceFilter: PurgeInstanceFilter
    recursive: bool
    def __init__(self, instanceId: _Optional[str] = ..., purgeInstanceFilter: _Optional[_Union[PurgeInstanceFilter, _Mapping]] = ..., recursive: bool = ...) -> None: ...

class PurgeInstanceFilter(_message.Message):
    __slots__ = ("createdTimeFrom", "createdTimeTo", "runtimeStatus")
    CREATEDTIMEFROM_FIELD_NUMBER: _ClassVar[int]
    CREATEDTIMETO_FIELD_NUMBER: _ClassVar[int]
    RUNTIMESTATUS_FIELD_NUMBER: _ClassVar[int]
    createdTimeFrom: _timestamp_pb2.Timestamp
    createdTimeTo: _timestamp_pb2.Timestamp
    runtimeStatus: _containers.RepeatedScalarFieldContainer[OrchestrationStatus]
    def __init__(self, createdTimeFrom: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., createdTimeTo: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., runtimeStatus: _Optional[_Iterable[_Union[OrchestrationStatus, str]]] = ...) -> None: ...

class PurgeInstancesResponse(_message.Message):
    __slots__ = ("deletedInstanceCount",)
    DELETEDINSTANCECOUNT_FIELD_NUMBER: _ClassVar[int]
    deletedInstanceCount: int
    def __init__(self, deletedInstanceCount: _Optional[int] = ...) -> None: ...

class CreateTaskHubRequest(_message.Message):
    __slots__ = ("recreateIfExists",)
    RECREATEIFEXISTS_FIELD_NUMBER: _ClassVar[int]
    recreateIfExists: bool
    def __init__(self, recreateIfExists: bool = ...) -> None: ...

class CreateTaskHubResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DeleteTaskHubRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DeleteTaskHubResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SignalEntityRequest(_message.Message):
    __slots__ = ("instanceId", "name", "input", "requestId", "scheduledTime")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    SCHEDULEDTIME_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    name: str
    input: _wrappers_pb2.StringValue
    requestId: str
    scheduledTime: _timestamp_pb2.Timestamp
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., requestId: _Optional[str] = ..., scheduledTime: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class SignalEntityResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetEntityRequest(_message.Message):
    __slots__ = ("instanceId", "includeState")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    INCLUDESTATE_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    includeState: bool
    def __init__(self, instanceId: _Optional[str] = ..., includeState: bool = ...) -> None: ...

class GetEntityResponse(_message.Message):
    __slots__ = ("exists", "entity")
    EXISTS_FIELD_NUMBER: _ClassVar[int]
    ENTITY_FIELD_NUMBER: _ClassVar[int]
    exists: bool
    entity: EntityMetadata
    def __init__(self, exists: bool = ..., entity: _Optional[_Union[EntityMetadata, _Mapping]] = ...) -> None: ...

class EntityQuery(_message.Message):
    __slots__ = ("instanceIdStartsWith", "lastModifiedFrom", "lastModifiedTo", "includeState", "includeTransient", "pageSize", "continuationToken")
    INSTANCEIDSTARTSWITH_FIELD_NUMBER: _ClassVar[int]
    LASTMODIFIEDFROM_FIELD_NUMBER: _ClassVar[int]
    LASTMODIFIEDTO_FIELD_NUMBER: _ClassVar[int]
    INCLUDESTATE_FIELD_NUMBER: _ClassVar[int]
    INCLUDETRANSIENT_FIELD_NUMBER: _ClassVar[int]
    PAGESIZE_FIELD_NUMBER: _ClassVar[int]
    CONTINUATIONTOKEN_FIELD_NUMBER: _ClassVar[int]
    instanceIdStartsWith: _wrappers_pb2.StringValue
    lastModifiedFrom: _timestamp_pb2.Timestamp
    lastModifiedTo: _timestamp_pb2.Timestamp
    includeState: bool
    includeTransient: bool
    pageSize: _wrappers_pb2.Int32Value
    continuationToken: _wrappers_pb2.StringValue
    def __init__(self, instanceIdStartsWith: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., lastModifiedFrom: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., lastModifiedTo: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., includeState: bool = ..., includeTransient: bool = ..., pageSize: _Optional[_Union[_wrappers_pb2.Int32Value, _Mapping]] = ..., continuationToken: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class QueryEntitiesRequest(_message.Message):
    __slots__ = ("query",)
    QUERY_FIELD_NUMBER: _ClassVar[int]
    query: EntityQuery
    def __init__(self, query: _Optional[_Union[EntityQuery, _Mapping]] = ...) -> None: ...

class QueryEntitiesResponse(_message.Message):
    __slots__ = ("entities", "continuationToken")
    ENTITIES_FIELD_NUMBER: _ClassVar[int]
    CONTINUATIONTOKEN_FIELD_NUMBER: _ClassVar[int]
    entities: _containers.RepeatedCompositeFieldContainer[EntityMetadata]
    continuationToken: _wrappers_pb2.StringValue
    def __init__(self, entities: _Optional[_Iterable[_Union[EntityMetadata, _Mapping]]] = ..., continuationToken: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class EntityMetadata(_message.Message):
    __slots__ = ("instanceId", "lastModifiedTime", "backlogQueueSize", "lockedBy", "serializedState")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    LASTMODIFIEDTIME_FIELD_NUMBER: _ClassVar[int]
    BACKLOGQUEUESIZE_FIELD_NUMBER: _ClassVar[int]
    LOCKEDBY_FIELD_NUMBER: _ClassVar[int]
    SERIALIZEDSTATE_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    lastModifiedTime: _timestamp_pb2.Timestamp
    backlogQueueSize: int
    lockedBy: _wrappers_pb2.StringValue
    serializedState: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., lastModifiedTime: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., backlogQueueSize: _Optional[int] = ..., lockedBy: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., serializedState: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class CleanEntityStorageRequest(_message.Message):
    __slots__ = ("continuationToken", "removeEmptyEntities", "releaseOrphanedLocks")
    CONTINUATIONTOKEN_FIELD_NUMBER: _ClassVar[int]
    REMOVEEMPTYENTITIES_FIELD_NUMBER: _ClassVar[int]
    RELEASEORPHANEDLOCKS_FIELD_NUMBER: _ClassVar[int]
    continuationToken: _wrappers_pb2.StringValue
    removeEmptyEntities: bool
    releaseOrphanedLocks: bool
    def __init__(self, continuationToken: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., removeEmptyEntities: bool = ..., releaseOrphanedLocks: bool = ...) -> None: ...

class CleanEntityStorageResponse(_message.Message):
    __slots__ = ("continuationToken", "emptyEntitiesRemoved", "orphanedLocksReleased")
    CONTINUATIONTOKEN_FIELD_NUMBER: _ClassVar[int]
    EMPTYENTITIESREMOVED_FIELD_NUMBER: _ClassVar[int]
    ORPHANEDLOCKSRELEASED_FIELD_NUMBER: _ClassVar[int]
    continuationToken: _wrappers_pb2.StringValue
    emptyEntitiesRemoved: int
    orphanedLocksReleased: int
    def __init__(self, continuationToken: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., emptyEntitiesRemoved: _Optional[int] = ..., orphanedLocksReleased: _Optional[int] = ...) -> None: ...

class OrchestratorEntityParameters(_message.Message):
    __slots__ = ("entityMessageReorderWindow",)
    ENTITYMESSAGEREORDERWINDOW_FIELD_NUMBER: _ClassVar[int]
    entityMessageReorderWindow: _duration_pb2.Duration
    def __init__(self, entityMessageReorderWindow: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class EntityBatchRequest(_message.Message):
    __slots__ = ("instanceId", "entityState", "operations")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    ENTITYSTATE_FIELD_NUMBER: _ClassVar[int]
    OPERATIONS_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    entityState: _wrappers_pb2.StringValue
    operations: _containers.RepeatedCompositeFieldContainer[OperationRequest]
    def __init__(self, instanceId: _Optional[str] = ..., entityState: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., operations: _Optional[_Iterable[_Union[OperationRequest, _Mapping]]] = ...) -> None: ...

class EntityBatchResult(_message.Message):
    __slots__ = ("results", "actions", "entityState", "failureDetails")
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    ACTIONS_FIELD_NUMBER: _ClassVar[int]
    ENTITYSTATE_FIELD_NUMBER: _ClassVar[int]
    FAILUREDETAILS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[OperationResult]
    actions: _containers.RepeatedCompositeFieldContainer[OperationAction]
    entityState: _wrappers_pb2.StringValue
    failureDetails: TaskFailureDetails
    def __init__(self, results: _Optional[_Iterable[_Union[OperationResult, _Mapping]]] = ..., actions: _Optional[_Iterable[_Union[OperationAction, _Mapping]]] = ..., entityState: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., failureDetails: _Optional[_Union[TaskFailureDetails, _Mapping]] = ...) -> None: ...

class OperationRequest(_message.Message):
    __slots__ = ("operation", "requestId", "input")
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    operation: str
    requestId: str
    input: _wrappers_pb2.StringValue
    def __init__(self, operation: _Optional[str] = ..., requestId: _Optional[str] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class OperationResult(_message.Message):
    __slots__ = ("success", "failure")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    success: OperationResultSuccess
    failure: OperationResultFailure
    def __init__(self, success: _Optional[_Union[OperationResultSuccess, _Mapping]] = ..., failure: _Optional[_Union[OperationResultFailure, _Mapping]] = ...) -> None: ...

class OperationResultSuccess(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: _wrappers_pb2.StringValue
    def __init__(self, result: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class OperationResultFailure(_message.Message):
    __slots__ = ("failureDetails",)
    FAILUREDETAILS_FIELD_NUMBER: _ClassVar[int]
    failureDetails: TaskFailureDetails
    def __init__(self, failureDetails: _Optional[_Union[TaskFailureDetails, _Mapping]] = ...) -> None: ...

class OperationAction(_message.Message):
    __slots__ = ("id", "sendSignal", "startNewOrchestration")
    ID_FIELD_NUMBER: _ClassVar[int]
    SENDSIGNAL_FIELD_NUMBER: _ClassVar[int]
    STARTNEWORCHESTRATION_FIELD_NUMBER: _ClassVar[int]
    id: int
    sendSignal: SendSignalAction
    startNewOrchestration: StartNewOrchestrationAction
    def __init__(self, id: _Optional[int] = ..., sendSignal: _Optional[_Union[SendSignalAction, _Mapping]] = ..., startNewOrchestration: _Optional[_Union[StartNewOrchestrationAction, _Mapping]] = ...) -> None: ...

class SendSignalAction(_message.Message):
    __slots__ = ("instanceId", "name", "input", "scheduledTime")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    SCHEDULEDTIME_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    name: str
    input: _wrappers_pb2.StringValue
    scheduledTime: _timestamp_pb2.Timestamp
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., scheduledTime: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class StartNewOrchestrationAction(_message.Message):
    __slots__ = ("instanceId", "name", "version", "input", "scheduledTime")
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    SCHEDULEDTIME_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    name: str
    version: _wrappers_pb2.StringValue
    input: _wrappers_pb2.StringValue
    scheduledTime: _timestamp_pb2.Timestamp
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., scheduledTime: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class GetWorkItemsRequest(_message.Message):
    __slots__ = ("maxConcurrentOrchestrationWorkItems", "maxConcurrentActivityWorkItems")
    MAXCONCURRENTORCHESTRATIONWORKITEMS_FIELD_NUMBER: _ClassVar[int]
    MAXCONCURRENTACTIVITYWORKITEMS_FIELD_NUMBER: _ClassVar[int]
    maxConcurrentOrchestrationWorkItems: int
    maxConcurrentActivityWorkItems: int
    def __init__(self, maxConcurrentOrchestrationWorkItems: _Optional[int] = ..., maxConcurrentActivityWorkItems: _Optional[int] = ...) -> None: ...

class WorkItem(_message.Message):
    __slots__ = ("orchestratorRequest", "activityRequest", "entityRequest", "healthPing", "completionToken")
    ORCHESTRATORREQUEST_FIELD_NUMBER: _ClassVar[int]
    ACTIVITYREQUEST_FIELD_NUMBER: _ClassVar[int]
    ENTITYREQUEST_FIELD_NUMBER: _ClassVar[int]
    HEALTHPING_FIELD_NUMBER: _ClassVar[int]
    COMPLETIONTOKEN_FIELD_NUMBER: _ClassVar[int]
    orchestratorRequest: OrchestratorRequest
    activityRequest: ActivityRequest
    entityRequest: EntityBatchRequest
    healthPing: HealthPing
    completionToken: str
    def __init__(self, orchestratorRequest: _Optional[_Union[OrchestratorRequest, _Mapping]] = ..., activityRequest: _Optional[_Union[ActivityRequest, _Mapping]] = ..., entityRequest: _Optional[_Union[EntityBatchRequest, _Mapping]] = ..., healthPing: _Optional[_Union[HealthPing, _Mapping]] = ..., completionToken: _Optional[str] = ...) -> None: ...

class CompleteTaskResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class HealthPing(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
