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
ORCHESTRATION_STATUS_CANCELED: OrchestrationStatus
ORCHESTRATION_STATUS_COMPLETED: OrchestrationStatus
ORCHESTRATION_STATUS_CONTINUED_AS_NEW: OrchestrationStatus
ORCHESTRATION_STATUS_FAILED: OrchestrationStatus
ORCHESTRATION_STATUS_PENDING: OrchestrationStatus
ORCHESTRATION_STATUS_RUNNING: OrchestrationStatus
ORCHESTRATION_STATUS_SUSPENDED: OrchestrationStatus
ORCHESTRATION_STATUS_TERMINATED: OrchestrationStatus

class ActivityRequest(_message.Message):
    __slots__ = ["input", "name", "orchestrationInstance", "taskId", "version"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATIONINSTANCE_FIELD_NUMBER: _ClassVar[int]
    TASKID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    name: str
    orchestrationInstance: OrchestrationInstance
    taskId: int
    version: _wrappers_pb2.StringValue
    def __init__(self, name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., orchestrationInstance: _Optional[_Union[OrchestrationInstance, _Mapping]] = ..., taskId: _Optional[int] = ...) -> None: ...

class ActivityResponse(_message.Message):
    __slots__ = ["failureDetails", "instanceId", "result", "taskId"]
    FAILUREDETAILS_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    TASKID_FIELD_NUMBER: _ClassVar[int]
    failureDetails: TaskFailureDetails
    instanceId: str
    result: _wrappers_pb2.StringValue
    taskId: int
    def __init__(self, instanceId: _Optional[str] = ..., taskId: _Optional[int] = ..., result: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., failureDetails: _Optional[_Union[TaskFailureDetails, _Mapping]] = ...) -> None: ...

class CleanEntityStorageRequest(_message.Message):
    __slots__ = ["continuationToken", "releaseOrphanedLocks", "removeEmptyEntities"]
    CONTINUATIONTOKEN_FIELD_NUMBER: _ClassVar[int]
    RELEASEORPHANEDLOCKS_FIELD_NUMBER: _ClassVar[int]
    REMOVEEMPTYENTITIES_FIELD_NUMBER: _ClassVar[int]
    continuationToken: _wrappers_pb2.StringValue
    releaseOrphanedLocks: bool
    removeEmptyEntities: bool
    def __init__(self, continuationToken: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., removeEmptyEntities: bool = ..., releaseOrphanedLocks: bool = ...) -> None: ...

class CleanEntityStorageResponse(_message.Message):
    __slots__ = ["continuationToken", "emptyEntitiesRemoved", "orphanedLocksReleased"]
    CONTINUATIONTOKEN_FIELD_NUMBER: _ClassVar[int]
    EMPTYENTITIESREMOVED_FIELD_NUMBER: _ClassVar[int]
    ORPHANEDLOCKSRELEASED_FIELD_NUMBER: _ClassVar[int]
    continuationToken: _wrappers_pb2.StringValue
    emptyEntitiesRemoved: int
    orphanedLocksReleased: int
    def __init__(self, continuationToken: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., emptyEntitiesRemoved: _Optional[int] = ..., orphanedLocksReleased: _Optional[int] = ...) -> None: ...

class CompleteOrchestrationAction(_message.Message):
    __slots__ = ["carryoverEvents", "details", "failureDetails", "newVersion", "orchestrationStatus", "result"]
    CARRYOVEREVENTS_FIELD_NUMBER: _ClassVar[int]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    FAILUREDETAILS_FIELD_NUMBER: _ClassVar[int]
    NEWVERSION_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATIONSTATUS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    carryoverEvents: _containers.RepeatedCompositeFieldContainer[HistoryEvent]
    details: _wrappers_pb2.StringValue
    failureDetails: TaskFailureDetails
    newVersion: _wrappers_pb2.StringValue
    orchestrationStatus: OrchestrationStatus
    result: _wrappers_pb2.StringValue
    def __init__(self, orchestrationStatus: _Optional[_Union[OrchestrationStatus, str]] = ..., result: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., details: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., newVersion: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., carryoverEvents: _Optional[_Iterable[_Union[HistoryEvent, _Mapping]]] = ..., failureDetails: _Optional[_Union[TaskFailureDetails, _Mapping]] = ...) -> None: ...

class CompleteTaskResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ContinueAsNewEvent(_message.Message):
    __slots__ = ["input"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    def __init__(self, input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class CreateInstanceRequest(_message.Message):
    __slots__ = ["input", "instanceId", "name", "scheduledStartTimestamp", "version"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEDULEDSTARTTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    instanceId: str
    name: str
    scheduledStartTimestamp: _timestamp_pb2.Timestamp
    version: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., scheduledStartTimestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class CreateInstanceResponse(_message.Message):
    __slots__ = ["instanceId"]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    def __init__(self, instanceId: _Optional[str] = ...) -> None: ...

class CreateSubOrchestrationAction(_message.Message):
    __slots__ = ["input", "instanceId", "name", "version"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    instanceId: str
    name: str
    version: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class CreateTaskHubRequest(_message.Message):
    __slots__ = ["recreateIfExists"]
    RECREATEIFEXISTS_FIELD_NUMBER: _ClassVar[int]
    recreateIfExists: bool
    def __init__(self, recreateIfExists: bool = ...) -> None: ...

class CreateTaskHubResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class CreateTimerAction(_message.Message):
    __slots__ = ["fireAt"]
    FIREAT_FIELD_NUMBER: _ClassVar[int]
    fireAt: _timestamp_pb2.Timestamp
    def __init__(self, fireAt: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class DeleteTaskHubRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class DeleteTaskHubResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class EntityBatchRequest(_message.Message):
    __slots__ = ["entityState", "instanceId", "operations"]
    ENTITYSTATE_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    OPERATIONS_FIELD_NUMBER: _ClassVar[int]
    entityState: _wrappers_pb2.StringValue
    instanceId: str
    operations: _containers.RepeatedCompositeFieldContainer[OperationRequest]
    def __init__(self, instanceId: _Optional[str] = ..., entityState: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., operations: _Optional[_Iterable[_Union[OperationRequest, _Mapping]]] = ...) -> None: ...

class EntityBatchResult(_message.Message):
    __slots__ = ["actions", "entityState", "failureDetails", "results"]
    ACTIONS_FIELD_NUMBER: _ClassVar[int]
    ENTITYSTATE_FIELD_NUMBER: _ClassVar[int]
    FAILUREDETAILS_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    actions: _containers.RepeatedCompositeFieldContainer[OperationAction]
    entityState: _wrappers_pb2.StringValue
    failureDetails: TaskFailureDetails
    results: _containers.RepeatedCompositeFieldContainer[OperationResult]
    def __init__(self, results: _Optional[_Iterable[_Union[OperationResult, _Mapping]]] = ..., actions: _Optional[_Iterable[_Union[OperationAction, _Mapping]]] = ..., entityState: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., failureDetails: _Optional[_Union[TaskFailureDetails, _Mapping]] = ...) -> None: ...

class EntityMetadata(_message.Message):
    __slots__ = ["backlogQueueSize", "instanceId", "lastModifiedTime", "lockedBy", "serializedState"]
    BACKLOGQUEUESIZE_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    LASTMODIFIEDTIME_FIELD_NUMBER: _ClassVar[int]
    LOCKEDBY_FIELD_NUMBER: _ClassVar[int]
    SERIALIZEDSTATE_FIELD_NUMBER: _ClassVar[int]
    backlogQueueSize: int
    instanceId: str
    lastModifiedTime: _timestamp_pb2.Timestamp
    lockedBy: _wrappers_pb2.StringValue
    serializedState: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., lastModifiedTime: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., backlogQueueSize: _Optional[int] = ..., lockedBy: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., serializedState: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class EntityQuery(_message.Message):
    __slots__ = ["continuationToken", "includeState", "includeTransient", "instanceIdStartsWith", "lastModifiedFrom", "lastModifiedTo", "pageSize"]
    CONTINUATIONTOKEN_FIELD_NUMBER: _ClassVar[int]
    INCLUDESTATE_FIELD_NUMBER: _ClassVar[int]
    INCLUDETRANSIENT_FIELD_NUMBER: _ClassVar[int]
    INSTANCEIDSTARTSWITH_FIELD_NUMBER: _ClassVar[int]
    LASTMODIFIEDFROM_FIELD_NUMBER: _ClassVar[int]
    LASTMODIFIEDTO_FIELD_NUMBER: _ClassVar[int]
    PAGESIZE_FIELD_NUMBER: _ClassVar[int]
    continuationToken: _wrappers_pb2.StringValue
    includeState: bool
    includeTransient: bool
    instanceIdStartsWith: _wrappers_pb2.StringValue
    lastModifiedFrom: _timestamp_pb2.Timestamp
    lastModifiedTo: _timestamp_pb2.Timestamp
    pageSize: _wrappers_pb2.Int32Value
    def __init__(self, instanceIdStartsWith: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., lastModifiedFrom: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., lastModifiedTo: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., includeState: bool = ..., includeTransient: bool = ..., pageSize: _Optional[_Union[_wrappers_pb2.Int32Value, _Mapping]] = ..., continuationToken: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class EventRaisedEvent(_message.Message):
    __slots__ = ["input", "name"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    name: str
    def __init__(self, name: _Optional[str] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class EventSentEvent(_message.Message):
    __slots__ = ["input", "instanceId", "name"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    instanceId: str
    name: str
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class ExecutionCompletedEvent(_message.Message):
    __slots__ = ["failureDetails", "orchestrationStatus", "result"]
    FAILUREDETAILS_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATIONSTATUS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    failureDetails: TaskFailureDetails
    orchestrationStatus: OrchestrationStatus
    result: _wrappers_pb2.StringValue
    def __init__(self, orchestrationStatus: _Optional[_Union[OrchestrationStatus, str]] = ..., result: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., failureDetails: _Optional[_Union[TaskFailureDetails, _Mapping]] = ...) -> None: ...

class ExecutionResumedEvent(_message.Message):
    __slots__ = ["input"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    def __init__(self, input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class ExecutionStartedEvent(_message.Message):
    __slots__ = ["input", "name", "orchestrationInstance", "orchestrationSpanID", "parentInstance", "parentTraceContext", "scheduledStartTimestamp", "version"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATIONINSTANCE_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATIONSPANID_FIELD_NUMBER: _ClassVar[int]
    PARENTINSTANCE_FIELD_NUMBER: _ClassVar[int]
    PARENTTRACECONTEXT_FIELD_NUMBER: _ClassVar[int]
    SCHEDULEDSTARTTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    name: str
    orchestrationInstance: OrchestrationInstance
    orchestrationSpanID: _wrappers_pb2.StringValue
    parentInstance: ParentInstanceInfo
    parentTraceContext: TraceContext
    scheduledStartTimestamp: _timestamp_pb2.Timestamp
    version: _wrappers_pb2.StringValue
    def __init__(self, name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., orchestrationInstance: _Optional[_Union[OrchestrationInstance, _Mapping]] = ..., parentInstance: _Optional[_Union[ParentInstanceInfo, _Mapping]] = ..., scheduledStartTimestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., parentTraceContext: _Optional[_Union[TraceContext, _Mapping]] = ..., orchestrationSpanID: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class ExecutionSuspendedEvent(_message.Message):
    __slots__ = ["input"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    def __init__(self, input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class ExecutionTerminatedEvent(_message.Message):
    __slots__ = ["input", "recurse"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    RECURSE_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    recurse: bool
    def __init__(self, input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., recurse: bool = ...) -> None: ...

class GenericEvent(_message.Message):
    __slots__ = ["data"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: str
    def __init__(self, data: _Optional[str] = ...) -> None: ...

class GetEntityRequest(_message.Message):
    __slots__ = ["includeState", "instanceId"]
    INCLUDESTATE_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    includeState: bool
    instanceId: str
    def __init__(self, instanceId: _Optional[str] = ..., includeState: bool = ...) -> None: ...

class GetEntityResponse(_message.Message):
    __slots__ = ["entity", "exists"]
    ENTITY_FIELD_NUMBER: _ClassVar[int]
    EXISTS_FIELD_NUMBER: _ClassVar[int]
    entity: EntityMetadata
    exists: bool
    def __init__(self, exists: bool = ..., entity: _Optional[_Union[EntityMetadata, _Mapping]] = ...) -> None: ...

class GetInstanceRequest(_message.Message):
    __slots__ = ["getInputsAndOutputs", "instanceId"]
    GETINPUTSANDOUTPUTS_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    getInputsAndOutputs: bool
    instanceId: str
    def __init__(self, instanceId: _Optional[str] = ..., getInputsAndOutputs: bool = ...) -> None: ...

class GetInstanceResponse(_message.Message):
    __slots__ = ["exists", "orchestrationState"]
    EXISTS_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATIONSTATE_FIELD_NUMBER: _ClassVar[int]
    exists: bool
    orchestrationState: OrchestrationState
    def __init__(self, exists: bool = ..., orchestrationState: _Optional[_Union[OrchestrationState, _Mapping]] = ...) -> None: ...

class GetWorkItemsRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class HistoryEvent(_message.Message):
    __slots__ = ["continueAsNew", "eventId", "eventRaised", "eventSent", "executionCompleted", "executionResumed", "executionStarted", "executionSuspended", "executionTerminated", "genericEvent", "historyState", "orchestratorCompleted", "orchestratorStarted", "subOrchestrationInstanceCompleted", "subOrchestrationInstanceCreated", "subOrchestrationInstanceFailed", "taskCompleted", "taskFailed", "taskScheduled", "timerCreated", "timerFired", "timestamp"]
    CONTINUEASNEW_FIELD_NUMBER: _ClassVar[int]
    EVENTID_FIELD_NUMBER: _ClassVar[int]
    EVENTRAISED_FIELD_NUMBER: _ClassVar[int]
    EVENTSENT_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONCOMPLETED_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONRESUMED_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONSTARTED_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONSUSPENDED_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONTERMINATED_FIELD_NUMBER: _ClassVar[int]
    GENERICEVENT_FIELD_NUMBER: _ClassVar[int]
    HISTORYSTATE_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATORCOMPLETED_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATORSTARTED_FIELD_NUMBER: _ClassVar[int]
    SUBORCHESTRATIONINSTANCECOMPLETED_FIELD_NUMBER: _ClassVar[int]
    SUBORCHESTRATIONINSTANCECREATED_FIELD_NUMBER: _ClassVar[int]
    SUBORCHESTRATIONINSTANCEFAILED_FIELD_NUMBER: _ClassVar[int]
    TASKCOMPLETED_FIELD_NUMBER: _ClassVar[int]
    TASKFAILED_FIELD_NUMBER: _ClassVar[int]
    TASKSCHEDULED_FIELD_NUMBER: _ClassVar[int]
    TIMERCREATED_FIELD_NUMBER: _ClassVar[int]
    TIMERFIRED_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    continueAsNew: ContinueAsNewEvent
    eventId: int
    eventRaised: EventRaisedEvent
    eventSent: EventSentEvent
    executionCompleted: ExecutionCompletedEvent
    executionResumed: ExecutionResumedEvent
    executionStarted: ExecutionStartedEvent
    executionSuspended: ExecutionSuspendedEvent
    executionTerminated: ExecutionTerminatedEvent
    genericEvent: GenericEvent
    historyState: HistoryStateEvent
    orchestratorCompleted: OrchestratorCompletedEvent
    orchestratorStarted: OrchestratorStartedEvent
    subOrchestrationInstanceCompleted: SubOrchestrationInstanceCompletedEvent
    subOrchestrationInstanceCreated: SubOrchestrationInstanceCreatedEvent
    subOrchestrationInstanceFailed: SubOrchestrationInstanceFailedEvent
    taskCompleted: TaskCompletedEvent
    taskFailed: TaskFailedEvent
    taskScheduled: TaskScheduledEvent
    timerCreated: TimerCreatedEvent
    timerFired: TimerFiredEvent
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, eventId: _Optional[int] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., executionStarted: _Optional[_Union[ExecutionStartedEvent, _Mapping]] = ..., executionCompleted: _Optional[_Union[ExecutionCompletedEvent, _Mapping]] = ..., executionTerminated: _Optional[_Union[ExecutionTerminatedEvent, _Mapping]] = ..., taskScheduled: _Optional[_Union[TaskScheduledEvent, _Mapping]] = ..., taskCompleted: _Optional[_Union[TaskCompletedEvent, _Mapping]] = ..., taskFailed: _Optional[_Union[TaskFailedEvent, _Mapping]] = ..., subOrchestrationInstanceCreated: _Optional[_Union[SubOrchestrationInstanceCreatedEvent, _Mapping]] = ..., subOrchestrationInstanceCompleted: _Optional[_Union[SubOrchestrationInstanceCompletedEvent, _Mapping]] = ..., subOrchestrationInstanceFailed: _Optional[_Union[SubOrchestrationInstanceFailedEvent, _Mapping]] = ..., timerCreated: _Optional[_Union[TimerCreatedEvent, _Mapping]] = ..., timerFired: _Optional[_Union[TimerFiredEvent, _Mapping]] = ..., orchestratorStarted: _Optional[_Union[OrchestratorStartedEvent, _Mapping]] = ..., orchestratorCompleted: _Optional[_Union[OrchestratorCompletedEvent, _Mapping]] = ..., eventSent: _Optional[_Union[EventSentEvent, _Mapping]] = ..., eventRaised: _Optional[_Union[EventRaisedEvent, _Mapping]] = ..., genericEvent: _Optional[_Union[GenericEvent, _Mapping]] = ..., historyState: _Optional[_Union[HistoryStateEvent, _Mapping]] = ..., continueAsNew: _Optional[_Union[ContinueAsNewEvent, _Mapping]] = ..., executionSuspended: _Optional[_Union[ExecutionSuspendedEvent, _Mapping]] = ..., executionResumed: _Optional[_Union[ExecutionResumedEvent, _Mapping]] = ...) -> None: ...

class HistoryStateEvent(_message.Message):
    __slots__ = ["orchestrationState"]
    ORCHESTRATIONSTATE_FIELD_NUMBER: _ClassVar[int]
    orchestrationState: OrchestrationState
    def __init__(self, orchestrationState: _Optional[_Union[OrchestrationState, _Mapping]] = ...) -> None: ...

class InstanceQuery(_message.Message):
    __slots__ = ["continuationToken", "createdTimeFrom", "createdTimeTo", "fetchInputsAndOutputs", "instanceIdPrefix", "maxInstanceCount", "runtimeStatus", "taskHubNames"]
    CONTINUATIONTOKEN_FIELD_NUMBER: _ClassVar[int]
    CREATEDTIMEFROM_FIELD_NUMBER: _ClassVar[int]
    CREATEDTIMETO_FIELD_NUMBER: _ClassVar[int]
    FETCHINPUTSANDOUTPUTS_FIELD_NUMBER: _ClassVar[int]
    INSTANCEIDPREFIX_FIELD_NUMBER: _ClassVar[int]
    MAXINSTANCECOUNT_FIELD_NUMBER: _ClassVar[int]
    RUNTIMESTATUS_FIELD_NUMBER: _ClassVar[int]
    TASKHUBNAMES_FIELD_NUMBER: _ClassVar[int]
    continuationToken: _wrappers_pb2.StringValue
    createdTimeFrom: _timestamp_pb2.Timestamp
    createdTimeTo: _timestamp_pb2.Timestamp
    fetchInputsAndOutputs: bool
    instanceIdPrefix: _wrappers_pb2.StringValue
    maxInstanceCount: int
    runtimeStatus: _containers.RepeatedScalarFieldContainer[OrchestrationStatus]
    taskHubNames: _containers.RepeatedCompositeFieldContainer[_wrappers_pb2.StringValue]
    def __init__(self, runtimeStatus: _Optional[_Iterable[_Union[OrchestrationStatus, str]]] = ..., createdTimeFrom: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., createdTimeTo: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., taskHubNames: _Optional[_Iterable[_Union[_wrappers_pb2.StringValue, _Mapping]]] = ..., maxInstanceCount: _Optional[int] = ..., continuationToken: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., instanceIdPrefix: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., fetchInputsAndOutputs: bool = ...) -> None: ...

class OperationAction(_message.Message):
    __slots__ = ["id", "sendSignal", "startNewOrchestration"]
    ID_FIELD_NUMBER: _ClassVar[int]
    SENDSIGNAL_FIELD_NUMBER: _ClassVar[int]
    STARTNEWORCHESTRATION_FIELD_NUMBER: _ClassVar[int]
    id: int
    sendSignal: SendSignalAction
    startNewOrchestration: StartNewOrchestrationAction
    def __init__(self, id: _Optional[int] = ..., sendSignal: _Optional[_Union[SendSignalAction, _Mapping]] = ..., startNewOrchestration: _Optional[_Union[StartNewOrchestrationAction, _Mapping]] = ...) -> None: ...

class OperationRequest(_message.Message):
    __slots__ = ["input", "operation", "requestId"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    operation: str
    requestId: str
    def __init__(self, operation: _Optional[str] = ..., requestId: _Optional[str] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class OperationResult(_message.Message):
    __slots__ = ["failure", "success"]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    failure: OperationResultFailure
    success: OperationResultSuccess
    def __init__(self, success: _Optional[_Union[OperationResultSuccess, _Mapping]] = ..., failure: _Optional[_Union[OperationResultFailure, _Mapping]] = ...) -> None: ...

class OperationResultFailure(_message.Message):
    __slots__ = ["failureDetails"]
    FAILUREDETAILS_FIELD_NUMBER: _ClassVar[int]
    failureDetails: TaskFailureDetails
    def __init__(self, failureDetails: _Optional[_Union[TaskFailureDetails, _Mapping]] = ...) -> None: ...

class OperationResultSuccess(_message.Message):
    __slots__ = ["result"]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: _wrappers_pb2.StringValue
    def __init__(self, result: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class OrchestrationInstance(_message.Message):
    __slots__ = ["executionId", "instanceId"]
    EXECUTIONID_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    executionId: _wrappers_pb2.StringValue
    instanceId: str
    def __init__(self, instanceId: _Optional[str] = ..., executionId: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class OrchestrationState(_message.Message):
    __slots__ = ["createdTimestamp", "customStatus", "failureDetails", "input", "instanceId", "lastUpdatedTimestamp", "name", "orchestrationStatus", "output", "scheduledStartTimestamp", "version"]
    CREATEDTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    CUSTOMSTATUS_FIELD_NUMBER: _ClassVar[int]
    FAILUREDETAILS_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    LASTUPDATEDTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATIONSTATUS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    SCHEDULEDSTARTTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    createdTimestamp: _timestamp_pb2.Timestamp
    customStatus: _wrappers_pb2.StringValue
    failureDetails: TaskFailureDetails
    input: _wrappers_pb2.StringValue
    instanceId: str
    lastUpdatedTimestamp: _timestamp_pb2.Timestamp
    name: str
    orchestrationStatus: OrchestrationStatus
    output: _wrappers_pb2.StringValue
    scheduledStartTimestamp: _timestamp_pb2.Timestamp
    version: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., orchestrationStatus: _Optional[_Union[OrchestrationStatus, str]] = ..., scheduledStartTimestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., createdTimestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., lastUpdatedTimestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., output: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., customStatus: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., failureDetails: _Optional[_Union[TaskFailureDetails, _Mapping]] = ...) -> None: ...

class OrchestratorAction(_message.Message):
    __slots__ = ["completeOrchestration", "createSubOrchestration", "createTimer", "id", "scheduleTask", "sendEvent", "terminateOrchestration"]
    COMPLETEORCHESTRATION_FIELD_NUMBER: _ClassVar[int]
    CREATESUBORCHESTRATION_FIELD_NUMBER: _ClassVar[int]
    CREATETIMER_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    SCHEDULETASK_FIELD_NUMBER: _ClassVar[int]
    SENDEVENT_FIELD_NUMBER: _ClassVar[int]
    TERMINATEORCHESTRATION_FIELD_NUMBER: _ClassVar[int]
    completeOrchestration: CompleteOrchestrationAction
    createSubOrchestration: CreateSubOrchestrationAction
    createTimer: CreateTimerAction
    id: int
    scheduleTask: ScheduleTaskAction
    sendEvent: SendEventAction
    terminateOrchestration: TerminateOrchestrationAction
    def __init__(self, id: _Optional[int] = ..., scheduleTask: _Optional[_Union[ScheduleTaskAction, _Mapping]] = ..., createSubOrchestration: _Optional[_Union[CreateSubOrchestrationAction, _Mapping]] = ..., createTimer: _Optional[_Union[CreateTimerAction, _Mapping]] = ..., sendEvent: _Optional[_Union[SendEventAction, _Mapping]] = ..., completeOrchestration: _Optional[_Union[CompleteOrchestrationAction, _Mapping]] = ..., terminateOrchestration: _Optional[_Union[TerminateOrchestrationAction, _Mapping]] = ...) -> None: ...

class OrchestratorCompletedEvent(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class OrchestratorEntityParameters(_message.Message):
    __slots__ = ["entityMessageReorderWindow"]
    ENTITYMESSAGEREORDERWINDOW_FIELD_NUMBER: _ClassVar[int]
    entityMessageReorderWindow: _duration_pb2.Duration
    def __init__(self, entityMessageReorderWindow: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class OrchestratorRequest(_message.Message):
    __slots__ = ["entityParameters", "executionId", "instanceId", "newEvents", "pastEvents"]
    ENTITYPARAMETERS_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONID_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NEWEVENTS_FIELD_NUMBER: _ClassVar[int]
    PASTEVENTS_FIELD_NUMBER: _ClassVar[int]
    entityParameters: OrchestratorEntityParameters
    executionId: _wrappers_pb2.StringValue
    instanceId: str
    newEvents: _containers.RepeatedCompositeFieldContainer[HistoryEvent]
    pastEvents: _containers.RepeatedCompositeFieldContainer[HistoryEvent]
    def __init__(self, instanceId: _Optional[str] = ..., executionId: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., pastEvents: _Optional[_Iterable[_Union[HistoryEvent, _Mapping]]] = ..., newEvents: _Optional[_Iterable[_Union[HistoryEvent, _Mapping]]] = ..., entityParameters: _Optional[_Union[OrchestratorEntityParameters, _Mapping]] = ...) -> None: ...

class OrchestratorResponse(_message.Message):
    __slots__ = ["actions", "customStatus", "instanceId"]
    ACTIONS_FIELD_NUMBER: _ClassVar[int]
    CUSTOMSTATUS_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    actions: _containers.RepeatedCompositeFieldContainer[OrchestratorAction]
    customStatus: _wrappers_pb2.StringValue
    instanceId: str
    def __init__(self, instanceId: _Optional[str] = ..., actions: _Optional[_Iterable[_Union[OrchestratorAction, _Mapping]]] = ..., customStatus: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class OrchestratorStartedEvent(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ParentInstanceInfo(_message.Message):
    __slots__ = ["name", "orchestrationInstance", "taskScheduledId", "version"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATIONINSTANCE_FIELD_NUMBER: _ClassVar[int]
    TASKSCHEDULEDID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    name: _wrappers_pb2.StringValue
    orchestrationInstance: OrchestrationInstance
    taskScheduledId: int
    version: _wrappers_pb2.StringValue
    def __init__(self, taskScheduledId: _Optional[int] = ..., name: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., orchestrationInstance: _Optional[_Union[OrchestrationInstance, _Mapping]] = ...) -> None: ...

class PurgeInstanceFilter(_message.Message):
    __slots__ = ["createdTimeFrom", "createdTimeTo", "runtimeStatus"]
    CREATEDTIMEFROM_FIELD_NUMBER: _ClassVar[int]
    CREATEDTIMETO_FIELD_NUMBER: _ClassVar[int]
    RUNTIMESTATUS_FIELD_NUMBER: _ClassVar[int]
    createdTimeFrom: _timestamp_pb2.Timestamp
    createdTimeTo: _timestamp_pb2.Timestamp
    runtimeStatus: _containers.RepeatedScalarFieldContainer[OrchestrationStatus]
    def __init__(self, createdTimeFrom: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., createdTimeTo: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., runtimeStatus: _Optional[_Iterable[_Union[OrchestrationStatus, str]]] = ...) -> None: ...

class PurgeInstancesRequest(_message.Message):
    __slots__ = ["instanceId", "purgeInstanceFilter"]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    PURGEINSTANCEFILTER_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    purgeInstanceFilter: PurgeInstanceFilter
    def __init__(self, instanceId: _Optional[str] = ..., purgeInstanceFilter: _Optional[_Union[PurgeInstanceFilter, _Mapping]] = ...) -> None: ...

class PurgeInstancesResponse(_message.Message):
    __slots__ = ["deletedInstanceCount"]
    DELETEDINSTANCECOUNT_FIELD_NUMBER: _ClassVar[int]
    deletedInstanceCount: int
    def __init__(self, deletedInstanceCount: _Optional[int] = ...) -> None: ...

class QueryEntitiesRequest(_message.Message):
    __slots__ = ["query"]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    query: EntityQuery
    def __init__(self, query: _Optional[_Union[EntityQuery, _Mapping]] = ...) -> None: ...

class QueryEntitiesResponse(_message.Message):
    __slots__ = ["continuationToken", "entities"]
    CONTINUATIONTOKEN_FIELD_NUMBER: _ClassVar[int]
    ENTITIES_FIELD_NUMBER: _ClassVar[int]
    continuationToken: _wrappers_pb2.StringValue
    entities: _containers.RepeatedCompositeFieldContainer[EntityMetadata]
    def __init__(self, entities: _Optional[_Iterable[_Union[EntityMetadata, _Mapping]]] = ..., continuationToken: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class QueryInstancesRequest(_message.Message):
    __slots__ = ["query"]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    query: InstanceQuery
    def __init__(self, query: _Optional[_Union[InstanceQuery, _Mapping]] = ...) -> None: ...

class QueryInstancesResponse(_message.Message):
    __slots__ = ["continuationToken", "orchestrationState"]
    CONTINUATIONTOKEN_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATIONSTATE_FIELD_NUMBER: _ClassVar[int]
    continuationToken: _wrappers_pb2.StringValue
    orchestrationState: _containers.RepeatedCompositeFieldContainer[OrchestrationState]
    def __init__(self, orchestrationState: _Optional[_Iterable[_Union[OrchestrationState, _Mapping]]] = ..., continuationToken: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class RaiseEventRequest(_message.Message):
    __slots__ = ["input", "instanceId", "name"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    instanceId: str
    name: str
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class RaiseEventResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ResumeRequest(_message.Message):
    __slots__ = ["instanceId", "reason"]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    reason: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., reason: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class ResumeResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class RewindInstanceRequest(_message.Message):
    __slots__ = ["instanceId", "reason"]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    reason: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., reason: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class RewindInstanceResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ScheduleTaskAction(_message.Message):
    __slots__ = ["input", "name", "version"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    name: str
    version: _wrappers_pb2.StringValue
    def __init__(self, name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class SendEventAction(_message.Message):
    __slots__ = ["data", "instance", "name"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    data: _wrappers_pb2.StringValue
    instance: OrchestrationInstance
    name: str
    def __init__(self, instance: _Optional[_Union[OrchestrationInstance, _Mapping]] = ..., name: _Optional[str] = ..., data: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class SendSignalAction(_message.Message):
    __slots__ = ["input", "instanceId", "name", "scheduledTime"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEDULEDTIME_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    instanceId: str
    name: str
    scheduledTime: _timestamp_pb2.Timestamp
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., scheduledTime: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class SignalEntityRequest(_message.Message):
    __slots__ = ["input", "instanceId", "name", "requestId", "scheduledTime"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    REQUESTID_FIELD_NUMBER: _ClassVar[int]
    SCHEDULEDTIME_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    instanceId: str
    name: str
    requestId: str
    scheduledTime: _timestamp_pb2.Timestamp
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., requestId: _Optional[str] = ..., scheduledTime: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class SignalEntityResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class StartNewOrchestrationAction(_message.Message):
    __slots__ = ["input", "instanceId", "name", "scheduledTime", "version"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEDULEDTIME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    instanceId: str
    name: str
    scheduledTime: _timestamp_pb2.Timestamp
    version: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., scheduledTime: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class SubOrchestrationInstanceCompletedEvent(_message.Message):
    __slots__ = ["result", "taskScheduledId"]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    TASKSCHEDULEDID_FIELD_NUMBER: _ClassVar[int]
    result: _wrappers_pb2.StringValue
    taskScheduledId: int
    def __init__(self, taskScheduledId: _Optional[int] = ..., result: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class SubOrchestrationInstanceCreatedEvent(_message.Message):
    __slots__ = ["input", "instanceId", "name", "parentTraceContext", "version"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PARENTTRACECONTEXT_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    instanceId: str
    name: str
    parentTraceContext: TraceContext
    version: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., parentTraceContext: _Optional[_Union[TraceContext, _Mapping]] = ...) -> None: ...

class SubOrchestrationInstanceFailedEvent(_message.Message):
    __slots__ = ["failureDetails", "taskScheduledId"]
    FAILUREDETAILS_FIELD_NUMBER: _ClassVar[int]
    TASKSCHEDULEDID_FIELD_NUMBER: _ClassVar[int]
    failureDetails: TaskFailureDetails
    taskScheduledId: int
    def __init__(self, taskScheduledId: _Optional[int] = ..., failureDetails: _Optional[_Union[TaskFailureDetails, _Mapping]] = ...) -> None: ...

class SuspendRequest(_message.Message):
    __slots__ = ["instanceId", "reason"]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    reason: _wrappers_pb2.StringValue
    def __init__(self, instanceId: _Optional[str] = ..., reason: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class SuspendResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class TaskCompletedEvent(_message.Message):
    __slots__ = ["result", "taskScheduledId"]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    TASKSCHEDULEDID_FIELD_NUMBER: _ClassVar[int]
    result: _wrappers_pb2.StringValue
    taskScheduledId: int
    def __init__(self, taskScheduledId: _Optional[int] = ..., result: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class TaskFailedEvent(_message.Message):
    __slots__ = ["failureDetails", "taskScheduledId"]
    FAILUREDETAILS_FIELD_NUMBER: _ClassVar[int]
    TASKSCHEDULEDID_FIELD_NUMBER: _ClassVar[int]
    failureDetails: TaskFailureDetails
    taskScheduledId: int
    def __init__(self, taskScheduledId: _Optional[int] = ..., failureDetails: _Optional[_Union[TaskFailureDetails, _Mapping]] = ...) -> None: ...

class TaskFailureDetails(_message.Message):
    __slots__ = ["errorMessage", "errorType", "innerFailure", "isNonRetriable", "stackTrace"]
    ERRORMESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERRORTYPE_FIELD_NUMBER: _ClassVar[int]
    INNERFAILURE_FIELD_NUMBER: _ClassVar[int]
    ISNONRETRIABLE_FIELD_NUMBER: _ClassVar[int]
    STACKTRACE_FIELD_NUMBER: _ClassVar[int]
    errorMessage: str
    errorType: str
    innerFailure: TaskFailureDetails
    isNonRetriable: bool
    stackTrace: _wrappers_pb2.StringValue
    def __init__(self, errorType: _Optional[str] = ..., errorMessage: _Optional[str] = ..., stackTrace: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., innerFailure: _Optional[_Union[TaskFailureDetails, _Mapping]] = ..., isNonRetriable: bool = ...) -> None: ...

class TaskScheduledEvent(_message.Message):
    __slots__ = ["input", "name", "parentTraceContext", "version"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PARENTTRACECONTEXT_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    input: _wrappers_pb2.StringValue
    name: str
    parentTraceContext: TraceContext
    version: _wrappers_pb2.StringValue
    def __init__(self, name: _Optional[str] = ..., version: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., input: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., parentTraceContext: _Optional[_Union[TraceContext, _Mapping]] = ...) -> None: ...

class TerminateOrchestrationAction(_message.Message):
    __slots__ = ["instanceId", "reason", "recurse"]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    RECURSE_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    reason: _wrappers_pb2.StringValue
    recurse: bool
    def __init__(self, instanceId: _Optional[str] = ..., reason: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., recurse: bool = ...) -> None: ...

class TerminateRequest(_message.Message):
    __slots__ = ["instanceId", "output", "recursive"]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    RECURSIVE_FIELD_NUMBER: _ClassVar[int]
    instanceId: str
    output: _wrappers_pb2.StringValue
    recursive: bool
    def __init__(self, instanceId: _Optional[str] = ..., output: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., recursive: bool = ...) -> None: ...

class TerminateResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class TimerCreatedEvent(_message.Message):
    __slots__ = ["fireAt"]
    FIREAT_FIELD_NUMBER: _ClassVar[int]
    fireAt: _timestamp_pb2.Timestamp
    def __init__(self, fireAt: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class TimerFiredEvent(_message.Message):
    __slots__ = ["fireAt", "timerId"]
    FIREAT_FIELD_NUMBER: _ClassVar[int]
    TIMERID_FIELD_NUMBER: _ClassVar[int]
    fireAt: _timestamp_pb2.Timestamp
    timerId: int
    def __init__(self, fireAt: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., timerId: _Optional[int] = ...) -> None: ...

class TraceContext(_message.Message):
    __slots__ = ["spanID", "traceParent", "traceState"]
    SPANID_FIELD_NUMBER: _ClassVar[int]
    TRACEPARENT_FIELD_NUMBER: _ClassVar[int]
    TRACESTATE_FIELD_NUMBER: _ClassVar[int]
    spanID: str
    traceParent: str
    traceState: _wrappers_pb2.StringValue
    def __init__(self, traceParent: _Optional[str] = ..., spanID: _Optional[str] = ..., traceState: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...

class WorkItem(_message.Message):
    __slots__ = ["activityRequest", "entityRequest", "orchestratorRequest"]
    ACTIVITYREQUEST_FIELD_NUMBER: _ClassVar[int]
    ENTITYREQUEST_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATORREQUEST_FIELD_NUMBER: _ClassVar[int]
    activityRequest: ActivityRequest
    entityRequest: EntityBatchRequest
    orchestratorRequest: OrchestratorRequest
    def __init__(self, orchestratorRequest: _Optional[_Union[OrchestratorRequest, _Mapping]] = ..., activityRequest: _Optional[_Union[ActivityRequest, _Mapping]] = ..., entityRequest: _Optional[_Union[EntityBatchRequest, _Mapping]] = ...) -> None: ...

class OrchestrationStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
