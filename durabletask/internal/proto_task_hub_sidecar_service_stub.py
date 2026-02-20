from typing import Any, Callable, Protocol


class ProtoTaskHubSidecarServiceStub(Protocol):
    """A stub class matching the TaskHubSidecarServiceStub generated from the .proto file.
    Allows the use of TaskHubGrpcWorker methods when a real sidecar stub is not available.
    """
    Hello: Callable[..., Any]
    StartInstance: Callable[..., Any]
    GetInstance: Callable[..., Any]
    RewindInstance: Callable[..., Any]
    RestartInstance: Callable[..., Any]
    WaitForInstanceStart: Callable[..., Any]
    WaitForInstanceCompletion: Callable[..., Any]
    RaiseEvent: Callable[..., Any]
    TerminateInstance: Callable[..., Any]
    SuspendInstance: Callable[..., Any]
    ResumeInstance: Callable[..., Any]
    QueryInstances: Callable[..., Any]
    ListInstanceIds: Callable[..., Any]
    PurgeInstances: Callable[..., Any]
    GetWorkItems: Callable[..., Any]
    CompleteActivityTask: Callable[..., Any]
    CompleteOrchestratorTask: Callable[..., Any]
    CompleteEntityTask: Callable[..., Any]
    StreamInstanceHistory: Callable[..., Any]
    CreateTaskHub: Callable[..., Any]
    DeleteTaskHub: Callable[..., Any]
    SignalEntity: Callable[..., Any]
    GetEntity: Callable[..., Any]
    QueryEntities: Callable[..., Any]
    CleanEntityStorage: Callable[..., Any]
    AbandonTaskActivityWorkItem: Callable[..., Any]
    AbandonTaskOrchestratorWorkItem: Callable[..., Any]
    AbandonTaskEntityWorkItem: Callable[..., Any]
    SkipGracefulOrchestrationTerminations: Callable[..., Any]