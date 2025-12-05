from typing import Any, Callable, Protocol


class ProtoTaskHubSidecarServiceStub(Protocol):
    """A stub class roughly matching the TaskHubSidecarServiceStub generated from the .proto file.
    Used by Azure Functions during orchestration and entity executions to inject custom behavior,
    as no real sidecar stub is available.
    """
    Hello: Callable[..., Any]
    StartInstance: Callable[..., Any]
    GetInstance: Callable[..., Any]
    RewindInstance: Callable[..., Any]
    WaitForInstanceStart: Callable[..., Any]
    WaitForInstanceCompletion: Callable[..., Any]
    RaiseEvent: Callable[..., Any]
    TerminateInstance: Callable[..., Any]
    SuspendInstance: Callable[..., Any]
    ResumeInstance: Callable[..., Any]
    QueryInstances: Callable[..., Any]
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
