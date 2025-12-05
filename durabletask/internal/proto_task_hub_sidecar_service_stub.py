from typing import Any, Callable


class ProtoTaskHubSidecarServiceStub(object):
    """A stub class roughly matching the TaskHubSidecarServiceStub generated from the .proto file.
    Used by Azure Functions during orchestration and entity executions to inject custom behavior,
    as no real sidecar stub is available.
    """

    def __init__(self):
        """Constructor.
        """
        self.Hello: Callable[..., Any]
        self.StartInstance: Callable[..., Any]
        self.GetInstance: Callable[..., Any]
        self.RewindInstance: Callable[..., Any]
        self.WaitForInstanceStart: Callable[..., Any]
        self.WaitForInstanceCompletion: Callable[..., Any]
        self.RaiseEvent: Callable[..., Any]
        self.TerminateInstance: Callable[..., Any]
        self.SuspendInstance: Callable[..., Any]
        self.ResumeInstance: Callable[..., Any]
        self.QueryInstances: Callable[..., Any]
        self.PurgeInstances: Callable[..., Any]
        self.GetWorkItems: Callable[..., Any]
        self.CompleteActivityTask: Callable[..., Any]
        self.CompleteOrchestratorTask: Callable[..., Any]
        self.CompleteEntityTask: Callable[..., Any]
        self.StreamInstanceHistory: Callable[..., Any]
        self.CreateTaskHub: Callable[..., Any]
        self.DeleteTaskHub: Callable[..., Any]
        self.SignalEntity: Callable[..., Any]
        self.GetEntity: Callable[..., Any]
        self.QueryEntities: Callable[..., Any]
        self.CleanEntityStorage: Callable[..., Any]
        self.AbandonTaskActivityWorkItem: Callable[..., Any]
        self.AbandonTaskOrchestratorWorkItem: Callable[..., Any]
        self.AbandonTaskEntityWorkItem: Callable[..., Any]
