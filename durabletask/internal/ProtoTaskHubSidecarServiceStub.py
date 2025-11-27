from typing import Callable


class ProtoTaskHubSidecarServiceStub(object):
    """A stub class roughly matching the TaskHubSidecarServiceStub generated from the .proto file.
    Used by Azure Functions during orchestration and entity executions to inject custom behavior,
    as no real sidecar stub is available.
    """

    def __init__(self):
        """Constructor.
        """
        self.Hello: Callable[..., None]
        self.StartInstance: Callable[..., None]
        self.GetInstance: Callable[..., None]
        self.RewindInstance: Callable[..., None]
        self.WaitForInstanceStart: Callable[..., None]
        self.WaitForInstanceCompletion: Callable[..., None]
        self.RaiseEvent: Callable[..., None]
        self.TerminateInstance: Callable[..., None]
        self.SuspendInstance: Callable[..., None]
        self.ResumeInstance: Callable[..., None]
        self.QueryInstances: Callable[..., None]
        self.PurgeInstances: Callable[..., None]
        self.GetWorkItems: Callable[..., None]
        self.CompleteActivityTask: Callable[..., None]
        self.CompleteOrchestratorTask: Callable[..., None]
        self.CompleteEntityTask: Callable[..., None]
        self.StreamInstanceHistory: Callable[..., None]
        self.CreateTaskHub: Callable[..., None]
        self.DeleteTaskHub: Callable[..., None]
        self.SignalEntity: Callable[..., None]
        self.GetEntity: Callable[..., None]
        self.QueryEntities: Callable[..., None]
        self.CleanEntityStorage: Callable[..., None]
        self.AbandonTaskActivityWorkItem: Callable[..., None]
        self.AbandonTaskOrchestratorWorkItem: Callable[..., None]
        self.AbandonTaskEntityWorkItem: Callable[..., None]
