
from durabletask.internal.ProtoTaskHubSidecarServiceStub import ProtoTaskHubSidecarServiceStub


class AzureFunctionsNullStub(ProtoTaskHubSidecarServiceStub):
    """Missing associated documentation comment in .proto file."""

    def __init__(self):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Hello = lambda *args, **kwargs: None
        self.StartInstance = lambda *args, **kwargs: None
        self.GetInstance = lambda *args, **kwargs: None
        self.RewindInstance = lambda *args, **kwargs: None
        self.WaitForInstanceStart = lambda *args, **kwargs: None
        self.WaitForInstanceCompletion = lambda *args, **kwargs: None
        self.RaiseEvent = lambda *args, **kwargs: None
        self.TerminateInstance = lambda *args, **kwargs: None
        self.SuspendInstance = lambda *args, **kwargs: None
        self.ResumeInstance = lambda *args, **kwargs: None
        self.QueryInstances = lambda *args, **kwargs: None
        self.PurgeInstances = lambda *args, **kwargs: None
        self.GetWorkItems = lambda *args, **kwargs: None
        self.CompleteActivityTask = lambda *args, **kwargs: None
        self.CompleteOrchestratorTask = lambda *args, **kwargs: None
        self.CompleteEntityTask = lambda *args, **kwargs: None
        self.StreamInstanceHistory = lambda *args, **kwargs: None
        self.CreateTaskHub = lambda *args, **kwargs: None
        self.DeleteTaskHub = lambda *args, **kwargs: None
        self.SignalEntity = lambda *args, **kwargs: None
        self.GetEntity = lambda *args, **kwargs: None
        self.QueryEntities = lambda *args, **kwargs: None
        self.CleanEntityStorage = lambda *args, **kwargs: None
        self.AbandonTaskActivityWorkItem = lambda *args, **kwargs: None
        self.AbandonTaskOrchestratorWorkItem = lambda *args, **kwargs: None
        self.AbandonTaskEntityWorkItem = lambda *args, **kwargs: None
