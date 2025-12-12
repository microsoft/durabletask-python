# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from durabletask.internal.proto_task_hub_sidecar_service_stub import ProtoTaskHubSidecarServiceStub


class AzureFunctionsNullStub(ProtoTaskHubSidecarServiceStub):
    """A task hub sidecar stub class that implements all methods as no-ops."""
    Hello = lambda *args, **kwargs: None
    StartInstance = lambda *args, **kwargs: None
    GetInstance = lambda *args, **kwargs: None
    RewindInstance = lambda *args, **kwargs: None
    WaitForInstanceStart = lambda *args, **kwargs: None
    WaitForInstanceCompletion = lambda *args, **kwargs: None
    RaiseEvent = lambda *args, **kwargs: None
    TerminateInstance = lambda *args, **kwargs: None
    SuspendInstance = lambda *args, **kwargs: None
    ResumeInstance = lambda *args, **kwargs: None
    QueryInstances = lambda *args, **kwargs: None
    PurgeInstances = lambda *args, **kwargs: None
    GetWorkItems = lambda *args, **kwargs: None
    CompleteActivityTask = lambda *args, **kwargs: None
    CompleteOrchestratorTask = lambda *args, **kwargs: None
    CompleteEntityTask = lambda *args, **kwargs: None
    StreamInstanceHistory = lambda *args, **kwargs: None
    CreateTaskHub = lambda *args, **kwargs: None
    DeleteTaskHub = lambda *args, **kwargs: None
    SignalEntity = lambda *args, **kwargs: None
    GetEntity = lambda *args, **kwargs: None
    QueryEntities = lambda *args, **kwargs: None
    CleanEntityStorage = lambda *args, **kwargs: None
    AbandonTaskActivityWorkItem = lambda *args, **kwargs: None
    AbandonTaskOrchestratorWorkItem = lambda *args, **kwargs: None
    AbandonTaskEntityWorkItem = lambda *args, **kwargs: None
