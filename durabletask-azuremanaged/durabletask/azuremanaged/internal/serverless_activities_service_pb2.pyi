from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SubstrateKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SUBSTRATE_KIND_UNSPECIFIED: _ClassVar[SubstrateKind]
    SUBSTRATE_KIND_ACA_SESSION_POOL: _ClassVar[SubstrateKind]
    SUBSTRATE_KIND_SANDBOX: _ClassVar[SubstrateKind]
SUBSTRATE_KIND_UNSPECIFIED: SubstrateKind
SUBSTRATE_KIND_ACA_SESSION_POOL: SubstrateKind
SUBSTRATE_KIND_SANDBOX: SubstrateKind

class OnDemandSandboxActivityWorkerMessage(_message.Message):
    __slots__ = ("start", "heartbeat")
    START_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_FIELD_NUMBER: _ClassVar[int]
    start: OnDemandSandboxActivityWorkerStart
    heartbeat: OnDemandSandboxActivityWorkerHeartbeat
    def __init__(self, start: _Optional[_Union[OnDemandSandboxActivityWorkerStart, _Mapping]] = ..., heartbeat: _Optional[_Union[OnDemandSandboxActivityWorkerHeartbeat, _Mapping]] = ...) -> None: ...

class OnDemandSandboxActivityWorkerStart(_message.Message):
    __slots__ = ("task_hub", "max_activities_count", "substrate", "dts_sandbox_identifier", "worker_profile_id", "activity_names")
    TASK_HUB_FIELD_NUMBER: _ClassVar[int]
    MAX_ACTIVITIES_COUNT_FIELD_NUMBER: _ClassVar[int]
    SUBSTRATE_FIELD_NUMBER: _ClassVar[int]
    DTS_SANDBOX_IDENTIFIER_FIELD_NUMBER: _ClassVar[int]
    WORKER_PROFILE_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_NAMES_FIELD_NUMBER: _ClassVar[int]
    task_hub: str
    max_activities_count: int
    substrate: SubstrateKind
    dts_sandbox_identifier: str
    worker_profile_id: str
    activity_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, task_hub: _Optional[str] = ..., max_activities_count: _Optional[int] = ..., substrate: _Optional[_Union[SubstrateKind, str]] = ..., dts_sandbox_identifier: _Optional[str] = ..., worker_profile_id: _Optional[str] = ..., activity_names: _Optional[_Iterable[str]] = ...) -> None: ...

class OnDemandSandboxActivityWorkerHeartbeat(_message.Message):
    __slots__ = ("active_activities_count",)
    ACTIVE_ACTIVITIES_COUNT_FIELD_NUMBER: _ClassVar[int]
    active_activities_count: int
    def __init__(self, active_activities_count: _Optional[int] = ...) -> None: ...

class OnDemandSandboxActivityWorkerSessionResult(_message.Message):
    __slots__ = ("accepted", "message")
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    accepted: bool
    message: str
    def __init__(self, accepted: bool = ..., message: _Optional[str] = ...) -> None: ...

class OnDemandSandboxActivityDeclaration(_message.Message):
    __slots__ = ("worker_profile_id", "activity_names", "image", "environment_variables", "max_concurrent_activities", "resources", "entrypoint", "cmd")
    class EnvironmentVariablesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    WORKER_PROFILE_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_NAMES_FIELD_NUMBER: _ClassVar[int]
    IMAGE_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_VARIABLES_FIELD_NUMBER: _ClassVar[int]
    MAX_CONCURRENT_ACTIVITIES_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    ENTRYPOINT_FIELD_NUMBER: _ClassVar[int]
    CMD_FIELD_NUMBER: _ClassVar[int]
    worker_profile_id: str
    activity_names: _containers.RepeatedScalarFieldContainer[str]
    image: OnDemandSandboxActivityImage
    environment_variables: _containers.ScalarMap[str, str]
    max_concurrent_activities: int
    resources: OnDemandSandboxActivityResources
    entrypoint: _containers.RepeatedScalarFieldContainer[str]
    cmd: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, worker_profile_id: _Optional[str] = ..., activity_names: _Optional[_Iterable[str]] = ..., image: _Optional[_Union[OnDemandSandboxActivityImage, _Mapping]] = ..., environment_variables: _Optional[_Mapping[str, str]] = ..., max_concurrent_activities: _Optional[int] = ..., resources: _Optional[_Union[OnDemandSandboxActivityResources, _Mapping]] = ..., entrypoint: _Optional[_Iterable[str]] = ..., cmd: _Optional[_Iterable[str]] = ...) -> None: ...

class OnDemandSandboxActivityImage(_message.Message):
    __slots__ = ("image_ref",)
    IMAGE_REF_FIELD_NUMBER: _ClassVar[int]
    image_ref: str
    def __init__(self, image_ref: _Optional[str] = ...) -> None: ...

class OnDemandSandboxActivityResources(_message.Message):
    __slots__ = ("cpu", "memory")
    CPU_FIELD_NUMBER: _ClassVar[int]
    MEMORY_FIELD_NUMBER: _ClassVar[int]
    cpu: str
    memory: str
    def __init__(self, cpu: _Optional[str] = ..., memory: _Optional[str] = ...) -> None: ...

class OnDemandSandboxActivityDeclarationResult(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RemoveOnDemandSandboxActivityDeclarationRequest(_message.Message):
    __slots__ = ("worker_profile_id",)
    WORKER_PROFILE_ID_FIELD_NUMBER: _ClassVar[int]
    worker_profile_id: str
    def __init__(self, worker_profile_id: _Optional[str] = ...) -> None: ...

class RemoveOnDemandSandboxActivityDeclarationResult(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
