from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SandboxProviderKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SANDBOX_PROVIDER_KIND_UNSPECIFIED: _ClassVar[SandboxProviderKind]
    SANDBOX_PROVIDER_KIND_ACA_SESSION_POOL: _ClassVar[SandboxProviderKind]
    SANDBOX_PROVIDER_KIND_SANDBOX: _ClassVar[SandboxProviderKind]
SANDBOX_PROVIDER_KIND_UNSPECIFIED: SandboxProviderKind
SANDBOX_PROVIDER_KIND_ACA_SESSION_POOL: SandboxProviderKind
SANDBOX_PROVIDER_KIND_SANDBOX: SandboxProviderKind

class SandboxActivityWorkerMessage(_message.Message):
    __slots__ = ("start", "heartbeat")
    START_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_FIELD_NUMBER: _ClassVar[int]
    start: SandboxActivityWorkerStart
    heartbeat: SandboxActivityWorkerHeartbeat
    def __init__(self, start: _Optional[_Union[SandboxActivityWorkerStart, _Mapping]] = ..., heartbeat: _Optional[_Union[SandboxActivityWorkerHeartbeat, _Mapping]] = ...) -> None: ...

class SandboxActivityWorkerStart(_message.Message):
    __slots__ = ("task_hub", "max_activities_count", "sandbox_provider", "dts_sandbox_identifier", "worker_profile_id", "activity_names")
    TASK_HUB_FIELD_NUMBER: _ClassVar[int]
    MAX_ACTIVITIES_COUNT_FIELD_NUMBER: _ClassVar[int]
    SANDBOX_PROVIDER_FIELD_NUMBER: _ClassVar[int]
    DTS_SANDBOX_IDENTIFIER_FIELD_NUMBER: _ClassVar[int]
    WORKER_PROFILE_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_NAMES_FIELD_NUMBER: _ClassVar[int]
    task_hub: str
    max_activities_count: int
    sandbox_provider: SandboxProviderKind
    dts_sandbox_identifier: str
    worker_profile_id: str
    activity_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, task_hub: _Optional[str] = ..., max_activities_count: _Optional[int] = ..., sandbox_provider: _Optional[_Union[SandboxProviderKind, str]] = ..., dts_sandbox_identifier: _Optional[str] = ..., worker_profile_id: _Optional[str] = ..., activity_names: _Optional[_Iterable[str]] = ...) -> None: ...

class SandboxActivityWorkerHeartbeat(_message.Message):
    __slots__ = ("active_activities_count",)
    ACTIVE_ACTIVITIES_COUNT_FIELD_NUMBER: _ClassVar[int]
    active_activities_count: int
    def __init__(self, active_activities_count: _Optional[int] = ...) -> None: ...

class SandboxActivityWorkerSessionResult(_message.Message):
    __slots__ = ("message",)
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    message: str
    def __init__(self, message: _Optional[str] = ...) -> None: ...

class SandboxActivityDeclaration(_message.Message):
    __slots__ = ("worker_profile_id", "activity_names", "image", "environment_variables", "max_concurrent_activities", "resources", "entrypoint", "cmd", "scheduler_managed_identity_client_id")
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
    SCHEDULER_MANAGED_IDENTITY_CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    worker_profile_id: str
    activity_names: _containers.RepeatedScalarFieldContainer[str]
    image: SandboxActivityImage
    environment_variables: _containers.ScalarMap[str, str]
    max_concurrent_activities: int
    resources: SandboxActivityResources
    entrypoint: _containers.RepeatedScalarFieldContainer[str]
    cmd: _containers.RepeatedScalarFieldContainer[str]
    scheduler_managed_identity_client_id: str
    def __init__(self, worker_profile_id: _Optional[str] = ..., activity_names: _Optional[_Iterable[str]] = ..., image: _Optional[_Union[SandboxActivityImage, _Mapping]] = ..., environment_variables: _Optional[_Mapping[str, str]] = ..., max_concurrent_activities: _Optional[int] = ..., resources: _Optional[_Union[SandboxActivityResources, _Mapping]] = ..., entrypoint: _Optional[_Iterable[str]] = ..., cmd: _Optional[_Iterable[str]] = ..., scheduler_managed_identity_client_id: _Optional[str] = ...) -> None: ...

class SandboxActivityImage(_message.Message):
    __slots__ = ("image_ref", "managed_identity_client_id")
    IMAGE_REF_FIELD_NUMBER: _ClassVar[int]
    MANAGED_IDENTITY_CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    image_ref: str
    managed_identity_client_id: str
    def __init__(self, image_ref: _Optional[str] = ..., managed_identity_client_id: _Optional[str] = ...) -> None: ...

class SandboxActivityResources(_message.Message):
    __slots__ = ("cpu", "memory")
    CPU_FIELD_NUMBER: _ClassVar[int]
    MEMORY_FIELD_NUMBER: _ClassVar[int]
    cpu: str
    memory: str
    def __init__(self, cpu: _Optional[str] = ..., memory: _Optional[str] = ...) -> None: ...

class SandboxActivityDeclarationResult(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RemoveSandboxActivityDeclarationRequest(_message.Message):
    __slots__ = ("worker_profile_id",)
    WORKER_PROFILE_ID_FIELD_NUMBER: _ClassVar[int]
    worker_profile_id: str
    def __init__(self, worker_profile_id: _Optional[str] = ...) -> None: ...

class RemoveSandboxActivityDeclarationResult(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
