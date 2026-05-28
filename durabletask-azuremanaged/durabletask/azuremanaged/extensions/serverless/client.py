# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional, Sequence

import grpc
from azure.core.credentials import TokenCredential

from durabletask import task
from durabletask.azuremanaged.internal.durabletask_grpc_interceptor import (
    DTSDefaultClientInterceptorImpl,
)
from durabletask.azuremanaged.internal import serverless_activities_service_pb2 as pb
from durabletask.azuremanaged.internal import serverless_activities_service_pb2_grpc as stubs
from durabletask.grpc_options import GrpcChannelOptions
import durabletask.internal.shared as shared


DEFAULT_WORKER_PROFILE_ID = "default"
DEFAULT_CPU = "1000m"
DEFAULT_MEMORY = "2048Mi"
DEFAULT_MAX_CONCURRENT_ACTIVITIES = 100


@dataclass
class ServerlessWorkerProfileOptions:
    """Options for a decorated serverless worker profile."""

    worker_profile_id: str
    container_image: Optional[str] = None
    registry_server: Optional[str] = None
    repository: Optional[str] = None
    tag: Optional[str] = None
    image_digest: Optional[str] = None
    cpu: str = DEFAULT_CPU
    memory: str = DEFAULT_MEMORY
    environment_variables: dict[str, str] = field(default_factory=dict)
    max_concurrent_activities: int = DEFAULT_MAX_CONCURRENT_ACTIVITIES
    entrypoint: list[str] = field(default_factory=list)
    cmd: list[str] = field(default_factory=list)
    activity_names: list[str] = field(default_factory=list)

    def add_activity(self, activity: str | Callable) -> None:
        """Add an activity to the serverless worker profile declaration."""
        activity_name = task.get_name(activity) if callable(activity) else activity
        self.activity_names.append(
            _normalize_required(activity_name, "Serverless activity name is required."))


class ServerlessWorkerProfile:
    """Base class for configuring a decorated serverless worker profile."""

    def configure(self, options: ServerlessWorkerProfileOptions) -> None:
        """Configure the serverless worker profile declaration options."""


_worker_profiles: dict[str, ServerlessWorkerProfileOptions] = {}


def serverless_worker_profile(worker_profile_id: str) -> Callable[[type], type]:
    """Declare a serverless worker profile using a decorated marker class."""
    normalized_profile = _normalize_required(worker_profile_id, "Serverless worker profile ID is required.")

    def decorator(cls: type) -> type:
        if normalized_profile in _worker_profiles:
            raise ValueError(f"Serverless worker profile '{normalized_profile}' is declared more than once.")

        options = ServerlessWorkerProfileOptions(worker_profile_id=normalized_profile)
        try:
            profile = cls()
        except TypeError as ex:
            raise TypeError("Serverless worker profile classes must have a parameterless constructor.") from ex

        configure = getattr(profile, "configure", None)
        if callable(configure):
            configure(options)

        _worker_profiles[normalized_profile] = options
        return cls

    return decorator


def resolve_activity_names(activity_names: str | Iterable[str]) -> list[str]:
    resolved: list[str] = []
    seen: set[str] = set()
    names = [activity_names] if isinstance(activity_names, str) else activity_names
    for name in names:
        normalized = name.strip()
        if normalized and normalized not in seen:
            resolved.append(normalized)
            seen.add(normalized)
    return resolved


def build_image_ref(
        *,
        container_image: Optional[str] = None,
        registry_server: Optional[str] = None,
        repository: Optional[str] = None,
        tag: Optional[str] = None,
        image_digest: Optional[str] = None) -> Optional[str]:
    if container_image and container_image.strip():
        return container_image.strip()

    if not repository or not repository.strip():
        return None

    image = repository.strip()
    if registry_server and registry_server.strip():
        image = f"{registry_server.strip()}/{image}"

    if image_digest and image_digest.strip():
        return f"{image}@{image_digest.strip()}"

    if tag and tag.strip():
        return f"{image}:{tag.strip()}"

    return image


def build_serverless_activity_declaration(
        *,
        activity_names: str | Iterable[str],
        worker_profile_id: str = DEFAULT_WORKER_PROFILE_ID,
        container_image: Optional[str] = None,
        registry_server: Optional[str] = None,
        repository: Optional[str] = None,
        tag: Optional[str] = None,
        image_digest: Optional[str] = None,
        cpu: str = DEFAULT_CPU,
        memory: str = DEFAULT_MEMORY,
        environment_variables: Optional[dict[str, str]] = None,
        max_concurrent_activities: int = DEFAULT_MAX_CONCURRENT_ACTIVITIES,
        entrypoint: Optional[Iterable[str]] = None,
        cmd: Optional[Iterable[str]] = None) -> pb.ServerlessActivityDeclaration:
    resolved_activity_names = resolve_activity_names(activity_names)
    if not resolved_activity_names:
        raise ValueError("Serverless activity declaration requires at least one activity name.")

    if not worker_profile_id or not worker_profile_id.strip():
        raise ValueError("Serverless activity declaration requires a worker profile ID.")

    if max_concurrent_activities <= 0:
        raise ValueError("Serverless activity max concurrent activities must be greater than zero.")

    image_ref = build_image_ref(
        container_image=container_image,
        registry_server=registry_server,
        repository=repository,
        tag=tag,
        image_digest=image_digest)
    if not image_ref:
        raise ValueError("Serverless activity image metadata requires a container image reference.")

    if not cpu or not cpu.strip():
        raise ValueError("Serverless activity declaration requires CPU resources.")

    if not memory or not memory.strip():
        raise ValueError("Serverless activity declaration requires memory resources.")

    declaration = pb.ServerlessActivityDeclaration(
        worker_profile_id=worker_profile_id.strip(),
        image=pb.ServerlessActivityImage(
            image_ref=image_ref),
        resources=pb.ServerlessActivityResources(
            cpu=cpu.strip(),
            memory=memory.strip()),
        max_concurrent_activities=max_concurrent_activities)
    declaration.activity_names.extend(resolved_activity_names)
    declaration.environment_variables.update(environment_variables or {})
    declaration.entrypoint.extend(_normalize_optional_strings(entrypoint or []))
    declaration.cmd.extend(_normalize_optional_strings(cmd or []))
    return declaration


def build_profile_serverless_activity_declarations() -> list[pb.ServerlessActivityDeclaration]:
    """Build serverless declarations from worker profile configuration."""
    declarations: list[pb.ServerlessActivityDeclaration] = []
    activity_owners: dict[str, str] = {}
    for profile in _worker_profiles.values():
        activity_names = resolve_activity_names(profile.activity_names)
        if not activity_names:
            continue

        for activity_name in activity_names:
            existing_profile = activity_owners.get(activity_name)
            if existing_profile and existing_profile != profile.worker_profile_id:
                raise ValueError(
                    f"Serverless activity '{activity_name}' is assigned to both worker profile "
                    f"'{existing_profile}' and '{profile.worker_profile_id}'.")
            activity_owners[activity_name] = profile.worker_profile_id

        declarations.append(build_serverless_activity_declaration(
            activity_names=activity_names,
            worker_profile_id=profile.worker_profile_id,
            container_image=profile.container_image,
            registry_server=profile.registry_server,
            repository=profile.repository,
            tag=profile.tag,
            image_digest=profile.image_digest,
            cpu=profile.cpu,
            memory=profile.memory,
            environment_variables=profile.environment_variables,
            max_concurrent_activities=profile.max_concurrent_activities,
            entrypoint=profile.entrypoint,
            cmd=profile.cmd))

    return declarations


def build_serverless_worker_start(
        *,
        taskhub: str,
        worker_profile_id: str,
        max_activities_count: int,
        activity_names: Iterable[str],
        substrate: Optional[str] = None,
        dts_sandbox_identifier: Optional[str] = None) -> pb.ServerlessActivityWorkerMessage:
    if not taskhub or not taskhub.strip():
        raise ValueError("Serverless activity worker registration requires a task hub name.")

    if not worker_profile_id or not worker_profile_id.strip():
        raise ValueError("Serverless activity worker registration requires a worker profile ID.")

    if max_activities_count <= 0:
        raise ValueError("Serverless activity worker max activity count must be greater than zero.")

    resolved_activity_names = resolve_activity_names(activity_names)
    if not resolved_activity_names:
        raise ValueError("Serverless activity worker registration requires at least one registered activity.")

    message = pb.ServerlessActivityWorkerMessage(
        start=pb.ServerlessActivityWorkerStart(
            task_hub=taskhub.strip(),
            worker_profile_id=worker_profile_id.strip(),
            max_activities_count=max_activities_count,
            substrate=_parse_substrate(substrate),
            dts_sandbox_identifier=(dts_sandbox_identifier or "").strip()))
    message.start.activity_names.extend(resolved_activity_names)
    return message


def build_serverless_worker_heartbeat(active_activities_count: int) -> pb.ServerlessActivityWorkerMessage:
    if active_activities_count < 0:
        raise ValueError("Serverless activity worker active activity count cannot be negative.")

    return pb.ServerlessActivityWorkerMessage(
        heartbeat=pb.ServerlessActivityWorkerHeartbeat(
            active_activities_count=active_activities_count))


class ServerlessActivitiesClient:
    """Client for DTS serverless activity management operations."""

    def __init__(
            self, *,
            host_address: str,
            taskhub: str,
            token_credential: Optional[TokenCredential],
            channel: Optional[grpc.Channel] = None,
            secure_channel: bool = True,
            interceptors: Optional[Sequence[shared.ClientInterceptor]] = None,
            channel_options: Optional[GrpcChannelOptions] = None):
        if not taskhub:
            raise ValueError("Taskhub value cannot be empty. Please provide a value for your taskhub")

        self._owns_channel = channel is None
        if channel is None:
            resolved_interceptors: list[shared.ClientInterceptor] = (
                list(interceptors) if interceptors is not None else []
            )
            resolved_interceptors.append(DTSDefaultClientInterceptorImpl(token_credential, taskhub))
            channel = shared.get_grpc_channel(
                host_address=host_address,
                secure_channel=secure_channel,
                interceptors=resolved_interceptors,
                channel_options=channel_options)
        self._channel = channel
        self._stub = stubs.ServerlessActivitiesStub(channel)

    def close(self) -> None:
        if self._owns_channel:
            self._channel.close()

    def enable_serverless_activities(self) -> None:
        """Declare all configured serverless worker profiles with DTS."""
        declarations = build_profile_serverless_activity_declarations()
        if not declarations:
            raise ValueError("No configured serverless activities were found.")

        for declaration in declarations:
            self._stub.DeclareServerlessActivities(declaration)

    def remove_serverless_activity_declaration(self, worker_profile_id: str) -> None:
        worker_profile_id = _normalize_required(worker_profile_id, "Worker profile ID is required.")
        self._stub.RemoveServerlessActivityDeclaration(
            pb.RemoveServerlessActivityDeclarationRequest(worker_profile_id=worker_profile_id))

    def connect_serverless_activity_worker(
            self,
            messages: Iterable[pb.ServerlessActivityWorkerMessage]) -> pb.ServerlessActivityWorkerSessionResult:
        return self._stub.ConnectServerlessActivityWorker(messages)


def _normalize_optional_strings(values: Iterable[str]) -> list[str]:
    return [value.strip() for value in values if value and value.strip()]


def _normalize_required(value: str, message: str) -> str:
    if not value or not value.strip():
        raise ValueError(message)
    return value.strip()


def _parse_substrate(substrate: Optional[str]) -> "pb.SubstrateKind":
    if not substrate:
        return pb.SUBSTRATE_KIND_UNSPECIFIED
    if substrate.lower() == "sandbox":
        return pb.SUBSTRATE_KIND_SANDBOX
    if substrate.lower() == "acasessionpool":
        return pb.SUBSTRATE_KIND_ACA_SESSION_POOL
    return pb.SUBSTRATE_KIND_UNSPECIFIED
