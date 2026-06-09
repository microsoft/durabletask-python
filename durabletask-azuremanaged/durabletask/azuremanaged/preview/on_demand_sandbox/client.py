# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Callable, Iterable, Optional, Sequence

import grpc
from azure.core.credentials import TokenCredential

from durabletask import task
from durabletask.azuremanaged.internal.durabletask_grpc_interceptor import (
    DTSDefaultClientInterceptorImpl,
)
from durabletask.azuremanaged.internal import on_demand_sandbox_activities_service_pb2 as pb
from durabletask.azuremanaged.internal import on_demand_sandbox_activities_service_pb2_grpc as stubs
from durabletask.grpc_options import GrpcChannelOptions
import durabletask.internal.shared as shared


DEFAULT_WORKER_PROFILE_ID = "default"
DEFAULT_CPU = "1000m"
DEFAULT_MEMORY = "2048Mi"
DEFAULT_MAX_CONCURRENT_ACTIVITIES = 100


@dataclass
class OnDemandSandboxWorkerProfileOptions:
    """Options for a decorated on-demand sandbox worker profile."""

    worker_profile_id: str
    # Full OCI image reference for the sandbox worker container, for example
    # "myregistry.azurecr.io/workers/hello:1.0" or
    # "myregistry.azurecr.io/workers/hello@sha256:0123456789abcdef...".
    container_image: Optional[str] = None
    image_pull_managed_identity_client_id: Optional[str] = None
    scheduler_managed_identity_client_id: Optional[str] = None
    cpu: str = DEFAULT_CPU
    memory: str = DEFAULT_MEMORY
    environment_variables: dict[str, str] = field(default_factory=dict)
    max_concurrent_activities: int = DEFAULT_MAX_CONCURRENT_ACTIVITIES
    entrypoint: list[str] = field(default_factory=list)
    cmd: list[str] = field(default_factory=list)
    activity_names: list[str] = field(default_factory=list)

    def add_activity(self, activity: str | Callable) -> None:
        """Add an activity to the on-demand sandbox worker profile declaration."""
        activity_name = task.get_name(activity) if callable(activity) else activity
        self.activity_names.append(
            _normalize_required(activity_name, "On-demand sandbox activity name is required."))


class OnDemandSandboxWorkerProfile:
    """Base class for configuring a decorated on-demand sandbox worker profile."""

    def configure(self, options: OnDemandSandboxWorkerProfileOptions) -> None:
        """Configure the on-demand sandbox worker profile declaration options."""


_worker_profiles: dict[str, OnDemandSandboxWorkerProfileOptions] = {}


def on_demand_sandbox_worker_profile(worker_profile_id: str) -> Callable[[type], type]:
    """Declare an on-demand sandbox worker profile using a decorated marker class."""
    normalized_profile = _normalize_required(worker_profile_id, "On-demand sandbox worker profile ID is required.")

    def decorator(cls: type) -> type:
        if normalized_profile in _worker_profiles:
            raise ValueError(f"On-demand sandbox worker profile '{normalized_profile}' is declared more than once.")

        options = OnDemandSandboxWorkerProfileOptions(worker_profile_id=normalized_profile)
        try:
            profile = cls()
        except TypeError as ex:
            raise TypeError("On-demand sandbox worker profile classes must have a parameterless constructor.") from ex

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


def build_on_demand_sandbox_activity_declaration(
        *,
        activity_names: str | Iterable[str],
        scheduler_managed_identity_client_id: str,
        worker_profile_id: str = DEFAULT_WORKER_PROFILE_ID,
        container_image: Optional[str] = None,
        image_pull_managed_identity_client_id: Optional[str] = None,
        cpu: str = DEFAULT_CPU,
        memory: str = DEFAULT_MEMORY,
        environment_variables: Optional[dict[str, str]] = None,
        max_concurrent_activities: int = DEFAULT_MAX_CONCURRENT_ACTIVITIES,
        entrypoint: Optional[Iterable[str]] = None,
        cmd: Optional[Iterable[str]] = None) -> pb.OnDemandSandboxActivityDeclaration:
    """Build a sandbox activity declaration.

    Args:
        container_image: Full OCI image reference for the sandbox worker container,
            such as "myregistry.azurecr.io/workers/hello:1.0" or
            "myregistry.azurecr.io/workers/hello@sha256:0123456789abcdef...".
    """
    resolved_activity_names = resolve_activity_names(activity_names)
    if not resolved_activity_names:
        raise ValueError("On-demand sandbox activity declaration requires at least one activity name.")

    if not worker_profile_id or not worker_profile_id.strip():
        raise ValueError("On-demand sandbox activity declaration requires a worker profile ID.")

    if max_concurrent_activities <= 0:
        raise ValueError("On-demand sandbox activity max concurrent activities must be greater than zero.")

    image_ref = _normalize_required(
        container_image,
        "On-demand sandbox activity image metadata requires a container image reference like "
        "'myregistry.azurecr.io/workers/hello:1.0' or "
        "'myregistry.azurecr.io/workers/hello@sha256:...'.")

    resolved_scheduler_managed_identity_client_id = _normalize_required(
        scheduler_managed_identity_client_id,
        "On-demand sandbox activity declaration requires the managed identity client ID workers use to connect to Durable Task Scheduler.")
    resolved_image_pull_managed_identity_client_id = _normalize_required(
        image_pull_managed_identity_client_id,
        "On-demand sandbox activity declaration requires the managed identity client ID ADC uses to pull the worker image.")

    resolved_cpu = _normalize_cpu(cpu)
    resolved_memory = _normalize_memory(memory)

    declaration = pb.OnDemandSandboxActivityDeclaration(
        worker_profile_id=worker_profile_id.strip(),
        image=pb.OnDemandSandboxActivityImage(
            image_ref=image_ref,
            managed_identity_client_id=resolved_image_pull_managed_identity_client_id),
        resources=pb.OnDemandSandboxActivityResources(
            cpu=resolved_cpu,
            memory=resolved_memory),
        scheduler_managed_identity_client_id=resolved_scheduler_managed_identity_client_id,
        max_concurrent_activities=max_concurrent_activities)
    declaration.activity_names.extend(resolved_activity_names)
    declaration.environment_variables.update(environment_variables or {})
    declaration.entrypoint.extend(_normalize_optional_strings(entrypoint or []))
    declaration.cmd.extend(_normalize_optional_strings(cmd or []))
    return declaration


def build_profile_on_demand_sandbox_activity_declarations() -> list[pb.OnDemandSandboxActivityDeclaration]:
    """Build on-demand sandbox declarations from worker profile configuration."""
    declarations: list[pb.OnDemandSandboxActivityDeclaration] = []
    activity_owners: dict[str, str] = {}
    for profile in _worker_profiles.values():
        activity_names = resolve_activity_names(profile.activity_names)
        if not activity_names:
            continue

        for activity_name in activity_names:
            existing_profile = activity_owners.get(activity_name)
            if existing_profile and existing_profile != profile.worker_profile_id:
                raise ValueError(
                    f"On-demand sandbox activity '{activity_name}' is assigned to both worker profile "
                    f"'{existing_profile}' and '{profile.worker_profile_id}'.")
            activity_owners[activity_name] = profile.worker_profile_id

        declarations.append(build_on_demand_sandbox_activity_declaration(
            activity_names=activity_names,
            worker_profile_id=profile.worker_profile_id,
            container_image=profile.container_image,
            image_pull_managed_identity_client_id=profile.image_pull_managed_identity_client_id,
            scheduler_managed_identity_client_id=profile.scheduler_managed_identity_client_id,
            cpu=profile.cpu,
            memory=profile.memory,
            environment_variables=profile.environment_variables,
            max_concurrent_activities=profile.max_concurrent_activities,
            entrypoint=profile.entrypoint,
            cmd=profile.cmd))

    return declarations


def build_on_demand_sandbox_worker_start(
        *,
        taskhub: str,
        worker_profile_id: str,
        max_activities_count: int,
        activity_names: Iterable[str],
        substrate: Optional[str] = None,
        dts_sandbox_identifier: Optional[str] = None) -> pb.OnDemandSandboxActivityWorkerMessage:
    if not taskhub or not taskhub.strip():
        raise ValueError("On-demand sandbox activity worker registration requires a task hub name.")

    if not worker_profile_id or not worker_profile_id.strip():
        raise ValueError("On-demand sandbox activity worker registration requires a worker profile ID.")

    if max_activities_count <= 0:
        raise ValueError("On-demand sandbox activity worker max activity count must be greater than zero.")

    resolved_activity_names = resolve_activity_names(activity_names)
    if not resolved_activity_names:
        raise ValueError("On-demand sandbox activity worker registration requires at least one registered activity.")

    message = pb.OnDemandSandboxActivityWorkerMessage(
        start=pb.OnDemandSandboxActivityWorkerStart(
            task_hub=taskhub.strip(),
            worker_profile_id=worker_profile_id.strip(),
            max_activities_count=max_activities_count,
            substrate=_parse_substrate(substrate),
            dts_sandbox_identifier=(dts_sandbox_identifier or "").strip()))
    message.start.activity_names.extend(resolved_activity_names)
    return message


def build_on_demand_sandbox_worker_heartbeat(active_activities_count: int) -> pb.OnDemandSandboxActivityWorkerMessage:
    if active_activities_count < 0:
        raise ValueError("On-demand sandbox activity worker active activity count cannot be negative.")

    return pb.OnDemandSandboxActivityWorkerMessage(
        heartbeat=pb.OnDemandSandboxActivityWorkerHeartbeat(
            active_activities_count=active_activities_count))


class OnDemandSandboxActivitiesClient:
    """Client for Durable Task Scheduler on-demand sandbox activity management operations."""

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
        self._stub = stubs.OnDemandSandboxActivitiesStub(channel)

    def close(self) -> None:
        if self._owns_channel:
            self._channel.close()

    def enable_on_demand_sandbox_activities(self) -> None:
        """Declare all configured on-demand sandbox worker profiles with Durable Task Scheduler."""
        declarations = build_profile_on_demand_sandbox_activity_declarations()
        if not declarations:
            raise ValueError("No configured on-demand sandbox activities were found.")

        for declaration in declarations:
            self._stub.DeclareOnDemandSandboxActivities(declaration)

    def remove_on_demand_sandbox_activity_declaration(self, worker_profile_id: str) -> None:
        worker_profile_id = _normalize_required(worker_profile_id, "Worker profile ID is required.")
        self._stub.RemoveOnDemandSandboxActivityDeclaration(
            pb.RemoveOnDemandSandboxActivityDeclarationRequest(worker_profile_id=worker_profile_id))

    def connect_on_demand_sandbox_activity_worker(
            self,
            messages: Iterable[pb.OnDemandSandboxActivityWorkerMessage]
    ) -> pb.OnDemandSandboxActivityWorkerSessionResult:
        return self._stub.ConnectOnDemandSandboxActivityWorker(messages)


def _normalize_optional_strings(values: Iterable[str]) -> list[str]:
    return [value.strip() for value in values if value and value.strip()]


def _normalize_required(value: Optional[str], message: str) -> str:
    if not value or not value.strip():
        raise ValueError(message)
    return value.strip()


def _normalize_cpu(value: str) -> str:
    normalized = _normalize_required(value, "On-demand sandbox activity declaration requires CPU resources.")
    milli_cpu = _try_parse_cpu_millicores(normalized)
    if milli_cpu is None or milli_cpu <= 0:
        raise ValueError(
            "On-demand sandbox activity CPU resources must be a positive Kubernetes-style CPU quantity. "
            "Use formats like '500m', '2', or '0.5'.")
    return normalized


def _normalize_memory(value: str) -> str:
    normalized = _normalize_required(value, "On-demand sandbox activity declaration requires memory resources.")
    memory_mib = _try_parse_memory_mib(normalized)
    if memory_mib is None or memory_mib <= 0:
        raise ValueError(
            "On-demand sandbox activity memory resources must be a positive Kubernetes-style memory quantity. "
            "Use formats like '256Mi', '1Gi', or '2048'.")
    return normalized


def _try_parse_cpu_millicores(value: str) -> Optional[int]:
    try:
        if value[-1:].lower() == "m":
            return int(Decimal(value[:-1]))
        return int(Decimal(value) * 1000)
    except (InvalidOperation, ValueError):
        return None


def _try_parse_memory_mib(value: str) -> Optional[int]:
    try:
        if value[-2:].lower() == "gi":
            return int(Decimal(value[:-2]) * 1024)
        if value[-2:].lower() == "mi":
            return int(Decimal(value[:-2]))
        return int(Decimal(value))
    except (InvalidOperation, ValueError):
        return None


def _parse_substrate(substrate: Optional[str]) -> "pb.SubstrateKind":
    if not substrate:
        return pb.SUBSTRATE_KIND_UNSPECIFIED
    if substrate.lower() == "sandbox":
        return pb.SUBSTRATE_KIND_SANDBOX
    if substrate.lower() == "acasessionpool":
        return pb.SUBSTRATE_KIND_ACA_SESSION_POOL
    return pb.SUBSTRATE_KIND_UNSPECIFIED
