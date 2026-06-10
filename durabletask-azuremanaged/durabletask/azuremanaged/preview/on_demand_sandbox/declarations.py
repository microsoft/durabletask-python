# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Optional

from durabletask import task
from durabletask.azuremanaged.internal import on_demand_sandbox_activities_service_pb2 as pb
from durabletask.azuremanaged.preview.on_demand_sandbox.helpers import (
    normalize_required,
    resolve_activity_names,
)


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
    environment_variables: dict[str, str] = field(default_factory=dict[str, str])
    max_concurrent_activities: int = DEFAULT_MAX_CONCURRENT_ACTIVITIES
    entrypoint: list[str] = field(default_factory=list[str])
    cmd: list[str] = field(default_factory=list[str])
    activity_names: list[str] = field(default_factory=list[str])

    def add_activity(self, activity: str | Callable[..., Any]) -> None:
        """Add an activity to the on-demand sandbox worker profile declaration."""
        activity_name = task.get_name(activity) if callable(activity) else activity
        self.activity_names.append(
            normalize_required(activity_name, "On-demand sandbox activity name is required."))


class OnDemandSandboxWorkerProfile:
    """Base class for configuring a decorated on-demand sandbox worker profile."""

    def configure(self, options: OnDemandSandboxWorkerProfileOptions) -> None:
        """Configure the on-demand sandbox worker profile declaration options."""


_worker_profiles: dict[str, OnDemandSandboxWorkerProfileOptions] = {}


def on_demand_sandbox_worker_profile(worker_profile_id: str) -> Callable[[type], type]:
    """Declare an on-demand sandbox worker profile using a decorated marker class."""
    normalized_profile = normalize_required(worker_profile_id, "On-demand sandbox worker profile ID is required.")

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

        if not resolve_activity_names(options.activity_names):
            raise ValueError(
                f"On-demand sandbox worker profile '{normalized_profile}' must declare at least one activity.")

        _worker_profiles[normalized_profile] = options
        return cls

    return decorator


def _build_on_demand_sandbox_activity_declaration(
        *,
        activity_names: str | Iterable[str],
        scheduler_managed_identity_client_id: Optional[str],
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

    image_ref = normalize_required(
        container_image,
        "On-demand sandbox activity image metadata requires a container image reference like "
        "'myregistry.azurecr.io/workers/hello:1.0' or "
        "'myregistry.azurecr.io/workers/hello@sha256:...'.")

    resolved_scheduler_managed_identity_client_id = normalize_required(
        scheduler_managed_identity_client_id,
        "On-demand sandbox activity declaration requires the managed identity client ID workers use to connect to Durable Task Scheduler.")
    resolved_image_pull_managed_identity_client_id = normalize_required(
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

        for activity_name in activity_names:
            existing_profile = activity_owners.get(activity_name)
            if existing_profile and existing_profile != profile.worker_profile_id:
                raise ValueError(
                    f"On-demand sandbox activity '{activity_name}' is assigned to both worker profile "
                    f"'{existing_profile}' and '{profile.worker_profile_id}'.")
            activity_owners[activity_name] = profile.worker_profile_id

        declarations.append(_build_on_demand_sandbox_activity_declaration(
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


def _normalize_optional_strings(values: Iterable[str]) -> list[str]:
    return [value.strip() for value in values if value and value.strip()]


def _normalize_cpu(value: str) -> str:
    normalized = normalize_required(value, "On-demand sandbox activity declaration requires CPU resources.")
    milli_cpu = _try_parse_cpu_millicores(normalized)
    if milli_cpu is None or milli_cpu <= 0:
        raise ValueError(
            "On-demand sandbox activity CPU resources must be a positive Kubernetes-style CPU quantity. "
            "Use formats like '500m', '2', or '0.5'.")
    return normalized


def _normalize_memory(value: str) -> str:
    normalized = normalize_required(value, "On-demand sandbox activity declaration requires memory resources.")
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
