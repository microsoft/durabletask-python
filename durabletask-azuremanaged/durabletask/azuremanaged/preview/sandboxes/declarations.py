# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Optional

from durabletask import task
from durabletask.azuremanaged.internal import sandbox_service_pb2 as pb
from durabletask.azuremanaged.preview.sandboxes.helpers import (
    normalize_required,
    resolve_activity_names,
)


DEFAULT_CPU = "1000m"
DEFAULT_MEMORY = "2048Mi"
DEFAULT_MAX_CONCURRENT_ACTIVITIES = 100


@dataclass
class SandboxWorkerProfileOptions:
    """Options for a decorated sandbox worker profile."""

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
        """Add an activity to the sandbox worker profile declaration."""
        activity_name = task.get_name(activity) if callable(activity) else activity
        self.activity_names.append(
            normalize_required(activity_name, "Sandbox activity name is required."))


class SandboxWorkerProfile:
    """Base class for configuring a decorated sandbox worker profile."""

    def configure(self, options: SandboxWorkerProfileOptions) -> None:
        """Configure the sandbox worker profile declaration options."""


_worker_profiles: dict[str, SandboxWorkerProfileOptions] = {}


def sandbox_worker_profile(worker_profile_id: str) -> Callable[[type], type]:
    """Declare a sandbox worker profile using a decorated marker class."""
    normalized_profile = normalize_required(worker_profile_id, "Sandbox worker profile ID is required.")

    def decorator(cls: type) -> type:
        if normalized_profile in _worker_profiles:
            raise ValueError(f"Sandbox worker profile '{normalized_profile}' is declared more than once.")

        options = SandboxWorkerProfileOptions(worker_profile_id=normalized_profile)
        try:
            profile = cls()
        except TypeError as ex:
            raise TypeError("Sandbox worker profile classes must have a parameterless constructor.") from ex

        configure = getattr(profile, "configure", None)
        if callable(configure):
            configure(options)

        if not resolve_activity_names(options.activity_names):
            raise ValueError(
                f"Sandbox worker profile '{normalized_profile}' must declare at least one activity.")

        _worker_profiles[normalized_profile] = options
        return cls

    return decorator


def _build_sandbox_activity_declaration(
        *,
        activity_names: str | Iterable[str],
        scheduler_managed_identity_client_id: Optional[str],
        worker_profile_id: str,
        container_image: Optional[str] = None,
        image_pull_managed_identity_client_id: Optional[str] = None,
        cpu: str = DEFAULT_CPU,
        memory: str = DEFAULT_MEMORY,
        environment_variables: Optional[dict[str, str]] = None,
        max_concurrent_activities: int = DEFAULT_MAX_CONCURRENT_ACTIVITIES,
        entrypoint: Optional[Iterable[str]] = None,
        cmd: Optional[Iterable[str]] = None) -> pb.SandboxActivityDeclaration:
    """Build a sandbox activity declaration.

    Args:
        container_image: Full OCI image reference for the sandbox worker container,
            such as "myregistry.azurecr.io/workers/hello:1.0" or
            "myregistry.azurecr.io/workers/hello@sha256:0123456789abcdef...".
    """
    resolved_activity_names = resolve_activity_names(activity_names)
    if not resolved_activity_names:
        raise ValueError("Sandbox activity declaration requires at least one activity name.")

    if not worker_profile_id or not worker_profile_id.strip():
        raise ValueError("Sandbox activity declaration requires a worker profile ID.")

    if max_concurrent_activities <= 0:
        raise ValueError("Sandbox activity max concurrent activities must be greater than zero.")

    image_ref = normalize_required(
        container_image,
        "Sandbox activity image metadata requires a container image reference like "
        "'myregistry.azurecr.io/workers/hello:1.0' or "
        "'myregistry.azurecr.io/workers/hello@sha256:...'.")

    resolved_scheduler_managed_identity_client_id = normalize_required(
        scheduler_managed_identity_client_id,
        "Sandbox activity declaration requires the managed identity client ID workers use to connect to Durable Task Scheduler.")
    resolved_image_pull_managed_identity_client_id = normalize_required(
        image_pull_managed_identity_client_id,
        "Sandbox activity declaration requires the managed identity client ID ADC uses to pull the worker image.")

    resolved_cpu = _normalize_cpu(cpu)
    resolved_memory = _normalize_memory(memory)

    declaration = pb.SandboxActivityDeclaration(
        worker_profile_id=worker_profile_id.strip(),
        image=pb.SandboxActivityImage(
            image_ref=image_ref,
            managed_identity_client_id=resolved_image_pull_managed_identity_client_id),
        resources=pb.SandboxActivityResources(
            cpu=resolved_cpu,
            memory=resolved_memory),
        scheduler_managed_identity_client_id=resolved_scheduler_managed_identity_client_id,
        max_concurrent_activities=max_concurrent_activities)
    declaration.activity_names.extend(resolved_activity_names)
    declaration.environment_variables.update(environment_variables or {})
    declaration.entrypoint.extend(_normalize_optional_strings(entrypoint or []))
    declaration.cmd.extend(_normalize_optional_strings(cmd or []))
    return declaration


def build_profile_sandbox_activity_declarations() -> list[pb.SandboxActivityDeclaration]:
    """Build sandbox declarations from worker profile configuration."""
    declarations: list[pb.SandboxActivityDeclaration] = []
    activity_owners: dict[str, str] = {}
    for profile in _worker_profiles.values():
        activity_names = resolve_activity_names(profile.activity_names)

        for activity_name in activity_names:
            activity_key = activity_name.casefold()
            existing_profile = activity_owners.get(activity_key)
            if existing_profile and existing_profile != profile.worker_profile_id:
                raise ValueError(
                    f"Sandbox activity '{activity_name}' is assigned to both worker profile "
                    f"'{existing_profile}' and '{profile.worker_profile_id}'.")
            activity_owners[activity_key] = profile.worker_profile_id

        declarations.append(_build_sandbox_activity_declaration(
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


def build_sandbox_worker_start(
        *,
        taskhub: str,
        worker_profile_id: str,
        max_activities_count: int,
        activity_names: Iterable[str],
        sandbox_provider: Optional[str] = None,
        dts_sandbox_identifier: Optional[str] = None) -> pb.SandboxActivityWorkerMessage:
    if not taskhub or not taskhub.strip():
        raise ValueError("Sandbox activity worker registration requires a task hub name.")

    if not worker_profile_id or not worker_profile_id.strip():
        raise ValueError("Sandbox activity worker registration requires a worker profile ID.")

    if max_activities_count <= 0:
        raise ValueError("Sandbox activity worker max activity count must be greater than zero.")

    resolved_activity_names = resolve_activity_names(activity_names)
    if not resolved_activity_names:
        raise ValueError("Sandbox activity worker registration requires at least one registered activity.")

    message = pb.SandboxActivityWorkerMessage(
        start=pb.SandboxActivityWorkerStart(
            task_hub=taskhub.strip(),
            worker_profile_id=worker_profile_id.strip(),
            max_activities_count=max_activities_count,
            sandbox_provider=_parse_sandbox_provider(sandbox_provider),
            dts_sandbox_identifier=(dts_sandbox_identifier or "").strip()))
    message.start.activity_names.extend(resolved_activity_names)
    return message


def build_sandbox_worker_heartbeat(active_activities_count: int) -> pb.SandboxActivityWorkerMessage:
    if active_activities_count < 0:
        raise ValueError("Sandbox activity worker active activity count cannot be negative.")

    return pb.SandboxActivityWorkerMessage(
        heartbeat=pb.SandboxActivityWorkerHeartbeat(
            active_activities_count=active_activities_count))


def _normalize_optional_strings(values: Iterable[str]) -> list[str]:
    return [value.strip() for value in values if value and value.strip()]


def _normalize_cpu(value: str) -> str:
    normalized = normalize_required(value, "Sandbox activity declaration requires CPU resources.")
    milli_cpu = _try_parse_cpu_millicores(normalized)
    if milli_cpu is None or milli_cpu <= 0:
        raise ValueError(
            "Sandbox activity CPU resources must be a positive Kubernetes-style CPU quantity. "
            "Use formats like '500m', '2', or '0.5'.")
    return normalized


def _normalize_memory(value: str) -> str:
    normalized = normalize_required(value, "Sandbox activity declaration requires memory resources.")
    memory_mib = _try_parse_memory_mib(normalized)
    if memory_mib is None or memory_mib <= 0:
        raise ValueError(
            "Sandbox activity memory resources must be a positive Kubernetes-style memory quantity. "
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


def _parse_sandbox_provider(sandbox_provider: Optional[str]) -> "pb.SandboxProviderKind":
    if not sandbox_provider:
        return pb.SANDBOX_PROVIDER_KIND_UNSPECIFIED
    if sandbox_provider.lower() == "sandbox":
        return pb.SANDBOX_PROVIDER_KIND_SANDBOX
    if sandbox_provider.lower() == "acasessionpool":
        return pb.SANDBOX_PROVIDER_KIND_ACA_SESSION_POOL
    return pb.SANDBOX_PROVIDER_KIND_UNSPECIFIED
