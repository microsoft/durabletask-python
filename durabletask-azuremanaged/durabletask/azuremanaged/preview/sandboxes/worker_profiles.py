# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Optional

from durabletask import task
from durabletask.azuremanaged.internal import sandbox_service_pb2 as pb
from durabletask.azuremanaged.preview.sandboxes.helpers import (
    SandboxActivity,
    activities_overlap,
    format_activity,
    normalize_required,
    resolve_activities,
)


DEFAULT_CPU = "1000m"
DEFAULT_MEMORY = "2048Mi"
DEFAULT_MAX_CONCURRENT_ACTIVITIES = 100
MIN_CPU_MILLICORES = 250
MAX_CPU_MILLICORES = 16000
CPU_STEP_MILLICORES = 250
MEMORY_MIB_PER_CORE = 2 * 1024


@dataclass
class SandboxWorkerProfileImageOptions:
    """Options for the sandbox worker image DTS should start."""

    # Full OCI image reference for the sandbox worker container, for example
    # "myregistry.azurecr.io/workers/hello:1.0" or
    # "myregistry.azurecr.io/workers/hello@sha256:0123456789abcdef...".
    image_ref: str = ""
    managed_identity_client_id: str = ""
    entrypoint: list[str] = field(default_factory=list[str])
    cmd: list[str] = field(default_factory=list[str])


@dataclass
class SandboxWorkerProfileOptions:
    """Options for a decorated sandbox worker profile."""

    @dataclass(frozen=True)
    class Activity:
        """Activity name and optional version for a sandbox worker profile."""

        name: str
        version: Optional[str]

    worker_profile_id: str
    image: SandboxWorkerProfileImageOptions = field(default_factory=SandboxWorkerProfileImageOptions)
    scheduler_managed_identity_client_id: str = ""
    cpu: str = DEFAULT_CPU
    memory: str = DEFAULT_MEMORY
    environment_variables: dict[str, str] = field(default_factory=dict[str, str])
    max_concurrent_activities: int = DEFAULT_MAX_CONCURRENT_ACTIVITIES
    activities: list[SandboxActivity] = field(default_factory=list[SandboxActivity])

    def add_activity(
            self,
            activity: str | Callable[..., Any],
            version: Optional[str] = None) -> None:
        """Add an activity to the sandbox worker profile worker_profile."""
        activity_name = task.get_name(activity) if callable(activity) else activity
        self.activities.append(SandboxActivity(
            name=normalize_required(activity_name, "Sandbox activity name is required."),
            version=(version.strip() if version and version.strip() else None)))

    def add_activities(self, activities: Iterable[Activity]) -> None:
        """Add activity names and versions to the sandbox worker profile."""
        for activity in activities:
            self.add_activity(activity.name, activity.version)


class SandboxWorkerProfile:
    """Base class for configuring a decorated sandbox worker profile."""

    def configure(self, options: SandboxWorkerProfileOptions) -> None:
        """Configure the sandbox worker profile worker_profile options."""


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

        if not resolve_activities(options.activities):
            raise ValueError(
                f"Sandbox worker profile '{normalized_profile}' must declare at least one activity.")

        _worker_profiles[normalized_profile] = options
        return cls

    return decorator


def _build_sandbox_worker_profile(
        *,
        activities: Iterable[SandboxActivity],
        scheduler_managed_identity_client_id: str = "",
        worker_profile_id: str,
        image: Optional[SandboxWorkerProfileImageOptions] = None,
        cpu: str = DEFAULT_CPU,
        memory: str = DEFAULT_MEMORY,
        environment_variables: Optional[dict[str, str]] = None,
        max_concurrent_activities: int = DEFAULT_MAX_CONCURRENT_ACTIVITIES) -> pb.SandboxWorkerProfile:
    """Build a sandbox activity worker_profile.

    Args:
        image: Sandbox worker image options with the full OCI image reference,
            such as "myregistry.azurecr.io/workers/hello:1.0" or
            "myregistry.azurecr.io/workers/hello@sha256:0123456789abcdef...".
    """
    image_options = image or SandboxWorkerProfileImageOptions()
    resolved_activities = resolve_activities(activities)
    if not resolved_activities:
        raise ValueError("Sandbox activity worker_profile requires at least one activity.")

    if not worker_profile_id or not worker_profile_id.strip():
        raise ValueError("Sandbox activity worker_profile requires a worker profile ID.")

    if max_concurrent_activities <= 0:
        raise ValueError("Sandbox activity max concurrent activities must be greater than zero.")

    image_ref = normalize_required(
        image_options.image_ref,
        "Sandbox activity image metadata requires a container image reference like "
        "'myregistry.azurecr.io/workers/hello:1.0' or "
        "'myregistry.azurecr.io/workers/hello@sha256:...'.")

    resolved_scheduler_managed_identity_client_id = normalize_required(
        scheduler_managed_identity_client_id,
        "Sandbox activity worker_profile requires the managed identity client ID workers use to connect to Durable Task Scheduler.")
    resolved_image_pull_managed_identity_client_id = normalize_required(
        image_options.managed_identity_client_id,
        "Sandbox activity worker_profile requires the managed identity client ID used to pull the worker image.")

    resolved_cpu, cpu_millicores = _normalize_cpu(cpu)
    resolved_memory = _normalize_memory(memory, cpu_millicores)

    worker_profile = pb.SandboxWorkerProfile(
        worker_profile_id=worker_profile_id.strip(),
        image=pb.SandboxActivityImage(
            image_ref=image_ref,
            managed_identity_client_id=resolved_image_pull_managed_identity_client_id),
        resources=pb.SandboxActivityResources(
            cpu=resolved_cpu,
            memory=resolved_memory),
        scheduler_managed_identity_client_id=resolved_scheduler_managed_identity_client_id,
        max_concurrent_activities=max_concurrent_activities)
    worker_profile.activities.extend([
        pb.SandboxActivity(name=activity.name, version=activity.version or "")
        for activity in resolved_activities
    ])
    worker_profile.environment_variables.update(environment_variables or {})
    worker_profile.image.entrypoint.extend(_normalize_optional_strings(image_options.entrypoint))
    worker_profile.image.cmd.extend(_normalize_optional_strings(image_options.cmd))
    return worker_profile


def build_sandbox_worker_profiles() -> list[pb.SandboxWorkerProfile]:
    """Build sandbox worker_profiles from worker profile configuration."""
    worker_profiles: list[pb.SandboxWorkerProfile] = []
    activity_owners: list[tuple[SandboxActivity, str]] = []
    for profile in _worker_profiles.values():
        activities = resolve_activities(profile.activities)

        for activity in activities:
            existing_profile = next((owner_profile for owner_activity, owner_profile in activity_owners
                                     if activities_overlap(owner_activity, activity)
                                     and owner_profile != profile.worker_profile_id), None)
            if existing_profile:
                raise ValueError(
                    f"Sandbox activity '{format_activity(activity)}' is assigned to both worker profile "
                    f"'{existing_profile}' and '{profile.worker_profile_id}'.")
            activity_owners.append((activity, profile.worker_profile_id))

        worker_profiles.append(_build_sandbox_worker_profile(
            activities=activities,
            worker_profile_id=profile.worker_profile_id,
            image=profile.image,
            scheduler_managed_identity_client_id=profile.scheduler_managed_identity_client_id,
            cpu=profile.cpu,
            memory=profile.memory,
            environment_variables=profile.environment_variables,
            max_concurrent_activities=profile.max_concurrent_activities))

    return worker_profiles


def build_sandbox_worker_start(
        *,
        taskhub: str,
        worker_profile_id: str,
        max_activities_count: int,
        activities: Iterable[SandboxActivity],
        sandbox_provider: Optional[str] = None,
        dts_sandbox_identifier: Optional[str] = None) -> pb.SandboxActivityWorkerMessage:
    if not taskhub or not taskhub.strip():
        raise ValueError("Sandbox activity worker registration requires a task hub name.")

    if not worker_profile_id or not worker_profile_id.strip():
        raise ValueError("Sandbox activity worker registration requires a worker profile ID.")

    if max_activities_count <= 0:
        raise ValueError("Sandbox activity worker max activity count must be greater than zero.")

    resolved_activities = resolve_activities(activities)
    if not resolved_activities:
        raise ValueError("Sandbox activity worker registration requires at least one registered activity.")

    message = pb.SandboxActivityWorkerMessage(
        start=pb.SandboxActivityWorkerStart(
            task_hub=taskhub.strip(),
            worker_profile_id=worker_profile_id.strip(),
            max_activities_count=max_activities_count,
            sandbox_provider=_parse_sandbox_provider(sandbox_provider),
            dts_sandbox_identifier=(dts_sandbox_identifier or "").strip()))
    message.start.activities.extend([
        pb.SandboxActivity(name=activity.name, version=activity.version or "")
        for activity in resolved_activities
    ])
    return message


def build_sandbox_worker_heartbeat(active_activities_count: int) -> pb.SandboxActivityWorkerMessage:
    if active_activities_count < 0:
        raise ValueError("Sandbox activity worker active activity count cannot be negative.")

    return pb.SandboxActivityWorkerMessage(
        heartbeat=pb.SandboxActivityWorkerHeartbeat(
            active_activities_count=active_activities_count))


def _normalize_optional_strings(values: Iterable[str]) -> list[str]:
    return [value.strip() for value in values if value and value.strip()]


def _normalize_cpu(value: str) -> tuple[str, int]:
    normalized = normalize_required(value, "Sandbox activity worker_profile requires CPU resources.")
    milli_cpu = _try_parse_cpu_millicores(normalized)
    if (milli_cpu is None
            or milli_cpu < MIN_CPU_MILLICORES
            or milli_cpu > MAX_CPU_MILLICORES
            or milli_cpu % CPU_STEP_MILLICORES != 0):
        raise ValueError(
            "Sandbox activity CPU resources must match an ADC sandbox CPU tier: "
            "250m through 16000m, in 250m increments. "
            "Use formats like '500m', '2', or '0.5'.")
    return normalized, milli_cpu


def _normalize_memory(value: str, cpu_millicores: int) -> str:
    normalized = normalize_required(value, "Sandbox activity worker_profile requires memory resources.")
    max_memory_mib = cpu_millicores * MEMORY_MIB_PER_CORE // 1000
    memory_mib = _try_parse_memory_mib(normalized)
    if memory_mib is None or memory_mib <= 0:
        raise ValueError(
            "Sandbox activity memory resources must be a positive Kubernetes-style memory quantity. "
            "Use formats like '256Mi', '1Gi', or '2048'.")
    if memory_mib > max_memory_mib:
        raise ValueError(
            "Sandbox activity memory resources exceed the ADC sandbox tier maximum for the configured CPU. "
            f"Maximum memory for CPU '{cpu_millicores}m' is {max_memory_mib}Mi.")
    return normalized


def _try_parse_cpu_millicores(value: str) -> Optional[int]:
    try:
        if value[-1:].lower() == "m":
            return int(value[:-1])
        millicores = Decimal(value) * 1000
        return int(millicores) if millicores == millicores.to_integral_value() else None
    except (InvalidOperation, ValueError):
        return None


def _try_parse_memory_mib(value: str) -> Optional[int]:
    try:
        if value[-2:].lower() == "gi":
            return _try_convert_memory_to_mib(Decimal(value[:-2]), 1024)
        if value[-2:].lower() == "mi":
            return _try_convert_memory_to_mib(Decimal(value[:-2]), 1)
        return _try_convert_memory_to_mib(Decimal(value), 1)
    except (InvalidOperation, ValueError):
        return None


def _try_convert_memory_to_mib(value: Decimal, multiplier: int) -> Optional[int]:
    memory_mib = value * multiplier
    return int(memory_mib) if memory_mib == memory_mib.to_integral_value() else None


def _parse_sandbox_provider(sandbox_provider: Optional[str]) -> "pb.SandboxProviderKind":
    if not sandbox_provider:
        return pb.SANDBOX_PROVIDER_KIND_UNSPECIFIED
    if sandbox_provider.lower() == "sandbox":
        return pb.SANDBOX_PROVIDER_KIND_SANDBOX
    if sandbox_provider.lower() == "acasessionpool":
        return pb.SANDBOX_PROVIDER_KIND_ACA_SESSION_POOL
    return pb.SANDBOX_PROVIDER_KIND_UNSPECIFIED
