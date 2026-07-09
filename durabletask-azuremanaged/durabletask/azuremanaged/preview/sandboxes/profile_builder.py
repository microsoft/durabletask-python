# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from decimal import Decimal, InvalidOperation
from typing import Iterable, Optional

from durabletask.azuremanaged.internal import sandbox_service_pb2 as pb
from durabletask.azuremanaged.preview.sandboxes.helpers import (
    SandboxActivity,
    activities_overlap,
    format_activity,
    normalize_required,
    resolve_activities,
)
from durabletask.azuremanaged.preview.sandboxes.worker_profiles import (
    DEFAULT_CPU,
    DEFAULT_MAX_CONCURRENT_ACTIVITIES,
    DEFAULT_MEMORY,
    SandboxWorkerProfileImageOptions,
    registered_sandbox_worker_profiles,
)


MIN_CPU_MILLICORES = 250
MAX_CPU_MILLICORES = 16000
CPU_STEP_MILLICORES = 250
MEMORY_MIB_PER_CORE = 2 * 1024


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
    for profile in registered_sandbox_worker_profiles():
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
