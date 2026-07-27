# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from decimal import Decimal, InvalidOperation
from typing import Iterable, Optional

from durabletask.azuremanaged.internal import sandbox_service_pb2 as pb
from durabletask.azuremanaged.preview.sandboxes.helpers import (
    SandboxActivity,
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


class _ActivityOwnerSlot:
    """Earliest worker profiles recorded for a single activity overlap bucket.

    Only two owners ever need to be retained to answer "which worker profile
    first claimed an activity in this bucket, ignoring `worker_profile_id`":
    the very first owner, plus the first owner that differs from it.
    """

    __slots__ = ("_first", "_first_other")

    def __init__(self) -> None:
        self._first: Optional[tuple[int, str]] = None
        self._first_other: Optional[tuple[int, str]] = None

    def add(self, order: int, worker_profile_id: str) -> None:
        if self._first is None:
            self._first = (order, worker_profile_id)
        elif self._first_other is None and worker_profile_id != self._first[1]:
            self._first_other = (order, worker_profile_id)

    def first_owner_other_than(self, worker_profile_id: str) -> Optional[tuple[int, str]]:
        if self._first is not None and self._first[1] != worker_profile_id:
            return self._first
        return self._first_other


class _ActivityOwnerIndex:
    """Indexes activity ownership so overlap checks cost O(1) per activity.

    Reproduces the semantics of
    :func:`durabletask.azuremanaged.preview.sandboxes.helpers.activities_overlap`
    exactly: activity names are compared case insensitively, an unversioned
    activity overlaps every version of the same name, and two activities with
    the same name overlap when their explicit versions are equal. Registration
    order is tracked so the reported conflict matches the first overlapping
    activity, exactly as a linear scan would report it.
    """

    def __init__(self) -> None:
        self._registration_count = 0
        self._by_name: dict[str, _ActivityOwnerSlot] = {}
        self._by_name_and_version: dict[tuple[str, Optional[str]], _ActivityOwnerSlot] = {}

    def find_conflicting_profile(
            self,
            activity: SandboxActivity,
            worker_profile_id: str) -> Optional[str]:
        """Return the profile that first claimed an overlapping activity, if any."""
        name_key = activity.name.casefold()
        if activity.version is None:
            # An unversioned activity overlaps every version of the same name.
            owner = _first_owner_other_than(self._by_name.get(name_key), worker_profile_id)
        else:
            # A versioned activity overlaps the same version plus any
            # unversioned registration of the same name.
            owner = _earlier_owner(
                _first_owner_other_than(
                    self._by_name_and_version.get((name_key, None)), worker_profile_id),
                _first_owner_other_than(
                    self._by_name_and_version.get((name_key, activity.version)), worker_profile_id))
        return None if owner is None else owner[1]

    def add(self, activity: SandboxActivity, worker_profile_id: str) -> None:
        """Record `worker_profile_id` as an owner of `activity`."""
        name_key = activity.name.casefold()
        order = self._registration_count
        self._registration_count += 1
        self._by_name.setdefault(name_key, _ActivityOwnerSlot()).add(order, worker_profile_id)
        self._by_name_and_version.setdefault(
            (name_key, activity.version), _ActivityOwnerSlot()).add(order, worker_profile_id)


def _first_owner_other_than(
        slot: Optional[_ActivityOwnerSlot],
        worker_profile_id: str) -> Optional[tuple[int, str]]:
    return None if slot is None else slot.first_owner_other_than(worker_profile_id)


def _earlier_owner(
        left: Optional[tuple[int, str]],
        right: Optional[tuple[int, str]]) -> Optional[tuple[int, str]]:
    if left is None:
        return right
    if right is None:
        return left
    return left if left[0] <= right[0] else right


def build_sandbox_worker_profiles() -> list[pb.SandboxWorkerProfile]:
    """Build sandbox worker_profiles from worker profile configuration."""
    worker_profiles: list[pb.SandboxWorkerProfile] = []
    activity_owners = _ActivityOwnerIndex()
    for profile in registered_sandbox_worker_profiles():
        activities = resolve_activities(profile.activities)

        for activity in activities:
            existing_profile = activity_owners.find_conflicting_profile(
                activity, profile.worker_profile_id)
            if existing_profile:
                raise ValueError(
                    f"Sandbox activity '{format_activity(activity)}' is assigned to both worker profile "
                    f"'{existing_profile}' and '{profile.worker_profile_id}'.")
            activity_owners.add(activity, profile.worker_profile_id)

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
