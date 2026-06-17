# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

from durabletask import task
from durabletask.azuremanaged.preview.sandboxes.helpers import (
    SandboxActivity,
    normalize_required,
    resolve_activities,
)


DEFAULT_CPU = "1000m"
DEFAULT_MEMORY = "2048Mi"
DEFAULT_MAX_CONCURRENT_ACTIVITIES = 100


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


def registered_sandbox_worker_profiles() -> Iterable[SandboxWorkerProfileOptions]:
    return _worker_profiles.values()


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
