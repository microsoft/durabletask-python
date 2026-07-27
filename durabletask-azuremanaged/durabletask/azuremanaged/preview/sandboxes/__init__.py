# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Sandbox preview APIs for Durable Task Scheduler.

This extension provides preview APIs for declaring sandbox activity
worker images and running a Python activity worker inside a Durable Task
Scheduler-launched sandbox.

Usage::

    from durabletask.azuremanaged.preview.sandboxes import (
        SandboxWorker,
        SandboxActivitiesClient,
    )

The exports below are resolved lazily on first attribute access, so importing
this package does not load the sandbox worker runtime (and its Azure Identity
and gRPC dependencies) unless those APIs are actually used.
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from durabletask.azuremanaged.preview.sandboxes.client import SandboxActivitiesClient
    from durabletask.azuremanaged.preview.sandboxes.helpers import SandboxActivity
    from durabletask.azuremanaged.preview.sandboxes.worker_profiles import SandboxWorkerProfile
    from durabletask.azuremanaged.preview.sandboxes.worker_profiles import SandboxWorkerProfileOptions
    from durabletask.azuremanaged.preview.sandboxes.worker_profiles import sandbox_worker_profile
    from durabletask.azuremanaged.preview.sandboxes.worker import SandboxWorker

__all__ = [
    "SandboxWorker",
    "SandboxActivity",
    "SandboxWorkerProfile",
    "SandboxWorkerProfileOptions",
    "SandboxActivitiesClient",
    "sandbox_worker_profile",
]

# Public export name -> submodule of this package that defines it.
_LAZY_EXPORTS: dict[str, str] = {
    "SandboxWorker": "worker",
    "SandboxActivity": "helpers",
    "SandboxWorkerProfile": "worker_profiles",
    "SandboxWorkerProfileOptions": "worker_profiles",
    "SandboxActivitiesClient": "client",
    "sandbox_worker_profile": "worker_profiles",
}

# Submodules that eager imports previously bound as attributes of this package.
# They remain reachable through attribute access without an explicit import.
_LAZY_SUBMODULES: frozenset[str] = frozenset({
    "client",
    "helpers",
    "profile_builder",
    "transport",
    "worker",
    "worker_messages",
    "worker_profiles",
})


def __getattr__(name: str) -> Any:
    """Import public sandbox exports on first access (PEP 562)."""
    submodule = _LAZY_EXPORTS.get(name)
    if submodule is not None:
        value = getattr(import_module(f".{submodule}", __name__), name)
    elif name in _LAZY_SUBMODULES:
        value = import_module(f".{name}", __name__)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__) | _LAZY_SUBMODULES)
