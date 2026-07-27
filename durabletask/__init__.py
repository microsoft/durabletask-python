# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Durable Task SDK for Python"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from durabletask.grpc_options import GrpcChannelOptions, GrpcRetryPolicyOptions
    from durabletask.payload.store import LargePayloadStorageOptions, PayloadStore
    from durabletask.worker import (
        ActivityWorkItemFilter,
        ConcurrencyOptions,
        EntityWorkItemFilter,
        OrchestrationWorkItemFilter,
        VersioningOptions,
        WorkItemFilters,
    )

__all__ = [
    "ActivityWorkItemFilter",
    "ConcurrencyOptions",
    "EntityWorkItemFilter",
    "GrpcChannelOptions",
    "GrpcRetryPolicyOptions",
    "LargePayloadStorageOptions",
    "OrchestrationWorkItemFilter",
    "PayloadStore",
    "VersioningOptions",
    "WorkItemFilters",
]

PACKAGE_NAME = "durabletask"

# Public names are resolved lazily so that merely importing the ``durabletask``
# package - which also happens implicitly when importing anything from the
# ``durabletask.azuremanaged`` distribution, since both share this namespace -
# does not pull in the worker dependency graph (gRPC, protobuf, entities,
# serialization, OpenTelemetry). Client-only applications would otherwise pay
# that cost at process startup.
_LAZY_EXPORTS: dict[str, str] = {
    "ActivityWorkItemFilter": "durabletask.worker",
    "ConcurrencyOptions": "durabletask.worker",
    "EntityWorkItemFilter": "durabletask.worker",
    "GrpcChannelOptions": "durabletask.grpc_options",
    "GrpcRetryPolicyOptions": "durabletask.grpc_options",
    "LargePayloadStorageOptions": "durabletask.payload.store",
    "OrchestrationWorkItemFilter": "durabletask.worker",
    "PayloadStore": "durabletask.payload.store",
    "VersioningOptions": "durabletask.worker",
    "WorkItemFilters": "durabletask.worker",
}

# Submodules that the previous eager imports bound as attributes of this package
# as a side effect. Attribute access keeps them reachable without an explicit
# ``import durabletask.<name>``, exactly as before, but they are now imported
# only when actually touched. This list mirrors the old surface and must not be
# extended: ``durabletask.client``, for instance, was never bound this way.
_LAZY_SUBMODULES: frozenset[str] = frozenset({
    "entities",
    "grpc_options",
    "internal",
    "payload",
    "serialization",
    "task",
    "worker",
})


def __getattr__(name: str) -> Any:
    """Import and return a lazily exported public name or submodule (PEP 562)."""
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None and name not in _LAZY_SUBMODULES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from importlib import import_module

    if module_name is not None:
        value = getattr(import_module(module_name), name)
    else:
        value = import_module(f".{name}", __name__)

    # Cache on the module so subsequent lookups bypass this hook entirely.
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Include lazily exported names that have not been resolved yet."""
    return sorted(set(globals()) | set(__all__) | _LAZY_SUBMODULES)
