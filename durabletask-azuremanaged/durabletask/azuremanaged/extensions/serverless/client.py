# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Compatibility aliases for the on-demand sandbox preview client APIs."""

from durabletask.azuremanaged.preview.ondemand_sandbox.client import (
    DEFAULT_CPU,
    DEFAULT_MAX_CONCURRENT_ACTIVITIES,
    DEFAULT_MEMORY,
    DEFAULT_WORKER_PROFILE_ID,
    OnDemandSandboxActivitiesClient,
    OnDemandSandboxWorkerProfile,
    OnDemandSandboxWorkerProfileOptions,
    ServerlessActivitiesClient,
    ServerlessWorkerProfile,
    ServerlessWorkerProfileOptions,
    _worker_profiles,
    build_image_ref,
    build_on_demand_sandbox_activity_declaration,
    build_on_demand_sandbox_worker_heartbeat,
    build_on_demand_sandbox_worker_start,
    build_profile_on_demand_sandbox_activity_declarations,
    build_profile_serverless_activity_declarations,
    build_serverless_activity_declaration,
    build_serverless_worker_heartbeat,
    build_serverless_worker_start,
    on_demand_sandbox_worker_profile,
    resolve_activity_names,
    serverless_worker_profile,
)

__all__ = [
    "DEFAULT_CPU",
    "DEFAULT_MAX_CONCURRENT_ACTIVITIES",
    "DEFAULT_MEMORY",
    "DEFAULT_WORKER_PROFILE_ID",
    "ServerlessActivitiesClient",
    "ServerlessWorkerProfile",
    "ServerlessWorkerProfileOptions",
    "build_image_ref",
    "build_profile_serverless_activity_declarations",
    "build_serverless_activity_declaration",
    "build_serverless_worker_heartbeat",
    "build_serverless_worker_start",
    "resolve_activity_names",
    "serverless_worker_profile",
]
