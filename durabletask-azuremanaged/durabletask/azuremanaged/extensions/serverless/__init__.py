# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Serverless activities extension for Azure Managed Durable Task Scheduler.

This extension provides preview APIs for declaring serverless activity
worker images and running a Python activity worker inside a DTS-launched
sandbox.

Usage::

    from durabletask.azuremanaged.extensions.serverless import (
        DurableTaskSchedulerServerlessWorker,
        ServerlessActivitiesClient,
    )
"""

from durabletask.azuremanaged.extensions.serverless.client import (
    DEFAULT_CPU,
    DEFAULT_MAX_CONCURRENT_ACTIVITIES,
    DEFAULT_MEMORY,
    DEFAULT_WORKER_PROFILE_ID,
    ServerlessActivitiesClient,
    build_image_ref,
    build_serverless_activity_declaration,
    build_serverless_worker_heartbeat,
    build_serverless_worker_start,
    resolve_activity_names,
)
from durabletask.azuremanaged.extensions.serverless.worker import DurableTaskSchedulerServerlessWorker

__all__ = [
    "DEFAULT_CPU",
    "DEFAULT_MAX_CONCURRENT_ACTIVITIES",
    "DEFAULT_MEMORY",
    "DEFAULT_WORKER_PROFILE_ID",
    "DurableTaskSchedulerServerlessWorker",
    "ServerlessActivitiesClient",
    "build_image_ref",
    "build_serverless_activity_declaration",
    "build_serverless_worker_heartbeat",
    "build_serverless_worker_start",
    "resolve_activity_names",
]
