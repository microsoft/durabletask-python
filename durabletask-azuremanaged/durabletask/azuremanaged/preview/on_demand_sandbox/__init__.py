# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""On-demand sandbox preview APIs for Azure Managed Durable Task Scheduler.

This extension provides preview APIs for declaring on-demand sandbox activity
worker images and running a Python activity worker inside a Durable Task
Scheduler-launched sandbox.

Usage::

    from durabletask.azuremanaged.preview.on_demand_sandbox import (
        OnDemandSandboxWorker,
        OnDemandSandboxActivitiesClient,
    )
"""

from durabletask.azuremanaged.preview.on_demand_sandbox.client import OnDemandSandboxActivitiesClient
from durabletask.azuremanaged.preview.on_demand_sandbox.client import OnDemandSandboxWorkerProfile
from durabletask.azuremanaged.preview.on_demand_sandbox.client import OnDemandSandboxWorkerProfileOptions
from durabletask.azuremanaged.preview.on_demand_sandbox.client import on_demand_sandbox_worker_profile
from durabletask.azuremanaged.preview.on_demand_sandbox.worker import OnDemandSandboxWorker

__all__ = [
    "OnDemandSandboxWorker",
    "OnDemandSandboxWorkerProfile",
    "OnDemandSandboxWorkerProfileOptions",
    "OnDemandSandboxActivitiesClient",
    "on_demand_sandbox_worker_profile",
]
