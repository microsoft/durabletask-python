# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Sandbox preview APIs for Azure Managed Durable Task Scheduler.

This extension provides preview APIs for declaring sandbox activity
worker images and running a Python activity worker inside a Durable Task
Scheduler-launched sandbox.

Usage::

    from durabletask.azuremanaged.preview.sandboxes import (
        SandboxWorker,
        SandboxActivitiesClient,
    )
"""

from durabletask.azuremanaged.preview.sandboxes.client import SandboxActivitiesClient
from durabletask.azuremanaged.preview.sandboxes.declarations import SandboxWorkerProfile
from durabletask.azuremanaged.preview.sandboxes.declarations import SandboxWorkerProfileOptions
from durabletask.azuremanaged.preview.sandboxes.declarations import sandbox_worker_profile
from durabletask.azuremanaged.preview.sandboxes.worker import SandboxWorker

__all__ = [
    "SandboxWorker",
    "SandboxWorkerProfile",
    "SandboxWorkerProfileOptions",
    "SandboxActivitiesClient",
    "sandbox_worker_profile",
]
