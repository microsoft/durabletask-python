# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Serverless activities extension for Azure Managed Durable Task Scheduler.

This extension provides preview APIs for declaring serverless activity
worker images and running a Python activity worker inside a DTS-launched
sandbox.

Usage::

    from durabletask.azuremanaged.extensions.serverless import (
        ServerlessWorker,
        ServerlessActivitiesClient,
    )
"""

from durabletask.azuremanaged.extensions.serverless.client import ServerlessActivitiesClient
from durabletask.azuremanaged.extensions.serverless.client import ServerlessWorkerProfile
from durabletask.azuremanaged.extensions.serverless.client import ServerlessWorkerProfileOptions
from durabletask.azuremanaged.extensions.serverless.client import serverless_worker_profile
from durabletask.azuremanaged.extensions.serverless.worker import ServerlessWorker

__all__ = [
    "ServerlessWorker",
    "ServerlessWorkerProfile",
    "ServerlessWorkerProfileOptions",
    "ServerlessActivitiesClient",
    "serverless_worker_profile",
]
