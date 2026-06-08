# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Compatibility package for Azure Managed on-demand sandbox preview APIs.

New code should import from ``durabletask.azuremanaged.preview.ondemand_sandbox``.
"""

from durabletask.azuremanaged.preview.ondemand_sandbox.client import ServerlessActivitiesClient
from durabletask.azuremanaged.preview.ondemand_sandbox.client import ServerlessWorkerProfile
from durabletask.azuremanaged.preview.ondemand_sandbox.client import ServerlessWorkerProfileOptions
from durabletask.azuremanaged.preview.ondemand_sandbox.client import serverless_worker_profile
from durabletask.azuremanaged.preview.ondemand_sandbox.worker import ServerlessWorker

__all__ = [
    "ServerlessWorker",
    "ServerlessWorkerProfile",
    "ServerlessWorkerProfileOptions",
    "ServerlessActivitiesClient",
    "serverless_worker_profile",
]
