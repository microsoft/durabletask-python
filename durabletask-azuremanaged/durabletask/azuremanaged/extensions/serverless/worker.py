# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Compatibility aliases for the on-demand sandbox preview worker APIs."""

from durabletask.azuremanaged.preview.ondemand_sandbox.worker import (
    ManagedIdentityCredential,
    OnDemandSandboxWorker,
    ServerlessWorker,
)

__all__ = [
    "ServerlessWorker",
]
