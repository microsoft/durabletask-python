# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Optional, Sequence

import grpc
from azure.core.credentials import TokenCredential

from durabletask.azuremanaged.preview.sandboxes.helpers import normalize_required
from durabletask.azuremanaged.preview.sandboxes.profile_builder import (
    build_sandbox_worker_profiles,
)
from durabletask.azuremanaged.preview.sandboxes.transport import (
    SandboxActivitiesGrpcTransport,
)
from durabletask.grpc_options import GrpcChannelOptions
import durabletask.internal.shared as shared


class SandboxActivitiesClient:
    """Client for Durable Task Scheduler sandbox activity management operations."""

    def __init__(
            self, *,
            host_address: str,
            taskhub: str,
            token_credential: Optional[TokenCredential],
            channel: Optional[grpc.Channel] = None,
            secure_channel: bool = True,
            interceptors: Optional[Sequence[shared.ClientInterceptor]] = None,
            channel_options: Optional[GrpcChannelOptions] = None):
        self._transport = SandboxActivitiesGrpcTransport(
            host_address=host_address,
            taskhub=taskhub,
            token_credential=token_credential,
            channel=channel,
            secure_channel=secure_channel,
            interceptors=interceptors,
            channel_options=channel_options)

    def close(self) -> None:
        self._transport.close()

    def enable_sandbox_activities(self) -> None:
        """Declare all configured sandbox worker profiles with Durable Task Scheduler."""
        worker_profiles = build_sandbox_worker_profiles()
        if not worker_profiles:
            raise ValueError("No configured sandbox activities were found.")

        for worker_profile in worker_profiles:
            self._transport.declare_sandbox_worker_profile(worker_profile)

    def remove_sandbox_worker_profile(self, worker_profile_id: str) -> None:
        worker_profile_id = normalize_required(worker_profile_id, "Worker profile ID is required.")
        self._transport.remove_sandbox_worker_profile(worker_profile_id)
