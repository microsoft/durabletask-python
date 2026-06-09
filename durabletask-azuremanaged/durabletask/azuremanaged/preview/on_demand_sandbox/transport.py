# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Iterable, Optional, Sequence

import grpc
from azure.core.credentials import TokenCredential

from durabletask.azuremanaged.internal.durabletask_grpc_interceptor import (
    DTSDefaultClientInterceptorImpl,
)
from durabletask.azuremanaged.internal import on_demand_sandbox_activities_service_pb2 as pb
from durabletask.azuremanaged.internal import on_demand_sandbox_activities_service_pb2_grpc as stubs
from durabletask.grpc_options import GrpcChannelOptions
import durabletask.internal.shared as shared


class OnDemandSandboxActivitiesGrpcTransport:
    """Internal gRPC transport for on-demand sandbox activity RPCs."""

    def __init__(
            self, *,
            host_address: str,
            taskhub: str,
            token_credential: Optional[TokenCredential],
            channel: Optional[grpc.Channel] = None,
            secure_channel: bool = True,
            interceptors: Optional[Sequence[shared.ClientInterceptor]] = None,
            channel_options: Optional[GrpcChannelOptions] = None):
        if not taskhub:
            raise ValueError("Taskhub value cannot be empty. Please provide a value for your taskhub")

        self._owns_channel = channel is None
        if channel is None:
            resolved_interceptors: list[shared.ClientInterceptor] = (
                list(interceptors) if interceptors is not None else []
            )
            resolved_interceptors.append(DTSDefaultClientInterceptorImpl(token_credential, taskhub))
            channel = shared.get_grpc_channel(
                host_address=host_address,
                secure_channel=secure_channel,
                interceptors=resolved_interceptors,
                channel_options=channel_options)
        self._channel = channel
        self._stub = stubs.OnDemandSandboxActivitiesStub(channel)

    def close(self) -> None:
        if self._owns_channel:
            self._channel.close()

    def declare_on_demand_sandbox_activities(
            self,
            declaration: pb.OnDemandSandboxActivityDeclaration) -> pb.OnDemandSandboxActivityDeclarationResult:
        return self._stub.DeclareOnDemandSandboxActivities(declaration)

    def remove_on_demand_sandbox_activity_declaration(
            self,
            worker_profile_id: str) -> pb.RemoveOnDemandSandboxActivityDeclarationResult:
        return self._stub.RemoveOnDemandSandboxActivityDeclaration(
            pb.RemoveOnDemandSandboxActivityDeclarationRequest(worker_profile_id=worker_profile_id))

    def connect_on_demand_sandbox_activity_worker(
            self,
            messages: Iterable[pb.OnDemandSandboxActivityWorkerMessage]
    ) -> pb.OnDemandSandboxActivityWorkerSessionResult:
        return self._stub.ConnectOnDemandSandboxActivityWorker(messages)
