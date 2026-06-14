# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Iterable, Optional, Protocol, Sequence, cast

import grpc
from azure.core.credentials import TokenCredential

from durabletask.azuremanaged.internal.durabletask_grpc_interceptor import (
    DTSDefaultClientInterceptorImpl,
)
from durabletask.azuremanaged.internal import sandbox_service_pb2 as pb
from durabletask.azuremanaged.internal import sandbox_service_pb2_grpc as stubs
from durabletask.grpc_options import GrpcChannelOptions
import durabletask.internal.shared as shared


class _SandboxActivitiesStub(Protocol):
    def DeclareSandboxWorkerProfile(
            self,
            request: pb.SandboxWorkerProfile) -> pb.DeclareSandboxWorkerProfileResult:
        raise NotImplementedError

    def RemoveSandboxWorkerProfile(
            self,
            request: pb.RemoveSandboxWorkerProfileRequest) -> pb.RemoveSandboxWorkerProfileResult:
        raise NotImplementedError

    def ConnectSandboxActivityWorker(
            self,
            request_iterator: Iterable[pb.SandboxActivityWorkerMessage]) -> pb.SandboxActivityWorkerSessionResult:
        raise NotImplementedError


class SandboxActivitiesGrpcTransport:
    """Internal gRPC transport for sandbox activity RPCs."""

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
        self._stub = cast(_SandboxActivitiesStub, stubs.SandboxActivitiesStub(channel))

    def close(self) -> None:
        if self._owns_channel:
            self._channel.close()

    def declare_sandbox_worker_profile(
            self,
            worker_profile: pb.SandboxWorkerProfile) -> pb.DeclareSandboxWorkerProfileResult:
        return self._stub.DeclareSandboxWorkerProfile(worker_profile)

    def remove_sandbox_worker_profile(
            self,
            worker_profile_id: str) -> pb.RemoveSandboxWorkerProfileResult:
        return self._stub.RemoveSandboxWorkerProfile(
            pb.RemoveSandboxWorkerProfileRequest(worker_profile_id=worker_profile_id))

    def connect_sandbox_activity_worker(
            self,
            messages: Iterable[pb.SandboxActivityWorkerMessage]
    ) -> pb.SandboxActivityWorkerSessionResult:
        return self._stub.ConnectSandboxActivityWorker(messages)
