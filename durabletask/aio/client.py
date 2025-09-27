import logging
import uuid
from datetime import datetime
from typing import Any, Optional, Sequence, Union

import grpc
from google.protobuf import wrappers_pb2

import durabletask.internal.helpers as helpers
import durabletask.internal.orchestrator_service_pb2 as pb
import durabletask.internal.orchestrator_service_pb2_grpc as stubs
import durabletask.internal.shared as shared
from durabletask.aio.internal.shared import get_grpc_aio_channel, AioClientInterceptor
from durabletask import task
from durabletask.client import OrchestrationState, OrchestrationStatus, new_orchestration_state, TInput, TOutput
from durabletask.aio.internal.grpc_interceptor import DefaultAioClientInterceptorImpl


class AsyncTaskHubGrpcClient:

    def __init__(self, *,
                 host_address: Optional[str] = None,
                 metadata: Optional[list[tuple[str, str]]] = None,
                 log_handler: Optional[logging.Handler] = None,
                 log_formatter: Optional[logging.Formatter] = None,
                 secure_channel: bool = False,
                 interceptors: Optional[Sequence[AioClientInterceptor]] = None,
                 default_version: Optional[str] = None):

        if interceptors is not None:
            interceptors = list(interceptors)
            if metadata is not None:
                interceptors.append(DefaultAioClientInterceptorImpl(metadata))
        elif metadata is not None:
            interceptors = [DefaultAioClientInterceptorImpl(metadata)]
        else:
            interceptors = None

        channel = get_grpc_aio_channel(
            host_address=host_address,
            secure_channel=secure_channel,
            interceptors=interceptors
        )
        self._channel = channel
        self._stub = stubs.TaskHubSidecarServiceStub(channel)
        self._logger = shared.get_logger("client", log_handler, log_formatter)
        self.default_version = default_version

    async def aclose(self):
        await self._channel.close()

    async def schedule_new_orchestration(self, orchestrator: Union[task.Orchestrator[TInput, TOutput], str], *,
                                         input: Optional[TInput] = None,
                                         instance_id: Optional[str] = None,
                                         start_at: Optional[datetime] = None,
                                         reuse_id_policy: Optional[pb.OrchestrationIdReusePolicy] = None,
                                         tags: Optional[dict[str, str]] = None,
                                         version: Optional[str] = None) -> str:

        name = orchestrator if isinstance(orchestrator, str) else task.get_name(orchestrator)

        req = pb.CreateInstanceRequest(
            name=name,
            instanceId=instance_id if instance_id else uuid.uuid4().hex,
            input=wrappers_pb2.StringValue(value=shared.to_json(input)) if input is not None else None,
            scheduledStartTimestamp=helpers.new_timestamp(start_at) if start_at else None,
            version=helpers.get_string_value(version if version else self.default_version),
            orchestrationIdReusePolicy=reuse_id_policy,
            tags=tags
        )

        self._logger.info(f"Starting new '{name}' instance with ID = '{req.instanceId}'.")
        res: pb.CreateInstanceResponse = await self._stub.StartInstance(req)
        return res.instanceId

    async def get_orchestration_state(self, instance_id: str, *, fetch_payloads: bool = True) -> Optional[OrchestrationState]:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        res: pb.GetInstanceResponse = await self._stub.GetInstance(req)
        return new_orchestration_state(req.instanceId, res)

    async def wait_for_orchestration_start(self, instance_id: str, *,
                                           fetch_payloads: bool = False,
                                           timeout: int = 60) -> Optional[OrchestrationState]:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        try:
            self._logger.info(f"Waiting up to {timeout}s for instance '{instance_id}' to start.")
            res: pb.GetInstanceResponse = await self._stub.WaitForInstanceStart(req, timeout=timeout)
            return new_orchestration_state(req.instanceId, res)
        except grpc.RpcError as rpc_error:
            if rpc_error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:  # type: ignore
                raise TimeoutError("Timed-out waiting for the orchestration to start")
            else:
                raise

    async def wait_for_orchestration_completion(self, instance_id: str, *,
                                                fetch_payloads: bool = True,
                                                timeout: int = 60) -> Optional[OrchestrationState]:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        try:
            self._logger.info(f"Waiting {timeout}s for instance '{instance_id}' to complete.")
            res: pb.GetInstanceResponse = await self._stub.WaitForInstanceCompletion(req, timeout=timeout)
            state = new_orchestration_state(req.instanceId, res)
            if not state:
                return None

            if state.runtime_status == OrchestrationStatus.FAILED and state.failure_details is not None:
                details = state.failure_details
                self._logger.info(f"Instance '{instance_id}' failed: [{details.error_type}] {details.message}")
            elif state.runtime_status == OrchestrationStatus.TERMINATED:
                self._logger.info(f"Instance '{instance_id}' was terminated.")
            elif state.runtime_status == OrchestrationStatus.COMPLETED:
                self._logger.info(f"Instance '{instance_id}' completed.")

            return state
        except grpc.RpcError as rpc_error:
            if rpc_error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:  # type: ignore
                raise TimeoutError("Timed-out waiting for the orchestration to complete")
            else:
                raise

    async def raise_orchestration_event(
            self,
            instance_id: str,
            event_name: str,
            *,
            data: Optional[Any] = None):
        req = pb.RaiseEventRequest(
            instanceId=instance_id,
            name=event_name,
            input=wrappers_pb2.StringValue(value=shared.to_json(data)) if data else None)

        self._logger.info(f"Raising event '{event_name}' for instance '{instance_id}'.")
        await self._stub.RaiseEvent(req)

    async def terminate_orchestration(self, instance_id: str, *,
                                      output: Optional[Any] = None,
                                      recursive: bool = True):
        req = pb.TerminateRequest(
            instanceId=instance_id,
            output=wrappers_pb2.StringValue(value=shared.to_json(output)) if output else None,
            recursive=recursive)

        self._logger.info(f"Terminating instance '{instance_id}'.")
        await self._stub.TerminateInstance(req)

    async def suspend_orchestration(self, instance_id: str):
        req = pb.SuspendRequest(instanceId=instance_id)
        self._logger.info(f"Suspending instance '{instance_id}'.")
        await self._stub.SuspendInstance(req)

    async def resume_orchestration(self, instance_id: str):
        req = pb.ResumeRequest(instanceId=instance_id)
        self._logger.info(f"Resuming instance '{instance_id}'.")
        await self._stub.ResumeInstance(req)

    async def purge_orchestration(self, instance_id: str, recursive: bool = True):
        req = pb.PurgeInstancesRequest(instanceId=instance_id, recursive=recursive)
        self._logger.info(f"Purging instance '{instance_id}'.")
        await self._stub.PurgeInstances(req)
