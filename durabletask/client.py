# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, List, Tuple, TypeVar, Union

import grpc
from google.protobuf import wrappers_pb2

import durabletask.internal.helpers as helpers
import durabletask.internal.orchestrator_service_pb2 as pb
import durabletask.internal.orchestrator_service_pb2_grpc as stubs
import durabletask.internal.shared as shared
from durabletask import task

TInput = TypeVar('TInput')
TOutput = TypeVar('TOutput')


class OrchestrationStatus(Enum):
    """The status of an orchestration instance."""
    RUNNING = pb.ORCHESTRATION_STATUS_RUNNING
    COMPLETED = pb.ORCHESTRATION_STATUS_COMPLETED
    FAILED = pb.ORCHESTRATION_STATUS_FAILED
    TERMINATED = pb.ORCHESTRATION_STATUS_TERMINATED
    CONTINUED_AS_NEW = pb.ORCHESTRATION_STATUS_CONTINUED_AS_NEW
    PENDING = pb.ORCHESTRATION_STATUS_PENDING
    SUSPENDED = pb.ORCHESTRATION_STATUS_SUSPENDED

    def __str__(self):
        return helpers.get_orchestration_status_str(self.value)


@dataclass
class OrchestrationState:
    instance_id: str
    name: str
    runtime_status: OrchestrationStatus
    created_at: datetime
    last_updated_at: datetime
    serialized_input: Union[str, None]
    serialized_output: Union[str, None]
    serialized_custom_status: Union[str, None]
    failure_details: Union[task.FailureDetails, None]

    def raise_if_failed(self):
        if self.failure_details is not None:
            raise OrchestrationFailedError(
                f"Orchestration '{self.instance_id}' failed: {self.failure_details.message}",
                self.failure_details)


class OrchestrationFailedError(Exception):
    def __init__(self, message: str, failure_details: task.FailureDetails):
        super().__init__(message)
        self._failure_details = failure_details

    @property
    def failure_details(self):
        return self._failure_details


def new_orchestration_state(instance_id: str, res: pb.GetInstanceResponse) -> Union[OrchestrationState, None]:
    if not res.exists:
        return None

    state = res.orchestrationState

    failure_details = None
    if state.failureDetails.errorMessage != '' or state.failureDetails.errorType != '':
        failure_details = task.FailureDetails(
            state.failureDetails.errorMessage,
            state.failureDetails.errorType,
            state.failureDetails.stackTrace.value if not helpers.is_empty(state.failureDetails.stackTrace) else None)

    return OrchestrationState(
        instance_id,
        state.name,
        OrchestrationStatus(state.orchestrationStatus),
        state.createdTimestamp.ToDatetime(),
        state.lastUpdatedTimestamp.ToDatetime(),
        state.input.value if not helpers.is_empty(state.input) else None,
        state.output.value if not helpers.is_empty(state.output) else None,
        state.customStatus.value if not helpers.is_empty(state.customStatus) else None,
        failure_details)


class TaskHubGrpcClient:

    def __init__(self, *,
                 host_address: Union[str, None] = None,
                 metadata: Union[List[Tuple[str, str]], None] = None,
                 log_handler = None,
                 log_formatter: Union[logging.Formatter, None] = None,
                 secure_channel: bool = False):
        channel = shared.get_grpc_channel(host_address, metadata, secure_channel=secure_channel)
        self._stub = stubs.TaskHubSidecarServiceStub(channel)
        self._logger = shared.get_logger("client", log_handler, log_formatter)

    def schedule_new_orchestration(self, orchestrator: Union[task.Orchestrator[TInput, TOutput], str], *,
                                   input: Union[TInput, None] = None,
                                   instance_id: Union[str, None] = None,
                                   start_at: Union[datetime, None] = None,
                                   reuse_id_policy: Union[pb.OrchestrationIdReusePolicy, None] = None) -> str:

        name = orchestrator if isinstance(orchestrator, str) else task.get_name(orchestrator)

        req = pb.CreateInstanceRequest(
            name=name,
            instanceId=instance_id if instance_id else uuid.uuid4().hex,
            input=wrappers_pb2.StringValue(value=shared.to_json(input)) if input is not None else None,
            scheduledStartTimestamp=helpers.new_timestamp(start_at) if start_at else None,
            version=wrappers_pb2.StringValue(value=""),
            orchestrationIdReusePolicy=reuse_id_policy,
            )

        self._logger.info(f"Starting new '{name}' instance with ID = '{req.instanceId}'.")
        res: pb.CreateInstanceResponse = self._stub.StartInstance(req)
        return res.instanceId

    def get_orchestration_state(self, instance_id: str, *, fetch_payloads: bool = True) -> Union[OrchestrationState, None]:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        res: pb.GetInstanceResponse = self._stub.GetInstance(req)
        return new_orchestration_state(req.instanceId, res)

    def wait_for_orchestration_start(self, instance_id: str, *,
                                     fetch_payloads: bool = False,
                                     timeout: int = 60) -> Union[OrchestrationState, None]:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        try:
            self._logger.info(f"Waiting up to {timeout}s for instance '{instance_id}' to start.")
            res: pb.GetInstanceResponse = self._stub.WaitForInstanceStart(req, timeout=timeout)
            return new_orchestration_state(req.instanceId, res)
        except grpc.RpcError as rpc_error:
            if rpc_error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:  # type: ignore
                # Replace gRPC error with the built-in TimeoutError
                raise TimeoutError("Timed-out waiting for the orchestration to start")
            else:
                raise

    def wait_for_orchestration_completion(self, instance_id: str, *,
                                          fetch_payloads: bool = True,
                                          timeout: int = 60) -> Union[OrchestrationState, None]:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        try:
            self._logger.info(f"Waiting {timeout}s for instance '{instance_id}' to complete.")
            res: pb.GetInstanceResponse = self._stub.WaitForInstanceCompletion(req, timeout=timeout)
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
                # Replace gRPC error with the built-in TimeoutError
                raise TimeoutError("Timed-out waiting for the orchestration to complete")
            else:
                raise

    def raise_orchestration_event(self, instance_id: str, event_name: str, *,
                                  data: Union[Any, None] = None):
        req = pb.RaiseEventRequest(
            instanceId=instance_id,
            name=event_name,
            input=wrappers_pb2.StringValue(value=shared.to_json(data)) if data else None)

        self._logger.info(f"Raising event '{event_name}' for instance '{instance_id}'.")
        self._stub.RaiseEvent(req)

    def terminate_orchestration(self, instance_id: str, *,
                                output: Union[Any, None] = None,
                                recursive: bool = True):
        req = pb.TerminateRequest(
            instanceId=instance_id,
            output=wrappers_pb2.StringValue(value=shared.to_json(output)) if output else None,
            recursive=recursive)

        self._logger.info(f"Terminating instance '{instance_id}'.")
        self._stub.TerminateInstance(req)

    def suspend_orchestration(self, instance_id: str):
        req = pb.SuspendRequest(instanceId=instance_id)
        self._logger.info(f"Suspending instance '{instance_id}'.")
        self._stub.SuspendInstance(req)

    def resume_orchestration(self, instance_id: str):
        req = pb.ResumeRequest(instanceId=instance_id)
        self._logger.info(f"Resuming instance '{instance_id}'.")
        self._stub.ResumeInstance(req)
