# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import uuid
from datetime import datetime
from typing import TypeVar

import grpc
import simplejson as json
from google.protobuf import wrappers_pb2

import durabletask.internal.shared as shared
import durabletask.protos.helpers as helpers
import durabletask.protos.orchestrator_service_pb2 as pb
import durabletask.task.registry as registry
from durabletask.api.state import OrchestrationState, new_orchestration_state
from durabletask.protos.orchestrator_service_pb2_grpc import \
    TaskHubSidecarServiceStub
from durabletask.task.orchestration import Orchestrator

TInput = TypeVar('TInput')
TOutput = TypeVar('TOutput')


class TaskHubGrpcClient:

    def __init__(self, *,
                 host_address: str | None = None,
                 log_handler=None,
                 log_formatter: logging.Formatter | None = None):
        channel = shared.get_grpc_channel(host_address)
        self._stub = TaskHubSidecarServiceStub(channel)
        self._logger = shared.get_logger(log_handler, log_formatter)

    def schedule_new_orchestration(self, orchestrator: Orchestrator[TInput, TOutput], *,
                                   input: TInput | None = None,
                                   instance_id: str | None = None,
                                   start_at: datetime | None = None) -> str:

        name = registry.get_name(orchestrator)

        req = pb.CreateInstanceRequest(
            name=name,
            instanceId=instance_id if instance_id else uuid.uuid4().hex,
            input=wrappers_pb2.StringValue(value=json.dumps(input)) if input else None,
            scheduledStartTimestamp=helpers.new_timestamp(start_at) if start_at else None)

        self._logger.info(f"Starting new '{name}' instance with ID = '{instance_id}'.")
        res: pb.CreateInstanceResponse = self._stub.StartInstance(req)
        return res.instanceId

    def get_orchestration_state(self, instance_id: str, *, fetch_payloads: bool = True) -> OrchestrationState | None:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        res: pb.GetInstanceResponse = self._stub.GetInstance(req)
        return new_orchestration_state(req.instanceId, res)

    def wait_for_orchestration_start(self, instance_id: str, *,
                                     fetch_payloads: bool = False,
                                     timeout: int = 60) -> OrchestrationState | None:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        try:
            self._logger.info(f"Waiting {timeout}s for instance '{instance_id}' to start.")
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
                                          timeout: int = 60) -> OrchestrationState | None:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        try:
            self._logger.info(f"Waiting {timeout}s for instance '{instance_id}' to complete.")
            res: pb.GetInstanceResponse = self._stub.WaitForInstanceCompletion(req, timeout=timeout)
            return new_orchestration_state(req.instanceId, res)
        except grpc.RpcError as rpc_error:
            if rpc_error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:  # type: ignore
                # Replace gRPC error with the built-in TimeoutError
                raise TimeoutError("Timed-out waiting for the orchestration to complete")
            else:
                raise

    def terminate_orchestration(self):
        pass

    def suspend_orchestration(self):
        pass

    def resume_orchestration(self):
        pass

    def raise_orchestration_event(self):
        pass
