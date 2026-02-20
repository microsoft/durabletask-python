# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, List, Optional, Sequence, TypeVar, Union

import grpc
import grpc.aio

from durabletask.entities import EntityInstanceId
from durabletask.entities.entity_metadata import EntityMetadata
import durabletask.internal.helpers as helpers
import durabletask.internal.orchestrator_service_pb2 as pb
import durabletask.internal.orchestrator_service_pb2_grpc as stubs
import durabletask.internal.shared as shared
from durabletask import task
from durabletask.internal.client_helpers import (
    build_query_entities_req,
    build_query_instances_req,
    build_purge_by_filter_req,
    build_raise_event_req,
    build_schedule_new_orchestration_req,
    build_signal_entity_req,
    build_terminate_req,
    check_continuation_token,
    log_completion_state,
    prepare_async_interceptors,
    prepare_sync_interceptors,
)

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
    serialized_input: Optional[str]
    serialized_output: Optional[str]
    serialized_custom_status: Optional[str]
    failure_details: Optional[task.FailureDetails]

    def raise_if_failed(self):
        if self.failure_details is not None:
            raise OrchestrationFailedError(
                f"Orchestration '{self.instance_id}' failed: {self.failure_details.message}",
                self.failure_details)


@dataclass
class OrchestrationQuery:
    created_time_from: Optional[datetime] = None
    created_time_to: Optional[datetime] = None
    runtime_status: Optional[List[OrchestrationStatus]] = None
    # Some backends don't respond well with max_instance_count = None, so we use the integer limit for non-paginated
    # results instead.
    max_instance_count: Optional[int] = (1 << 31) - 1
    fetch_inputs_and_outputs: bool = False


@dataclass
class EntityQuery:
    instance_id_starts_with: Optional[str] = None
    last_modified_from: Optional[datetime] = None
    last_modified_to: Optional[datetime] = None
    include_state: bool = True
    include_transient: bool = False
    page_size: Optional[int] = None


@dataclass
class PurgeInstancesResult:
    deleted_instance_count: int
    is_complete: bool


@dataclass
class CleanEntityStorageResult:
    empty_entities_removed: int
    orphaned_locks_released: int


class OrchestrationFailedError(Exception):
    def __init__(self, message: str, failure_details: task.FailureDetails):
        super().__init__(message)
        self._failure_details = failure_details

    @property
    def failure_details(self):
        return self._failure_details


def new_orchestration_state(instance_id: str, res: pb.GetInstanceResponse) -> Optional[OrchestrationState]:
    if not res.exists:
        return None

    state = res.orchestrationState

    new_state = parse_orchestration_state(state)
    new_state.instance_id = instance_id  # Override instance_id with the one from the request, to match old behavior
    return new_state


def parse_orchestration_state(state: pb.OrchestrationState) -> OrchestrationState:
    failure_details = None
    if state.failureDetails.errorMessage != '' or state.failureDetails.errorType != '':
        failure_details = task.FailureDetails(
            state.failureDetails.errorMessage,
            state.failureDetails.errorType,
            state.failureDetails.stackTrace.value if not helpers.is_empty(state.failureDetails.stackTrace) else None)

    return OrchestrationState(
        state.instanceId,
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
                 host_address: Optional[str] = None,
                 metadata: Optional[list[tuple[str, str]]] = None,
                 log_handler: Optional[logging.Handler] = None,
                 log_formatter: Optional[logging.Formatter] = None,
                 secure_channel: bool = False,
                 interceptors: Optional[Sequence[shared.ClientInterceptor]] = None,
                 default_version: Optional[str] = None):

        interceptors = prepare_sync_interceptors(metadata, interceptors)

        channel = shared.get_grpc_channel(
            host_address=host_address,
            secure_channel=secure_channel,
            interceptors=interceptors
        )
        self._channel = channel
        self._stub = stubs.TaskHubSidecarServiceStub(channel)
        self._logger = shared.get_logger("client", log_handler, log_formatter)
        self.default_version = default_version

    def close(self) -> None:
        """Closes the underlying gRPC channel."""
        self._channel.close()

    def schedule_new_orchestration(self, orchestrator: Union[task.Orchestrator[TInput, TOutput], str], *,
                                   input: Optional[TInput] = None,
                                   instance_id: Optional[str] = None,
                                   start_at: Optional[datetime] = None,
                                   reuse_id_policy: Optional[pb.OrchestrationIdReusePolicy] = None,
                                   tags: Optional[dict[str, str]] = None,
                                   version: Optional[str] = None) -> str:

        req = build_schedule_new_orchestration_req(
            orchestrator, input=input, instance_id=instance_id, start_at=start_at,
            reuse_id_policy=reuse_id_policy, tags=tags,
            version=version if version else self.default_version)

        self._logger.info(f"Starting new '{req.name}' instance with ID = '{req.instanceId}'.")
        res: pb.CreateInstanceResponse = self._stub.StartInstance(req)
        return res.instanceId

    def get_orchestration_state(self, instance_id: str, *, fetch_payloads: bool = True) -> Optional[OrchestrationState]:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        res: pb.GetInstanceResponse = self._stub.GetInstance(req)
        return new_orchestration_state(req.instanceId, res)

    def get_all_orchestration_states(self,
                                     orchestration_query: Optional[OrchestrationQuery] = None
                                     ) -> List[OrchestrationState]:
        if orchestration_query is None:
            orchestration_query = OrchestrationQuery()
        _continuation_token = None

        self._logger.info(f"Querying orchestration instances with query: {orchestration_query}")

        states = []

        while True:
            req = build_query_instances_req(orchestration_query, _continuation_token)
            resp: pb.QueryInstancesResponse = self._stub.QueryInstances(req)
            states += [parse_orchestration_state(res) for res in resp.orchestrationState]
            if check_continuation_token(resp.continuationToken, _continuation_token, self._logger):
                _continuation_token = resp.continuationToken
            else:
                break

        return states

    def wait_for_orchestration_start(self, instance_id: str, *,
                                     fetch_payloads: bool = False,
                                     timeout: int = 60) -> Optional[OrchestrationState]:
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
                                          timeout: int = 60) -> Optional[OrchestrationState]:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        try:
            self._logger.info(f"Waiting {timeout}s for instance '{instance_id}' to complete.")
            res: pb.GetInstanceResponse = self._stub.WaitForInstanceCompletion(req, timeout=timeout)
            state = new_orchestration_state(req.instanceId, res)
            log_completion_state(self._logger, instance_id, state)
            return state
        except grpc.RpcError as rpc_error:
            if rpc_error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:  # type: ignore
                raise TimeoutError("Timed-out waiting for the orchestration to complete")
            else:
                raise

    def raise_orchestration_event(self, instance_id: str, event_name: str, *,
                                  data: Optional[Any] = None) -> None:
        req = build_raise_event_req(instance_id, event_name, data)

        self._logger.info(f"Raising event '{event_name}' for instance '{instance_id}'.")
        self._stub.RaiseEvent(req)

    def terminate_orchestration(self, instance_id: str, *,
                                output: Optional[Any] = None,
                                recursive: bool = True) -> None:
        req = build_terminate_req(instance_id, output, recursive)

        self._logger.info(f"Terminating instance '{instance_id}'.")
        self._stub.TerminateInstance(req)

    def suspend_orchestration(self, instance_id: str) -> None:
        req = pb.SuspendRequest(instanceId=instance_id)
        self._logger.info(f"Suspending instance '{instance_id}'.")
        self._stub.SuspendInstance(req)

    def resume_orchestration(self, instance_id: str) -> None:
        req = pb.ResumeRequest(instanceId=instance_id)
        self._logger.info(f"Resuming instance '{instance_id}'.")
        self._stub.ResumeInstance(req)

    def restart_orchestration(self, instance_id: str, *,
                              restart_with_new_instance_id: bool = False) -> str:
        """Restarts an existing orchestration instance.

        Args:
            instance_id: The ID of the orchestration instance to restart.
            restart_with_new_instance_id: If True, the restarted orchestration will use a new instance ID.
                If False (default), the restarted orchestration will reuse the same instance ID.

        Returns:
            The instance ID of the restarted orchestration.
        """
        req = pb.RestartInstanceRequest(
            instanceId=instance_id,
            restartWithNewInstanceId=restart_with_new_instance_id)

        self._logger.info(f"Restarting instance '{instance_id}'.")
        res: pb.RestartInstanceResponse = self._stub.RestartInstance(req)
        return res.instanceId

    def purge_orchestration(self, instance_id: str, recursive: bool = True) -> PurgeInstancesResult:
        req = pb.PurgeInstancesRequest(instanceId=instance_id, recursive=recursive)
        self._logger.info(f"Purging instance '{instance_id}'.")
        resp: pb.PurgeInstancesResponse = self._stub.PurgeInstances(req)
        return PurgeInstancesResult(resp.deletedInstanceCount, resp.isComplete.value)

    def purge_orchestrations_by(self,
                                created_time_from: Optional[datetime] = None,
                                created_time_to: Optional[datetime] = None,
                                runtime_status: Optional[List[OrchestrationStatus]] = None,
                                recursive: bool = False) -> PurgeInstancesResult:
        self._logger.info("Purging orchestrations by filter: "
                          f"created_time_from={created_time_from}, "
                          f"created_time_to={created_time_to}, "
                          f"runtime_status={[str(status) for status in runtime_status] if runtime_status else None}, "
                          f"recursive={recursive}")
        req = build_purge_by_filter_req(created_time_from, created_time_to, runtime_status, recursive)
        resp: pb.PurgeInstancesResponse = self._stub.PurgeInstances(req)
        return PurgeInstancesResult(resp.deletedInstanceCount, resp.isComplete.value)

    def signal_entity(self,
                      entity_instance_id: EntityInstanceId,
                      operation_name: str,
                      input: Optional[Any] = None) -> None:
        req = build_signal_entity_req(entity_instance_id, operation_name, input)
        self._logger.info(f"Signaling entity '{entity_instance_id}' operation '{operation_name}'.")
        self._stub.SignalEntity(req, None)  # TODO: Cancellation timeout?

    def get_entity(self,
                   entity_instance_id: EntityInstanceId,
                   include_state: bool = True
                   ) -> Optional[EntityMetadata]:
        req = pb.GetEntityRequest(instanceId=str(entity_instance_id), includeState=include_state)
        self._logger.info(f"Getting entity '{entity_instance_id}'.")
        res: pb.GetEntityResponse = self._stub.GetEntity(req)
        if not res.exists:
            return None

        return EntityMetadata.from_entity_metadata(res.entity, include_state)

    def get_all_entities(self,
                         entity_query: Optional[EntityQuery] = None) -> List[EntityMetadata]:
        if entity_query is None:
            entity_query = EntityQuery()
        _continuation_token = None

        self._logger.info(f"Retrieving entities by filter: {entity_query}")

        entities = []

        while True:
            query_request = build_query_entities_req(entity_query, _continuation_token)
            resp: pb.QueryEntitiesResponse = self._stub.QueryEntities(query_request)
            entities += [EntityMetadata.from_entity_metadata(entity, query_request.query.includeState) for entity in resp.entities]
            if check_continuation_token(resp.continuationToken, _continuation_token, self._logger):
                _continuation_token = resp.continuationToken
            else:
                break
        return entities

    def clean_entity_storage(self,
                             remove_empty_entities: bool = True,
                             release_orphaned_locks: bool = True
                             ) -> CleanEntityStorageResult:
        self._logger.info("Cleaning entity storage")

        empty_entities_removed = 0
        orphaned_locks_released = 0
        _continuation_token = None

        while True:
            req = pb.CleanEntityStorageRequest(
                removeEmptyEntities=remove_empty_entities,
                releaseOrphanedLocks=release_orphaned_locks,
                continuationToken=_continuation_token
            )
            resp: pb.CleanEntityStorageResponse = self._stub.CleanEntityStorage(req)
            empty_entities_removed += resp.emptyEntitiesRemoved
            orphaned_locks_released += resp.orphanedLocksReleased

            if check_continuation_token(resp.continuationToken, _continuation_token, self._logger):
                _continuation_token = resp.continuationToken
            else:
                break

        return CleanEntityStorageResult(empty_entities_removed, orphaned_locks_released)


class AsyncTaskHubGrpcClient:
    """Async version of TaskHubGrpcClient using grpc.aio for asyncio-based applications."""

    def __init__(self, *,
                 host_address: Optional[str] = None,
                 metadata: Optional[list[tuple[str, str]]] = None,
                 log_handler: Optional[logging.Handler] = None,
                 log_formatter: Optional[logging.Formatter] = None,
                 secure_channel: bool = False,
                 interceptors: Optional[Sequence[shared.AsyncClientInterceptor]] = None,
                 default_version: Optional[str] = None):

        interceptors = prepare_async_interceptors(metadata, interceptors)

        channel = shared.get_async_grpc_channel(
            host_address=host_address,
            secure_channel=secure_channel,
            interceptors=interceptors
        )
        self._channel = channel
        self._stub = stubs.TaskHubSidecarServiceStub(channel)
        self._logger = shared.get_logger("client", log_handler, log_formatter)
        self.default_version = default_version

    async def close(self) -> None:
        """Closes the underlying gRPC channel."""
        await self._channel.close()

    async def schedule_new_orchestration(self, orchestrator: Union[task.Orchestrator[TInput, TOutput], str], *,
                                         input: Optional[TInput] = None,
                                         instance_id: Optional[str] = None,
                                         start_at: Optional[datetime] = None,
                                         reuse_id_policy: Optional[pb.OrchestrationIdReusePolicy] = None,
                                         tags: Optional[dict[str, str]] = None,
                                         version: Optional[str] = None) -> str:

        req = build_schedule_new_orchestration_req(
            orchestrator, input=input, instance_id=instance_id, start_at=start_at,
            reuse_id_policy=reuse_id_policy, tags=tags,
            version=version if version else self.default_version)

        self._logger.info(f"Starting new '{req.name}' instance with ID = '{req.instanceId}'.")
        res: pb.CreateInstanceResponse = await self._stub.StartInstance(req)
        return res.instanceId

    async def get_orchestration_state(self, instance_id: str, *,
                                      fetch_payloads: bool = True) -> Optional[OrchestrationState]:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        res: pb.GetInstanceResponse = await self._stub.GetInstance(req)
        return new_orchestration_state(req.instanceId, res)

    async def get_all_orchestration_states(self,
                                           orchestration_query: Optional[OrchestrationQuery] = None
                                           ) -> List[OrchestrationState]:
        if orchestration_query is None:
            orchestration_query = OrchestrationQuery()
        _continuation_token = None

        self._logger.info(f"Querying orchestration instances with query: {orchestration_query}")

        states = []

        while True:
            req = build_query_instances_req(orchestration_query, _continuation_token)
            resp: pb.QueryInstancesResponse = await self._stub.QueryInstances(req)
            states += [parse_orchestration_state(res) for res in resp.orchestrationState]
            if check_continuation_token(resp.continuationToken, _continuation_token, self._logger):
                _continuation_token = resp.continuationToken
            else:
                break

        return states

    async def wait_for_orchestration_start(self, instance_id: str, *,
                                           fetch_payloads: bool = False,
                                           timeout: int = 60) -> Optional[OrchestrationState]:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        try:
            self._logger.info(f"Waiting up to {timeout}s for instance '{instance_id}' to start.")
            res: pb.GetInstanceResponse = await self._stub.WaitForInstanceStart(req, timeout=timeout)
            return new_orchestration_state(req.instanceId, res)
        except grpc.aio.AioRpcError as rpc_error:
            if rpc_error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
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
            log_completion_state(self._logger, instance_id, state)
            return state
        except grpc.aio.AioRpcError as rpc_error:
            if rpc_error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise TimeoutError("Timed-out waiting for the orchestration to complete")
            else:
                raise

    async def raise_orchestration_event(self, instance_id: str, event_name: str, *,
                                        data: Optional[Any] = None) -> None:
        req = build_raise_event_req(instance_id, event_name, data)

        self._logger.info(f"Raising event '{event_name}' for instance '{instance_id}'.")
        await self._stub.RaiseEvent(req)

    async def terminate_orchestration(self, instance_id: str, *,
                                      output: Optional[Any] = None,
                                      recursive: bool = True) -> None:
        req = build_terminate_req(instance_id, output, recursive)

        self._logger.info(f"Terminating instance '{instance_id}'.")
        await self._stub.TerminateInstance(req)

    async def suspend_orchestration(self, instance_id: str) -> None:
        req = pb.SuspendRequest(instanceId=instance_id)
        self._logger.info(f"Suspending instance '{instance_id}'.")
        await self._stub.SuspendInstance(req)

    async def resume_orchestration(self, instance_id: str) -> None:
        req = pb.ResumeRequest(instanceId=instance_id)
        self._logger.info(f"Resuming instance '{instance_id}'.")
        await self._stub.ResumeInstance(req)

    async def purge_orchestration(self, instance_id: str, recursive: bool = True) -> PurgeInstancesResult:
        req = pb.PurgeInstancesRequest(instanceId=instance_id, recursive=recursive)
        self._logger.info(f"Purging instance '{instance_id}'.")
        resp: pb.PurgeInstancesResponse = await self._stub.PurgeInstances(req)
        return PurgeInstancesResult(resp.deletedInstanceCount, resp.isComplete.value)

    async def purge_orchestrations_by(self,
                                      created_time_from: Optional[datetime] = None,
                                      created_time_to: Optional[datetime] = None,
                                      runtime_status: Optional[List[OrchestrationStatus]] = None,
                                      recursive: bool = False) -> PurgeInstancesResult:
        self._logger.info("Purging orchestrations by filter: "
                          f"created_time_from={created_time_from}, "
                          f"created_time_to={created_time_to}, "
                          f"runtime_status={[str(status) for status in runtime_status] if runtime_status else None}, "
                          f"recursive={recursive}")
        req = build_purge_by_filter_req(created_time_from, created_time_to, runtime_status, recursive)
        resp: pb.PurgeInstancesResponse = await self._stub.PurgeInstances(req)
        return PurgeInstancesResult(resp.deletedInstanceCount, resp.isComplete.value)

    async def signal_entity(self,
                            entity_instance_id: EntityInstanceId,
                            operation_name: str,
                            input: Optional[Any] = None) -> None:
        req = build_signal_entity_req(entity_instance_id, operation_name, input)
        self._logger.info(f"Signaling entity '{entity_instance_id}' operation '{operation_name}'.")
        await self._stub.SignalEntity(req, None)

    async def get_entity(self,
                         entity_instance_id: EntityInstanceId,
                         include_state: bool = True
                         ) -> Optional[EntityMetadata]:
        req = pb.GetEntityRequest(instanceId=str(entity_instance_id), includeState=include_state)
        self._logger.info(f"Getting entity '{entity_instance_id}'.")
        res: pb.GetEntityResponse = await self._stub.GetEntity(req)
        if not res.exists:
            return None

        return EntityMetadata.from_entity_metadata(res.entity, include_state)

    async def get_all_entities(self,
                               entity_query: Optional[EntityQuery] = None) -> List[EntityMetadata]:
        if entity_query is None:
            entity_query = EntityQuery()
        _continuation_token = None

        self._logger.info(f"Retrieving entities by filter: {entity_query}")

        entities = []

        while True:
            query_request = build_query_entities_req(entity_query, _continuation_token)
            resp: pb.QueryEntitiesResponse = await self._stub.QueryEntities(query_request)
            entities += [EntityMetadata.from_entity_metadata(entity, query_request.query.includeState) for entity in resp.entities]
            if check_continuation_token(resp.continuationToken, _continuation_token, self._logger):
                _continuation_token = resp.continuationToken
            else:
                break
        return entities

    async def clean_entity_storage(self,
                                   remove_empty_entities: bool = True,
                                   release_orphaned_locks: bool = True
                                   ) -> CleanEntityStorageResult:
        self._logger.info("Cleaning entity storage")

        empty_entities_removed = 0
        orphaned_locks_released = 0
        _continuation_token = None

        while True:
            req = pb.CleanEntityStorageRequest(
                removeEmptyEntities=remove_empty_entities,
                releaseOrphanedLocks=release_orphaned_locks,
                continuationToken=_continuation_token
            )
            resp: pb.CleanEntityStorageResponse = await self._stub.CleanEntityStorage(req)
            empty_entities_removed += resp.emptyEntitiesRemoved
            orphaned_locks_released += resp.orphanedLocksReleased

            if check_continuation_token(resp.continuationToken, _continuation_token, self._logger):
                _continuation_token = resp.continuationToken
            else:
                break

        return CleanEntityStorageResult(empty_entities_removed, orphaned_locks_released)
