# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Sequence, TypeVar, Union

import grpc
from google.protobuf import wrappers_pb2

import durabletask.internal.helpers as helpers
import durabletask.internal.orchestrator_service_pb2 as pb
import durabletask.internal.orchestrator_service_pb2_grpc as stubs
import durabletask.internal.shared as shared
from durabletask import task
from durabletask.internal.grpc_interceptor import DefaultClientInterceptorImpl

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
                 host_address: Optional[str] = None,
                 metadata: Optional[list[tuple[str, str]]] = None,
                 log_handler: Optional[logging.Handler] = None,
                 log_formatter: Optional[logging.Formatter] = None,
                 secure_channel: bool = False,
                 interceptors: Optional[Sequence[shared.ClientInterceptor]] = None):

        # If the caller provided metadata, we need to create a new interceptor for it and
        # add it to the list of interceptors.
        if interceptors is not None:
            interceptors = list(interceptors)
            if metadata is not None:
                interceptors.append(DefaultClientInterceptorImpl(metadata))
        elif metadata is not None:
            interceptors = [DefaultClientInterceptorImpl(metadata)]
        else:
            interceptors = None

        channel = shared.get_grpc_channel(
            host_address=host_address,
            secure_channel=secure_channel,
            interceptors=interceptors
        )
        self._stub = stubs.TaskHubSidecarServiceStub(channel)
        self._logger = shared.get_logger("client", log_handler, log_formatter)

    def schedule_new_orchestration(self, orchestrator: Union[task.Orchestrator[TInput, TOutput], str], *,
                                   input: Optional[TInput] = None,
                                   instance_id: Optional[str] = None,
                                   start_at: Optional[datetime] = None,
                                   reuse_id_policy: Optional[pb.OrchestrationIdReusePolicy] = None) -> str:

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

    def get_orchestration_state(self, instance_id: str, *, fetch_payloads: bool = True) -> Optional[OrchestrationState]:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        res: pb.GetInstanceResponse = self._stub.GetInstance(req)
        return new_orchestration_state(req.instanceId, res)

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
                                  data: Optional[Any] = None):
        req = pb.RaiseEventRequest(
            instanceId=instance_id,
            name=event_name,
            input=wrappers_pb2.StringValue(value=shared.to_json(data)) if data else None)

        self._logger.info(f"Raising event '{event_name}' for instance '{instance_id}'.")
        self._stub.RaiseEvent(req)

    def terminate_orchestration(self, instance_id: str, *,
                                output: Optional[Any] = None,
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

    def purge_orchestration(self, instance_id: str, recursive: bool = True):
        req = pb.PurgeInstancesRequest(instanceId=instance_id, recursive=recursive)
        self._logger.info(f"Purging instance '{instance_id}'.")
        self._stub.PurgeInstances(req)

    def signal_entity(self, entity_id: str, operation_name: str, *,
                      input: Optional[Any] = None,
                      request_id: Optional[str] = None,
                      scheduled_time: Optional[datetime] = None):
        """Signal an entity with an operation.

        Parameters
        ----------
        entity_id : str
            The ID of the entity to signal.
        operation_name : str
            The name of the operation to perform.
        input : Optional[Any]
            The JSON-serializable input to pass to the entity operation.
        request_id : Optional[str]
            A unique request ID for the operation. If not provided, a random UUID will be used.
        scheduled_time : Optional[datetime]
            The time to schedule the operation. If not provided, the operation is scheduled immediately.
        """
        req = pb.SignalEntityRequest(
            instanceId=entity_id,
            name=operation_name,
            input=wrappers_pb2.StringValue(value=shared.to_json(input)) if input is not None else None,
            requestId=request_id if request_id else uuid.uuid4().hex,
            scheduledTime=helpers.new_timestamp(scheduled_time) if scheduled_time else None)

        self._logger.info(f"Signaling entity '{entity_id}' with operation '{operation_name}'.")
        self._stub.SignalEntity(req)

    def get_entity(self, entity_id: str, *, include_state: bool = True) -> Optional[task.EntityState]:
        """Get the state of an entity.

        Parameters
        ----------
        entity_id : str
            The ID of the entity to query.
        include_state : bool
            Whether to include the entity's state in the response.

        Returns
        -------
        Optional[EntityState]
            The entity state if it exists, None otherwise.
        """
        req = pb.GetEntityRequest(instanceId=entity_id, includeState=include_state)
        res: pb.GetEntityResponse = self._stub.GetEntity(req)
        
        if not res.exists:
            return None

        entity_metadata = res.entity
        return task.EntityState(
            instance_id=entity_metadata.instanceId,
            last_modified_time=entity_metadata.lastModifiedTime.ToDatetime(),
            backlog_queue_size=entity_metadata.backlogQueueSize,
            locked_by=entity_metadata.lockedBy.value if not helpers.is_empty(entity_metadata.lockedBy) else None,
            serialized_state=entity_metadata.serializedState.value if not helpers.is_empty(entity_metadata.serializedState) else None)

    def query_entities(self, query: task.EntityQuery) -> task.EntityQueryResult:
        """Query entities based on the provided criteria.

        Parameters
        ----------
        query : EntityQuery
            The query criteria for entities.

        Returns
        -------
        EntityQueryResult
            The query result containing matching entities and continuation token.
        """
        # Build the protobuf query
        pb_query = pb.EntityQuery(
            includeState=query.include_state,
            includeTransient=query.include_transient)

        if query.instance_id_starts_with is not None:
            pb_query.instanceIdStartsWith = wrappers_pb2.StringValue(value=query.instance_id_starts_with)
        if query.last_modified_from is not None:
            pb_query.lastModifiedFrom = helpers.new_timestamp(query.last_modified_from)
        if query.last_modified_to is not None:
            pb_query.lastModifiedTo = helpers.new_timestamp(query.last_modified_to)
        if query.page_size is not None:
            pb_query.pageSize = wrappers_pb2.Int32Value(value=query.page_size)
        if query.continuation_token is not None:
            pb_query.continuationToken = wrappers_pb2.StringValue(value=query.continuation_token)

        req = pb.QueryEntitiesRequest(query=pb_query)
        res: pb.QueryEntitiesResponse = self._stub.QueryEntities(req)

        # Convert response to Python objects
        entities = []
        for entity_metadata in res.entities:
            entities.append(task.EntityState(
                instance_id=entity_metadata.instanceId,
                last_modified_time=entity_metadata.lastModifiedTime.ToDatetime(),
                backlog_queue_size=entity_metadata.backlogQueueSize,
                locked_by=entity_metadata.lockedBy.value if not helpers.is_empty(entity_metadata.lockedBy) else None,
                serialized_state=entity_metadata.serializedState.value if not helpers.is_empty(entity_metadata.serializedState) else None))

        return task.EntityQueryResult(
            entities=entities,
            continuation_token=res.continuationToken.value if not helpers.is_empty(res.continuationToken) else None)

    def clean_entity_storage(self, *,
                            remove_empty_entities: bool = True,
                            release_orphaned_locks: bool = True,
                            continuation_token: Optional[str] = None) -> tuple[int, int, Optional[str]]:
        """Clean up entity storage by removing empty entities and releasing orphaned locks.

        Parameters
        ----------
        remove_empty_entities : bool
            Whether to remove entities that have no state.
        release_orphaned_locks : bool
            Whether to release locks that are no longer held by active orchestrations.
        continuation_token : Optional[str]
            A continuation token from a previous cleanup operation.

        Returns
        -------
        tuple[int, int, Optional[str]]
            A tuple containing (empty_entities_removed, orphaned_locks_released, continuation_token).
        """
        req = pb.CleanEntityStorageRequest(
            removeEmptyEntities=remove_empty_entities,
            releaseOrphanedLocks=release_orphaned_locks)

        if continuation_token is not None:
            req.continuationToken = wrappers_pb2.StringValue(value=continuation_token)

        self._logger.info("Cleaning entity storage.")
        res: pb.CleanEntityStorageResponse = self._stub.CleanEntityStorage(req)

        return (res.emptyEntitiesRemoved, 
                res.orphanedLocksReleased,
                res.continuationToken.value if not helpers.is_empty(res.continuationToken) else None)
