# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, List, Optional, Sequence, TypeVar, Union

import grpc

from durabletask.entities import EntityInstanceId
from durabletask.entities.entity_metadata import EntityMetadata
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
        self.default_version = default_version

    def schedule_new_orchestration(self, orchestrator: Union[task.Orchestrator[TInput, TOutput], str], *,
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
            input=helpers.get_string_value(shared.to_json(input) if input is not None else None),
            scheduledStartTimestamp=helpers.new_timestamp(start_at) if start_at else None,
            version=helpers.get_string_value(version if version else self.default_version),
            orchestrationIdReusePolicy=reuse_id_policy,
            tags=tags
        )

        self._logger.info(f"Starting new '{name}' instance with ID = '{req.instanceId}'.")
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
            req = pb.QueryInstancesRequest(
                query=pb.InstanceQuery(
                    runtimeStatus=[status.value for status in orchestration_query.runtime_status] if orchestration_query.runtime_status else None,
                    createdTimeFrom=helpers.new_timestamp(orchestration_query.created_time_from) if orchestration_query.created_time_from else None,
                    createdTimeTo=helpers.new_timestamp(orchestration_query.created_time_to) if orchestration_query.created_time_to else None,
                    maxInstanceCount=orchestration_query.max_instance_count,
                    fetchInputsAndOutputs=orchestration_query.fetch_inputs_and_outputs,
                    continuationToken=_continuation_token
                )
            )
            resp: pb.QueryInstancesResponse = self._stub.QueryInstances(req)
            states += [parse_orchestration_state(res) for res in resp.orchestrationState]
            # Check the value for continuationToken - none or "0" indicates that there are no more results.
            if resp.continuationToken and resp.continuationToken.value and resp.continuationToken.value != "0":
                self._logger.info(f"Received continuation token with value {resp.continuationToken.value}, fetching next list of instances...")
                if _continuation_token and _continuation_token.value and _continuation_token.value == resp.continuationToken.value:
                    self._logger.warning(f"Received the same continuation token value {resp.continuationToken.value} again, stopping to avoid infinite loop.")
                    break
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
            input=helpers.get_string_value(shared.to_json(data) if data is not None else None)
        )

        self._logger.info(f"Raising event '{event_name}' for instance '{instance_id}'.")
        self._stub.RaiseEvent(req)

    def terminate_orchestration(self, instance_id: str, *,
                                output: Optional[Any] = None,
                                recursive: bool = True):
        req = pb.TerminateRequest(
            instanceId=instance_id,
            output=helpers.get_string_value(shared.to_json(output) if output is not None else None),
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
        resp: pb.PurgeInstancesResponse = self._stub.PurgeInstances(pb.PurgeInstancesRequest(
            purgeInstanceFilter=pb.PurgeInstanceFilter(
                createdTimeFrom=helpers.new_timestamp(created_time_from) if created_time_from else None,
                createdTimeTo=helpers.new_timestamp(created_time_to) if created_time_to else None,
                runtimeStatus=[status.value for status in runtime_status] if runtime_status else None
            ),
            recursive=recursive
        ))
        return PurgeInstancesResult(resp.deletedInstanceCount, resp.isComplete.value)

    def signal_entity(self,
                      entity_instance_id: EntityInstanceId,
                      operation_name: str,
                      input: Optional[Any] = None) -> None:
        req = pb.SignalEntityRequest(
            instanceId=str(entity_instance_id),
            name=operation_name,
            input=helpers.get_string_value(shared.to_json(input) if input is not None else None),
            requestId=str(uuid.uuid4()),
            scheduledTime=None,
            parentTraceContext=None,
            requestTime=helpers.new_timestamp(datetime.now(timezone.utc))
        )
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
            query_request = pb.QueryEntitiesRequest(
                query=pb.EntityQuery(
                    instanceIdStartsWith=helpers.get_string_value(entity_query.instance_id_starts_with),
                    lastModifiedFrom=helpers.new_timestamp(entity_query.last_modified_from) if entity_query.last_modified_from else None,
                    lastModifiedTo=helpers.new_timestamp(entity_query.last_modified_to) if entity_query.last_modified_to else None,
                    includeState=entity_query.include_state,
                    includeTransient=entity_query.include_transient,
                    pageSize=helpers.get_int_value(entity_query.page_size),
                    continuationToken=_continuation_token
                )
            )
            resp: pb.QueryEntitiesResponse = self._stub.QueryEntities(query_request)
            entities += [EntityMetadata.from_entity_metadata(entity, query_request.query.includeState) for entity in resp.entities]
            if resp.continuationToken and resp.continuationToken.value and resp.continuationToken.value != "0":
                self._logger.info(f"Received continuation token with value {resp.continuationToken.value}, fetching next page of entities...")
                if _continuation_token and _continuation_token.value and _continuation_token.value == resp.continuationToken.value:
                    self._logger.warning(f"Received the same continuation token value {resp.continuationToken.value} again, stopping to avoid infinite loop.")
                    break
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

            if resp.continuationToken and resp.continuationToken.value and resp.continuationToken.value != "0":
                self._logger.info(f"Received continuation token with value {resp.continuationToken.value}, cleaning next page...")
                if _continuation_token and _continuation_token.value and _continuation_token.value == resp.continuationToken.value:
                    self._logger.warning(f"Received the same continuation token value {resp.continuationToken.value} again, stopping to avoid infinite loop.")
                    break
                _continuation_token = resp.continuationToken
            else:
                break

        return CleanEntityStorageResult(empty_entities_removed, orphaned_locks_released)
