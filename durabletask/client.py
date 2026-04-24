# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import asyncio
import logging
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Generic, List, Optional, Sequence, TypeVar, Union

import grpc
import grpc.aio

import durabletask.history as history
from durabletask.entities import EntityInstanceId
from durabletask.entities.entity_metadata import EntityMetadata
from durabletask.grpc_options import (
    GrpcChannelOptions,
    GrpcClientResiliencyOptions,
)
import durabletask.internal.helpers as helpers
import durabletask.internal.history_helpers as history_helpers
import durabletask.internal.orchestrator_service_pb2 as pb
import durabletask.internal.orchestrator_service_pb2_grpc as stubs
import durabletask.internal.shared as shared
import durabletask.internal.tracing as tracing
from durabletask import task
from durabletask.internal.grpc_resiliency import (
    FailureTracker,
    is_client_transport_failure,
)
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
from durabletask.payload import helpers as payload_helpers
from durabletask.payload.store import PayloadStore

TInput = TypeVar('TInput')
TOutput = TypeVar('TOutput')
TItem = TypeVar('TItem')


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
class Page(Generic[TItem]):
    items: List[TItem]
    continuation_token: Optional[str]


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
                 channel: Optional[grpc.Channel] = None,
                 secure_channel: bool = False,
                 interceptors: Optional[Sequence[shared.ClientInterceptor]] = None,
                 channel_options: Optional[GrpcChannelOptions] = None,
                 resiliency_options: Optional[GrpcClientResiliencyOptions] = None,
                 default_version: Optional[str] = None,
                 payload_store: Optional[PayloadStore] = None):

        self._owns_channel = channel is None
        self._host_address = (
            host_address if host_address else shared.get_default_host_address()
        )
        self._secure_channel = secure_channel
        self._channel_options = channel_options
        self._resiliency_options = (
            resiliency_options
            if resiliency_options is not None
            else GrpcClientResiliencyOptions()
        )
        resolved_interceptors = (
            prepare_sync_interceptors(metadata, interceptors) if channel is None else interceptors
        )
        self._interceptors = (
            list(resolved_interceptors)
            if resolved_interceptors is not None
            else None
        )
        if channel is None:
            channel = shared.get_grpc_channel(
                host_address=self._host_address,
                secure_channel=secure_channel,
                interceptors=self._interceptors,
                channel_options=channel_options,
            )
        self._channel = channel
        self._stub = stubs.TaskHubSidecarServiceStub(channel)
        self._client_failure_tracker = FailureTracker(
            self._resiliency_options.channel_recreate_failure_threshold
        )
        self._closing = False
        self._last_recreate_time = 0.0
        self._recreate_lock = threading.Lock()
        self._retired_channels: dict[grpc.Channel, threading.Timer] = {}
        self._logger = shared.get_logger("client", log_handler, log_formatter)
        self.default_version = default_version
        self._payload_store = payload_store

    def _invoke_unary(
            self,
            method_name: str,
            request: Any,
            *,
            timeout: Optional[int] = None):
        method = getattr(self._stub, method_name)
        try:
            if timeout is None:
                response = method(request)
            else:
                response = method(request, timeout=timeout)
        except grpc.RpcError as rpc_error:
            status_code = rpc_error.code()
            if is_client_transport_failure(method_name, status_code):
                should_recreate = self._client_failure_tracker.record_failure()
                if should_recreate:
                    self._maybe_recreate_channel()
            else:
                self._client_failure_tracker.record_success()
            raise
        else:
            self._client_failure_tracker.record_success()
            return response

    def _maybe_recreate_channel(self) -> None:
        if not self._owns_channel or self._closing:
            return
        with self._recreate_lock:
            if self._closing:
                return
            now = time.monotonic()
            if now - self._last_recreate_time < self._resiliency_options.min_recreate_interval_seconds:
                return
            old_channel = self._channel
            self._channel = shared.get_grpc_channel(
                host_address=self._host_address,
                secure_channel=self._secure_channel,
                interceptors=self._interceptors,
                channel_options=self._channel_options,
            )
            self._stub = stubs.TaskHubSidecarServiceStub(self._channel)
            self._last_recreate_time = now
            self._client_failure_tracker.record_success()
            close_timer = threading.Timer(
                30.0,
                self._close_retired_channel,
                args=(old_channel,),
            )
            close_timer.daemon = True
            self._retired_channels[old_channel] = close_timer
            close_timer.start()

    def _close_retired_channel(self, channel: grpc.Channel) -> None:
        with self._recreate_lock:
            close_timer = self._retired_channels.pop(channel, None)
            if close_timer is None:
                return
        channel.close()

    def close(self) -> None:
        """Closes the underlying gRPC channel.

        Only closes channels created internally. If a pre-configured channel
        was passed via the ``channel`` constructor parameter, this method is
        a no-op — the caller retains ownership and is responsible for closing
        it.
        """
        if self._owns_channel:
            with self._recreate_lock:
                self._closing = True
                retired_channels = list(self._retired_channels.items())
                self._retired_channels.clear()
                current_channel = self._channel
            for retired_channel, close_timer in retired_channels:
                close_timer.cancel()
                retired_channel.close()
            current_channel.close()

    def schedule_new_orchestration(self, orchestrator: Union[task.Orchestrator[TInput, TOutput], str], *,
                                   input: Optional[TInput] = None,
                                   instance_id: Optional[str] = None,
                                   start_at: Optional[datetime] = None,
                                   reuse_id_policy: Optional[pb.OrchestrationIdReusePolicy] = None,
                                   tags: Optional[dict[str, str]] = None,
                                   version: Optional[str] = None) -> str:

        name = orchestrator if isinstance(orchestrator, str) else task.get_name(orchestrator)
        resolved_instance_id = instance_id if instance_id else uuid.uuid4().hex
        resolved_version = version if version else self.default_version

        with tracing.start_create_orchestration_span(
            name, resolved_instance_id, version=resolved_version,
        ):
            req = build_schedule_new_orchestration_req(
                orchestrator, input=input, instance_id=instance_id, start_at=start_at,
                reuse_id_policy=reuse_id_policy, tags=tags,
                version=version if version else self.default_version)

            # Inject the active PRODUCER span context into the request so the sidecar
            # stores it in the executionStarted event and the worker can parent all
            # orchestration/activity/timer spans under this trace.
            parent_trace_ctx = tracing.get_current_trace_context()
            if parent_trace_ctx is not None:
                req.parentTraceContext.CopyFrom(parent_trace_ctx)

            self._logger.info(f"Starting new '{req.name}' instance with ID = '{req.instanceId}'.")
            # Externalize any large payloads in the request
            if self._payload_store is not None:
                payload_helpers.externalize_payloads(
                    req, self._payload_store, instance_id=req.instanceId,
                )
            res: pb.CreateInstanceResponse = self._invoke_unary("StartInstance", req)
            return res.instanceId

    def get_orchestration_state(self, instance_id: str, *, fetch_payloads: bool = True) -> Optional[OrchestrationState]:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        res: pb.GetInstanceResponse = self._invoke_unary("GetInstance", req)
        # De-externalize any large-payload tokens in the response
        if self._payload_store is not None and res.exists:
            payload_helpers.deexternalize_payloads(res, self._payload_store)
        return new_orchestration_state(req.instanceId, res)

    def get_orchestration_history(self,
                                  instance_id: str, *,
                                  execution_id: Optional[str] = None,
                                  for_work_item_processing: bool = False) -> List[history.HistoryEvent]:
        req = pb.StreamInstanceHistoryRequest(
            instanceId=instance_id,
            executionId=helpers.get_string_value(execution_id),
            forWorkItemProcessing=for_work_item_processing,
        )
        self._logger.info(f"Retrieving history for instance '{instance_id}'.")
        stream = self._stub.StreamInstanceHistory(req)
        return history_helpers.collect_history_events(stream, self._payload_store)

    def list_instance_ids(self,
                          runtime_status: Optional[List[OrchestrationStatus]] = None,
                          completed_time_from: Optional[datetime] = None,
                          completed_time_to: Optional[datetime] = None,
                          page_size: Optional[int] = None,
                          continuation_token: Optional[str] = None) -> Page[str]:
        req = pb.ListInstanceIdsRequest(
            runtimeStatus=[status.value for status in runtime_status] if runtime_status else [],
            completedTimeFrom=helpers.new_timestamp(completed_time_from) if completed_time_from else None,
            completedTimeTo=helpers.new_timestamp(completed_time_to) if completed_time_to else None,
            pageSize=page_size or 0,
            lastInstanceKey=helpers.get_string_value(continuation_token),
        )
        self._logger.info(
            "Listing terminal instance IDs with filters: "
            f"runtime_status={[str(status) for status in runtime_status] if runtime_status else None}, "
            f"completed_time_from={completed_time_from}, "
            f"completed_time_to={completed_time_to}, "
            f"page_size={page_size}, "
            f"continuation_token={continuation_token}"
        )
        resp: pb.ListInstanceIdsResponse = self._invoke_unary("ListInstanceIds", req)
        next_token = resp.lastInstanceKey.value if resp.HasField("lastInstanceKey") else None
        return Page(items=list(resp.instanceIds), continuation_token=next_token)

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
            resp: pb.QueryInstancesResponse = self._invoke_unary("QueryInstances", req)
            if self._payload_store is not None:
                payload_helpers.deexternalize_payloads(resp, self._payload_store)
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
            res: pb.GetInstanceResponse = self._invoke_unary(
                "WaitForInstanceStart",
                req,
                timeout=timeout,
            )
            if self._payload_store is not None and res.exists:
                payload_helpers.deexternalize_payloads(res, self._payload_store)
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
            res: pb.GetInstanceResponse = self._invoke_unary(
                "WaitForInstanceCompletion",
                req,
                timeout=timeout,
            )
            if self._payload_store is not None and res.exists:
                payload_helpers.deexternalize_payloads(res, self._payload_store)
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
        with tracing.start_raise_event_span(event_name, instance_id):
            req = build_raise_event_req(instance_id, event_name, data)
            self._logger.info(f"Raising event '{event_name}' for instance '{instance_id}'.")
            if self._payload_store is not None:
                payload_helpers.externalize_payloads(
                    req, self._payload_store, instance_id=instance_id,
                )
            self._invoke_unary("RaiseEvent", req)

    def terminate_orchestration(self, instance_id: str, *,
                                output: Optional[Any] = None,
                                recursive: bool = True) -> None:
        req = build_terminate_req(instance_id, output, recursive)

        self._logger.info(f"Terminating instance '{instance_id}'.")
        if self._payload_store is not None:
            payload_helpers.externalize_payloads(
                req, self._payload_store, instance_id=instance_id,
            )
        self._invoke_unary("TerminateInstance", req)

    def suspend_orchestration(self, instance_id: str) -> None:
        req = pb.SuspendRequest(instanceId=instance_id)
        self._logger.info(f"Suspending instance '{instance_id}'.")
        self._invoke_unary("SuspendInstance", req)

    def resume_orchestration(self, instance_id: str) -> None:
        req = pb.ResumeRequest(instanceId=instance_id)
        self._logger.info(f"Resuming instance '{instance_id}'.")
        self._invoke_unary("ResumeInstance", req)

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
        res: pb.RestartInstanceResponse = self._invoke_unary("RestartInstance", req)
        return res.instanceId

    def purge_orchestration(self, instance_id: str, recursive: bool = True) -> PurgeInstancesResult:
        req = pb.PurgeInstancesRequest(instanceId=instance_id, recursive=recursive)
        self._logger.info(f"Purging instance '{instance_id}'.")
        resp: pb.PurgeInstancesResponse = self._invoke_unary("PurgeInstances", req)
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
        resp: pb.PurgeInstancesResponse = self._invoke_unary("PurgeInstances", req)
        return PurgeInstancesResult(resp.deletedInstanceCount, resp.isComplete.value)

    def signal_entity(self,
                      entity_instance_id: EntityInstanceId,
                      operation_name: str,
                      input: Optional[Any] = None) -> None:
        req = build_signal_entity_req(entity_instance_id, operation_name, input)
        self._logger.info(f"Signaling entity '{entity_instance_id}' operation '{operation_name}'.")
        if self._payload_store is not None:
            payload_helpers.externalize_payloads(
                req, self._payload_store, instance_id=str(entity_instance_id),
            )
        self._invoke_unary("SignalEntity", req)  # TODO: Cancellation timeout?

    def get_entity(self,
                   entity_instance_id: EntityInstanceId,
                   include_state: bool = True
                   ) -> Optional[EntityMetadata]:
        req = pb.GetEntityRequest(instanceId=str(entity_instance_id), includeState=include_state)
        self._logger.info(f"Getting entity '{entity_instance_id}'.")
        res: pb.GetEntityResponse = self._invoke_unary("GetEntity", req)
        if not res.exists:
            return None
        if self._payload_store is not None:
            payload_helpers.deexternalize_payloads(res, self._payload_store)
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
            resp: pb.QueryEntitiesResponse = self._invoke_unary("QueryEntities", query_request)
            if self._payload_store is not None:
                payload_helpers.deexternalize_payloads(resp, self._payload_store)
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
            resp: pb.CleanEntityStorageResponse = self._invoke_unary("CleanEntityStorage", req)
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
                 channel: Optional[grpc.aio.Channel] = None,
                 secure_channel: bool = False,
                 interceptors: Optional[Sequence[shared.AsyncClientInterceptor]] = None,
                 channel_options: Optional[GrpcChannelOptions] = None,
                 resiliency_options: Optional[GrpcClientResiliencyOptions] = None,
                 default_version: Optional[str] = None,
                 payload_store: Optional[PayloadStore] = None):

        self._owns_channel = channel is None
        self._host_address = (
            host_address if host_address else shared.get_default_host_address()
        )
        self._secure_channel = secure_channel
        self._channel_options = channel_options
        self._resiliency_options = (
            resiliency_options
            if resiliency_options is not None
            else GrpcClientResiliencyOptions()
        )
        resolved_interceptors = (
            prepare_async_interceptors(metadata, interceptors) if channel is None else interceptors
        )
        self._interceptors = (
            list(resolved_interceptors)
            if resolved_interceptors is not None
            else None
        )
        if channel is None:
            channel = shared.get_async_grpc_channel(
                host_address=self._host_address,
                secure_channel=secure_channel,
                interceptors=self._interceptors,
                channel_options=channel_options,
            )
        self._channel = channel
        self._stub = stubs.TaskHubSidecarServiceStub(channel)
        self._client_failure_tracker = FailureTracker(
            self._resiliency_options.channel_recreate_failure_threshold
        )
        self._closing = False
        self._recreate_lock = asyncio.Lock()
        self._last_recreate_time = 0.0
        self._retired_channels: list[grpc.aio.Channel] = []
        self._retired_channel_close_tasks: set[asyncio.Task[None]] = set()
        self._logger = shared.get_logger("async_client", log_handler, log_formatter)
        self.default_version = default_version
        self._payload_store = payload_store

    async def close(self) -> None:
        """Closes the underlying gRPC channel.

        Only closes channels created internally. If a pre-configured channel
        was passed via the ``channel`` constructor parameter, this method is
        a no-op — the caller retains ownership and is responsible for closing
        it.
        """
        if self._owns_channel:
            self._closing = True
            async with self._recreate_lock:
                retired_channels = list(self._retired_channels)
                self._retired_channels.clear()
                close_tasks = list(self._retired_channel_close_tasks)
                self._retired_channel_close_tasks.clear()
            for close_task in close_tasks:
                close_task.cancel()
            if close_tasks:
                await asyncio.gather(*close_tasks, return_exceptions=True)
            for retired_channel in retired_channels:
                await retired_channel.close()
            await self._channel.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def _invoke_unary(
            self,
            method_name: str,
            request: Any,
            *,
            timeout: Optional[int] = None):
        method = getattr(self._stub, method_name)
        try:
            if timeout is None:
                response = await method(request)
            else:
                response = await method(request, timeout=timeout)
        except grpc.aio.AioRpcError as rpc_error:
            if is_client_transport_failure(method_name, rpc_error.code()):
                should_recreate = self._client_failure_tracker.record_failure()
                if should_recreate:
                    await self._maybe_recreate_channel()
            else:
                self._client_failure_tracker.record_success()
            raise
        else:
            self._client_failure_tracker.record_success()
            return response

    async def _maybe_recreate_channel(self) -> None:
        if not self._owns_channel or self._closing:
            return
        async with self._recreate_lock:
            if self._closing:
                return
            now = time.monotonic()
            if now - self._last_recreate_time < self._resiliency_options.min_recreate_interval_seconds:
                return
            old_channel = self._channel
            self._channel = shared.get_async_grpc_channel(
                host_address=self._host_address,
                secure_channel=self._secure_channel,
                interceptors=self._interceptors,
                channel_options=self._channel_options,
            )
            self._stub = stubs.TaskHubSidecarServiceStub(self._channel)
            self._last_recreate_time = now
            self._client_failure_tracker.record_success()
            self._retired_channels.append(old_channel)
            close_task = asyncio.create_task(self._close_retired_channel(old_channel))
            self._retired_channel_close_tasks.add(close_task)
            close_task.add_done_callback(self._retired_channel_close_tasks.discard)

    async def _close_retired_channel(self, channel: grpc.aio.Channel) -> None:
        try:
            await asyncio.sleep(30.0)
            await channel.close()
        finally:
            try:
                self._retired_channels.remove(channel)
            except ValueError:
                pass

    async def schedule_new_orchestration(self, orchestrator: Union[task.Orchestrator[TInput, TOutput], str], *,
                                         input: Optional[TInput] = None,
                                         instance_id: Optional[str] = None,
                                         start_at: Optional[datetime] = None,
                                         reuse_id_policy: Optional[pb.OrchestrationIdReusePolicy] = None,
                                         tags: Optional[dict[str, str]] = None,
                                         version: Optional[str] = None) -> str:

        name = orchestrator if isinstance(orchestrator, str) else task.get_name(orchestrator)
        resolved_instance_id = instance_id if instance_id else uuid.uuid4().hex
        resolved_version = version if version else self.default_version

        with tracing.start_create_orchestration_span(
            name, resolved_instance_id, version=resolved_version,
        ):
            req = build_schedule_new_orchestration_req(
                orchestrator, input=input, instance_id=instance_id, start_at=start_at,
                reuse_id_policy=reuse_id_policy, tags=tags,
                version=version if version else self.default_version)

            parent_trace_ctx = tracing.get_current_trace_context()
            if parent_trace_ctx is not None:
                req.parentTraceContext.CopyFrom(parent_trace_ctx)

            self._logger.info(f"Starting new '{req.name}' instance with ID = '{req.instanceId}'.")
            # Externalize any large payloads in the request
            if self._payload_store is not None:
                await payload_helpers.externalize_payloads_async(
                    req, self._payload_store, instance_id=req.instanceId,
                )
            res: pb.CreateInstanceResponse = await self._invoke_unary("StartInstance", req)
            return res.instanceId

    async def get_orchestration_state(self, instance_id: str, *,
                                      fetch_payloads: bool = True) -> Optional[OrchestrationState]:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        res: pb.GetInstanceResponse = await self._invoke_unary("GetInstance", req)
        if self._payload_store is not None and res.exists:
            await payload_helpers.deexternalize_payloads_async(res, self._payload_store)
        return new_orchestration_state(req.instanceId, res)

    async def get_orchestration_history(self,
                                        instance_id: str, *,
                                        execution_id: Optional[str] = None,
                                        for_work_item_processing: bool = False) -> List[history.HistoryEvent]:
        req = pb.StreamInstanceHistoryRequest(
            instanceId=instance_id,
            executionId=helpers.get_string_value(execution_id),
            forWorkItemProcessing=for_work_item_processing,
        )
        self._logger.info(f"Retrieving history for instance '{instance_id}'.")
        stream = self._stub.StreamInstanceHistory(req)
        return await history_helpers.collect_history_events_async(stream, self._payload_store)

    async def list_instance_ids(self,
                                runtime_status: Optional[List[OrchestrationStatus]] = None,
                                completed_time_from: Optional[datetime] = None,
                                completed_time_to: Optional[datetime] = None,
                                page_size: Optional[int] = None,
                                continuation_token: Optional[str] = None) -> Page[str]:
        req = pb.ListInstanceIdsRequest(
            runtimeStatus=[status.value for status in runtime_status] if runtime_status else [],
            completedTimeFrom=helpers.new_timestamp(completed_time_from) if completed_time_from else None,
            completedTimeTo=helpers.new_timestamp(completed_time_to) if completed_time_to else None,
            pageSize=page_size or 0,
            lastInstanceKey=helpers.get_string_value(continuation_token),
        )
        self._logger.info(
            "Listing terminal instance IDs with filters: "
            f"runtime_status={[str(status) for status in runtime_status] if runtime_status else None}, "
            f"completed_time_from={completed_time_from}, "
            f"completed_time_to={completed_time_to}, "
            f"page_size={page_size}, "
            f"continuation_token={continuation_token}"
        )
        resp: pb.ListInstanceIdsResponse = await self._invoke_unary("ListInstanceIds", req)
        next_token = resp.lastInstanceKey.value if resp.HasField("lastInstanceKey") else None
        return Page(items=list(resp.instanceIds), continuation_token=next_token)

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
            resp: pb.QueryInstancesResponse = await self._invoke_unary("QueryInstances", req)
            if self._payload_store is not None:
                await payload_helpers.deexternalize_payloads_async(resp, self._payload_store)
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
            res: pb.GetInstanceResponse = await self._invoke_unary(
                "WaitForInstanceStart",
                req,
                timeout=timeout,
            )
            if self._payload_store is not None and res.exists:
                await payload_helpers.deexternalize_payloads_async(res, self._payload_store)
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
            res: pb.GetInstanceResponse = await self._invoke_unary(
                "WaitForInstanceCompletion",
                req,
                timeout=timeout,
            )
            if self._payload_store is not None and res.exists:
                await payload_helpers.deexternalize_payloads_async(res, self._payload_store)
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
        with tracing.start_raise_event_span(event_name, instance_id):
            req = build_raise_event_req(instance_id, event_name, data)
            self._logger.info(f"Raising event '{event_name}' for instance '{instance_id}'.")
            if self._payload_store is not None:
                await payload_helpers.externalize_payloads_async(
                    req, self._payload_store, instance_id=instance_id,
                )
            await self._invoke_unary("RaiseEvent", req)

    async def terminate_orchestration(self, instance_id: str, *,
                                      output: Optional[Any] = None,
                                      recursive: bool = True) -> None:
        req = build_terminate_req(instance_id, output, recursive)

        self._logger.info(f"Terminating instance '{instance_id}'.")
        if self._payload_store is not None:
            await payload_helpers.externalize_payloads_async(
                req, self._payload_store, instance_id=instance_id,
            )
        await self._invoke_unary("TerminateInstance", req)

    async def suspend_orchestration(self, instance_id: str) -> None:
        req = pb.SuspendRequest(instanceId=instance_id)
        self._logger.info(f"Suspending instance '{instance_id}'.")
        await self._invoke_unary("SuspendInstance", req)

    async def resume_orchestration(self, instance_id: str) -> None:
        req = pb.ResumeRequest(instanceId=instance_id)
        self._logger.info(f"Resuming instance '{instance_id}'.")
        await self._invoke_unary("ResumeInstance", req)

    async def restart_orchestration(self, instance_id: str, *,
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
        res: pb.RestartInstanceResponse = await self._invoke_unary("RestartInstance", req)
        return res.instanceId

    async def purge_orchestration(self, instance_id: str, recursive: bool = True) -> PurgeInstancesResult:
        req = pb.PurgeInstancesRequest(instanceId=instance_id, recursive=recursive)
        self._logger.info(f"Purging instance '{instance_id}'.")
        resp: pb.PurgeInstancesResponse = await self._invoke_unary("PurgeInstances", req)
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
        resp: pb.PurgeInstancesResponse = await self._invoke_unary("PurgeInstances", req)
        return PurgeInstancesResult(resp.deletedInstanceCount, resp.isComplete.value)

    async def signal_entity(self,
                            entity_instance_id: EntityInstanceId,
                            operation_name: str,
                            input: Optional[Any] = None) -> None:
        req = build_signal_entity_req(entity_instance_id, operation_name, input)
        self._logger.info(f"Signaling entity '{entity_instance_id}' operation '{operation_name}'.")
        if self._payload_store is not None:
            await payload_helpers.externalize_payloads_async(
                req, self._payload_store, instance_id=str(entity_instance_id),
            )
        await self._invoke_unary("SignalEntity", req)

    async def get_entity(self,
                         entity_instance_id: EntityInstanceId,
                         include_state: bool = True
                         ) -> Optional[EntityMetadata]:
        req = pb.GetEntityRequest(instanceId=str(entity_instance_id), includeState=include_state)
        self._logger.info(f"Getting entity '{entity_instance_id}'.")
        res: pb.GetEntityResponse = await self._invoke_unary("GetEntity", req)
        if not res.exists:
            return None
        if self._payload_store is not None:
            await payload_helpers.deexternalize_payloads_async(res, self._payload_store)
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
            resp: pb.QueryEntitiesResponse = await self._invoke_unary("QueryEntities", query_request)
            if self._payload_store is not None:
                await payload_helpers.deexternalize_payloads_async(resp, self._payload_store)
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
            resp: pb.CleanEntityStorageResponse = await self._invoke_unary("CleanEntityStorage", req)
            empty_entities_removed += resp.emptyEntitiesRemoved
            orphaned_locks_released += resp.orphanedLocksReleased

            if check_continuation_token(resp.continuationToken, _continuation_token, self._logger):
                _continuation_token = resp.continuationToken
            else:
                break

        return CleanEntityStorageResult(empty_entities_removed, orphaned_locks_released)
