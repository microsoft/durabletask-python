# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import asyncio
import logging
import threading
import time
import uuid
from collections.abc import AsyncIterable, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Generic, Protocol, TypeVar, cast

import grpc
import grpc.aio
from google.protobuf import wrappers_pb2

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
    AsyncClientResiliencyInterceptor,
    ClientResiliencyInterceptor,
    FailureTracker,
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

    def __str__(self) -> str:
        return cast(str, helpers.get_orchestration_status_str(self.value))


@dataclass
class OrchestrationState:
    instance_id: str
    name: str
    runtime_status: OrchestrationStatus
    created_at: datetime
    last_updated_at: datetime
    serialized_input: str | None
    serialized_output: str | None
    serialized_custom_status: str | None
    failure_details: task.FailureDetails | None

    def raise_if_failed(self):
        if self.failure_details is not None:
            raise OrchestrationFailedError(
                f"Orchestration '{self.instance_id}' failed: {self.failure_details.message}",
                self.failure_details)


@dataclass
class OrchestrationQuery:
    created_time_from: datetime | None = None
    created_time_to: datetime | None = None
    runtime_status: list[OrchestrationStatus] | None = None
    # Some backends don't respond well with max_instance_count = None, so we use the integer limit for non-paginated
    # results instead.
    max_instance_count: int | None = (1 << 31) - 1
    fetch_inputs_and_outputs: bool = False


@dataclass
class EntityQuery:
    instance_id_starts_with: str | None = None
    last_modified_from: datetime | None = None
    last_modified_to: datetime | None = None
    include_state: bool = True
    include_transient: bool = False
    page_size: int | None = None


@dataclass
class PurgeInstancesResult:
    deleted_instance_count: int
    is_complete: bool


@dataclass
class Page(Generic[TItem]):
    items: list[TItem]
    continuation_token: str | None


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


def new_orchestration_state(instance_id: str, res: pb.GetInstanceResponse) -> OrchestrationState | None:
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


# Grace period before a retired SDK-owned channel is force-closed. Long enough
# for in-flight unary RPCs to drain on their own, short enough that recreate
# storms don't pile up dozens of half-closed channels.
_RETIRED_CHANNEL_CLOSE_DELAY_SECONDS = 30.0


class _SyncTaskHubSidecarServiceStub(Protocol):
    def StartInstance(self, request: pb.CreateInstanceRequest) -> pb.CreateInstanceResponse:
        ...

    def GetInstance(self, request: pb.GetInstanceRequest) -> pb.GetInstanceResponse:
        ...

    def StreamInstanceHistory(self, request: pb.StreamInstanceHistoryRequest) -> Iterable[pb.HistoryChunk]:
        ...

    def ListInstanceIds(self, request: pb.ListInstanceIdsRequest) -> pb.ListInstanceIdsResponse:
        ...

    def QueryInstances(self, request: pb.QueryInstancesRequest) -> pb.QueryInstancesResponse:
        ...

    def WaitForInstanceStart(
            self,
            request: pb.GetInstanceRequest,
            *,
            timeout: float | None = None) -> pb.GetInstanceResponse:
        ...

    def WaitForInstanceCompletion(
            self,
            request: pb.GetInstanceRequest,
            *,
            timeout: float | None = None) -> pb.GetInstanceResponse:
        ...

    def RaiseEvent(self, request: pb.RaiseEventRequest) -> pb.RaiseEventResponse:
        ...

    def TerminateInstance(self, request: pb.TerminateRequest) -> pb.TerminateResponse:
        ...

    def SuspendInstance(self, request: pb.SuspendRequest) -> pb.SuspendResponse:
        ...

    def ResumeInstance(self, request: pb.ResumeRequest) -> pb.ResumeResponse:
        ...

    def RestartInstance(self, request: pb.RestartInstanceRequest) -> pb.RestartInstanceResponse:
        ...

    def PurgeInstances(self, request: pb.PurgeInstancesRequest) -> pb.PurgeInstancesResponse:
        ...

    def SignalEntity(self, request: pb.SignalEntityRequest) -> pb.SignalEntityResponse:
        ...

    def GetEntity(self, request: pb.GetEntityRequest) -> pb.GetEntityResponse:
        ...

    def QueryEntities(self, request: pb.QueryEntitiesRequest) -> pb.QueryEntitiesResponse:
        ...

    def CleanEntityStorage(self, request: pb.CleanEntityStorageRequest) -> pb.CleanEntityStorageResponse:
        ...


class _AsyncTaskHubSidecarServiceStub(Protocol):
    async def StartInstance(self, request: pb.CreateInstanceRequest) -> pb.CreateInstanceResponse:
        ...

    async def GetInstance(self, request: pb.GetInstanceRequest) -> pb.GetInstanceResponse:
        ...

    def StreamInstanceHistory(self, request: pb.StreamInstanceHistoryRequest) -> AsyncIterable[pb.HistoryChunk]:
        ...

    async def ListInstanceIds(self, request: pb.ListInstanceIdsRequest) -> pb.ListInstanceIdsResponse:
        ...

    async def QueryInstances(self, request: pb.QueryInstancesRequest) -> pb.QueryInstancesResponse:
        ...

    async def WaitForInstanceStart(
            self,
            request: pb.GetInstanceRequest,
            *,
            timeout: float | None = None) -> pb.GetInstanceResponse:
        ...

    async def WaitForInstanceCompletion(
            self,
            request: pb.GetInstanceRequest,
            *,
            timeout: float | None = None) -> pb.GetInstanceResponse:
        ...

    async def RaiseEvent(self, request: pb.RaiseEventRequest) -> pb.RaiseEventResponse:
        ...

    async def TerminateInstance(self, request: pb.TerminateRequest) -> pb.TerminateResponse:
        ...

    async def SuspendInstance(self, request: pb.SuspendRequest) -> pb.SuspendResponse:
        ...

    async def ResumeInstance(self, request: pb.ResumeRequest) -> pb.ResumeResponse:
        ...

    async def RestartInstance(self, request: pb.RestartInstanceRequest) -> pb.RestartInstanceResponse:
        ...

    async def PurgeInstances(self, request: pb.PurgeInstancesRequest) -> pb.PurgeInstancesResponse:
        ...

    async def SignalEntity(self, request: pb.SignalEntityRequest) -> pb.SignalEntityResponse:
        ...

    async def GetEntity(self, request: pb.GetEntityRequest) -> pb.GetEntityResponse:
        ...

    async def QueryEntities(self, request: pb.QueryEntitiesRequest) -> pb.QueryEntitiesResponse:
        ...

    async def CleanEntityStorage(self, request: pb.CleanEntityStorageRequest) -> pb.CleanEntityStorageResponse:
        ...


class TaskHubGrpcClient:
    def __init__(self, *,
                 host_address: str | None = None,
                 metadata: list[tuple[str, str]] | None = None,
                 log_handler: logging.Handler | None = None,
                 log_formatter: logging.Formatter | None = None,
                 channel: grpc.Channel | None = None,
                 secure_channel: bool = False,
                 interceptors: Sequence[shared.ClientInterceptor] | None = None,
                 channel_options: GrpcChannelOptions | None = None,
                 resiliency_options: GrpcClientResiliencyOptions | None = None,
                 default_version: str | None = None,
                 payload_store: PayloadStore | None = None):

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
        # Resiliency state must be initialised BEFORE the interceptor is
        # constructed because the interceptor receives a bound reference to
        # ``self._schedule_recreate``; any failure handled during construction
        # of the underlying channel could otherwise observe a half-built
        # client.
        self._closing = False
        self._last_recreate_time = 0.0
        self._recreate_lock = threading.Lock()
        self._retired_channels: dict[grpc.Channel, threading.Timer] = {}
        self._recreate_thread_lock = threading.Lock()
        self._recreate_thread: threading.Thread | None = None
        # Test seam: set after each fire-and-forget recreate attempt finishes
        # (whether it actually recreated the channel or short-circuited on
        # close / cooldown). Lets tests synchronise without polling and lets
        # ``close()`` wait deterministically for an in-flight recreate.
        self._recreate_done_event = threading.Event()
        self._client_failure_tracker = FailureTracker(
            threshold=self._resiliency_options.channel_recreate_failure_threshold,
        )
        self._resiliency_interceptor = ClientResiliencyInterceptor(
            self._client_failure_tracker,
            self._schedule_recreate,
        )
        resolved_interceptors = (
            prepare_sync_interceptors(metadata, interceptors) if channel is None else interceptors
        )
        # Defensive copy so the caller cannot later mutate the list we hold and
        # break the resiliency wiring on a subsequent channel recreate.
        user_interceptors = (
            list(resolved_interceptors)
            if resolved_interceptors is not None
            else None
        )
        self._interceptors = self._compose_interceptors(user_interceptors)
        if channel is None:
            channel = shared.get_grpc_channel(
                host_address=self._host_address,
                secure_channel=secure_channel,
                interceptors=self._interceptors,
                channel_options=channel_options,
            )
        # For caller-owned channels we deliberately do not wrap with the
        # resiliency interceptor: the caller controls the channel lifetime and
        # we never recreate it, so failure tracking against it would have no
        # observable effect. Callers wanting resiliency on a custom channel
        # can prepend the interceptor themselves via grpc.intercept_channel.
        self._channel = channel
        self._stub = cast(_SyncTaskHubSidecarServiceStub, stubs.TaskHubSidecarServiceStub(channel))
        self._logger = shared.get_logger("client", log_handler, log_formatter)
        self.default_version = default_version
        self._payload_store = payload_store

    def _compose_interceptors(
            self,
            user_interceptors: list[shared.ClientInterceptor] | None,
    ) -> list[shared.ClientInterceptor]:
        """Prepend the resiliency interceptor so user interceptors run after it.

        The resiliency interceptor wraps the underlying continuation, so it
        observes every unary call regardless of any user-supplied interceptors.
        """
        composed: list[shared.ClientInterceptor] = [self._resiliency_interceptor]
        if user_interceptors:
            composed.extend(user_interceptors)
        return composed

    def _schedule_recreate(self) -> None:
        """Spawn a daemon thread that recreates the channel fire-and-forget.

        Called from the resiliency interceptor on the caller's thread when a
        unary RPC fails with a transport error. The interceptor returns to its
        caller as soon as this method returns, so the failing RPC's original
        error propagates without being delayed by DNS, TLS handshake, or
        contention on ``_recreate_lock``.

        Single-flight under ``_recreate_thread_lock``: if a recreate thread is
        still alive, the new trigger is dropped. The in-flight recreate will
        pick up the latest channel state on completion; the cooldown inside
        ``_maybe_recreate_channel`` further prevents thrash. ``thread.start()``
        is called under the lock so a follow-up caller's ``is_alive()`` check
        observes the running state rather than racing the start.
        """
        try:
            if self._closing:
                return
            with self._recreate_thread_lock:
                existing = self._recreate_thread
                if existing is not None and existing.is_alive():
                    return
                self._recreate_done_event.clear()
                thread = threading.Thread(
                    target=self._run_recreate,
                    name="durabletask-client-recreate",
                    daemon=True,
                )
                self._recreate_thread = thread
                thread.start()
        except Exception:
            self._logger.exception("Failed to schedule channel recreate")

    def _run_recreate(self) -> None:
        try:
            self._maybe_recreate_channel()
        except Exception:
            self._logger.exception("Channel recreate failed")
        finally:
            self._recreate_done_event.set()

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
            self._stub = cast(_SyncTaskHubSidecarServiceStub, stubs.TaskHubSidecarServiceStub(self._channel))
            self._last_recreate_time = now
            self._client_failure_tracker.record_success()
            close_timer = threading.Timer(
                _RETIRED_CHANNEL_CLOSE_DELAY_SECONDS,
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
            # Signal early so any in-flight recreate thread bails out of
            # ``_maybe_recreate_channel`` before we tear the channel down.
            self._closing = True
            with self._recreate_thread_lock:
                recreate_thread = self._recreate_thread
            if recreate_thread is not None and recreate_thread.is_alive():
                recreate_thread.join(timeout=5.0)
            with self._recreate_lock:
                retired_channels = list(self._retired_channels.items())
                self._retired_channels.clear()
                current_channel = self._channel
            for retired_channel, close_timer in retired_channels:
                close_timer.cancel()
                retired_channel.close()
            current_channel.close()

    def __enter__(self) -> "TaskHubGrpcClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def schedule_new_orchestration(self, orchestrator: task.Orchestrator[TInput, TOutput] | str, *,
                                   input: TInput | None = None,
                                   instance_id: str | None = None,
                                   start_at: datetime | None = None,
                                   reuse_id_policy: pb.OrchestrationIdReusePolicy | None = None,
                                   tags: dict[str, str] | None = None,
                                   version: str | None = None) -> str:

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
            res: pb.CreateInstanceResponse = self._stub.StartInstance(req)
            return res.instanceId

    def get_orchestration_state(self, instance_id: str, *, fetch_payloads: bool = True) -> OrchestrationState | None:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        res: pb.GetInstanceResponse = self._stub.GetInstance(req)
        # De-externalize any large-payload tokens in the response
        if self._payload_store is not None and res.exists:
            payload_helpers.deexternalize_payloads(res, self._payload_store)
        return new_orchestration_state(req.instanceId, res)

    def get_orchestration_history(self,
                                  instance_id: str, *,
                                  execution_id: str | None = None,
                                  for_work_item_processing: bool = False) -> list[history.HistoryEvent]:
        req = pb.StreamInstanceHistoryRequest(
            instanceId=instance_id,
            executionId=helpers.get_string_value(execution_id),
            forWorkItemProcessing=for_work_item_processing,
        )
        self._logger.info(f"Retrieving history for instance '{instance_id}'.")
        stream = self._stub.StreamInstanceHistory(req)
        return history_helpers.collect_history_events(stream, self._payload_store)

    def list_instance_ids(self,
                          runtime_status: list[OrchestrationStatus] | None = None,
                          completed_time_from: datetime | None = None,
                          completed_time_to: datetime | None = None,
                          page_size: int | None = None,
                          continuation_token: str | None = None) -> Page[str]:
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
        resp: pb.ListInstanceIdsResponse = self._stub.ListInstanceIds(req)
        next_token = resp.lastInstanceKey.value if resp.HasField("lastInstanceKey") else None
        return Page(items=list(resp.instanceIds), continuation_token=next_token)

    def get_all_orchestration_states(self,
                                     orchestration_query: OrchestrationQuery | None = None
                                     ) -> list[OrchestrationState]:
        if orchestration_query is None:
            orchestration_query = OrchestrationQuery()
        _continuation_token: wrappers_pb2.StringValue | None = None

        self._logger.info(f"Querying orchestration instances with query: {orchestration_query}")

        states: list[OrchestrationState] = []

        while True:
            req = build_query_instances_req(orchestration_query, _continuation_token)
            resp: pb.QueryInstancesResponse = self._stub.QueryInstances(req)
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
                                     timeout: float = 60) -> OrchestrationState | None:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        try:
            self._logger.info(f"Waiting up to {timeout}s for instance '{instance_id}' to start.")
            res: pb.GetInstanceResponse = self._stub.WaitForInstanceStart(
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
                                          timeout: float = 60) -> OrchestrationState | None:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        try:
            self._logger.info(f"Waiting {timeout}s for instance '{instance_id}' to complete.")
            res: pb.GetInstanceResponse = self._stub.WaitForInstanceCompletion(
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
                                  data: Any | None = None) -> None:
        with tracing.start_raise_event_span(event_name, instance_id):
            req = build_raise_event_req(instance_id, event_name, data)
            self._logger.info(f"Raising event '{event_name}' for instance '{instance_id}'.")
            if self._payload_store is not None:
                payload_helpers.externalize_payloads(
                    req, self._payload_store, instance_id=instance_id,
                )
            self._stub.RaiseEvent(req)

    def terminate_orchestration(self, instance_id: str, *,
                                output: Any | None = None,
                                recursive: bool = True) -> None:
        req = build_terminate_req(instance_id, output, recursive)

        self._logger.info(f"Terminating instance '{instance_id}'.")
        if self._payload_store is not None:
            payload_helpers.externalize_payloads(
                req, self._payload_store, instance_id=instance_id,
            )
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
                                created_time_from: datetime | None = None,
                                created_time_to: datetime | None = None,
                                runtime_status: list[OrchestrationStatus] | None = None,
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
                      input: Any | None = None) -> None:
        req = build_signal_entity_req(entity_instance_id, operation_name, input)
        self._logger.info(f"Signaling entity '{entity_instance_id}' operation '{operation_name}'.")
        if self._payload_store is not None:
            payload_helpers.externalize_payloads(
                req, self._payload_store, instance_id=str(entity_instance_id),
            )
        self._stub.SignalEntity(req)  # TODO: Cancellation timeout?

    def get_entity(self,
                   entity_instance_id: EntityInstanceId,
                   include_state: bool = True
                   ) -> EntityMetadata | None:
        req = pb.GetEntityRequest(instanceId=str(entity_instance_id), includeState=include_state)
        self._logger.info(f"Getting entity '{entity_instance_id}'.")
        res: pb.GetEntityResponse = self._stub.GetEntity(req)
        if not res.exists:
            return None
        if self._payload_store is not None:
            payload_helpers.deexternalize_payloads(res, self._payload_store)
        return EntityMetadata.from_entity_metadata(res.entity, include_state)

    def get_all_entities(self,
                         entity_query: EntityQuery | None = None) -> list[EntityMetadata]:
        if entity_query is None:
            entity_query = EntityQuery()
        _continuation_token: wrappers_pb2.StringValue | None = None

        self._logger.info(f"Retrieving entities by filter: {entity_query}")

        entities: list[EntityMetadata] = []

        while True:
            query_request = build_query_entities_req(entity_query, _continuation_token)
            resp: pb.QueryEntitiesResponse = self._stub.QueryEntities(query_request)
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
        _continuation_token: wrappers_pb2.StringValue | None = None

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
                 host_address: str | None = None,
                 metadata: list[tuple[str, str]] | None = None,
                 log_handler: logging.Handler | None = None,
                 log_formatter: logging.Formatter | None = None,
                 channel: grpc.aio.Channel | None = None,
                 secure_channel: bool = False,
                 interceptors: Sequence[shared.AsyncClientInterceptor] | None = None,
                 channel_options: GrpcChannelOptions | None = None,
                 resiliency_options: GrpcClientResiliencyOptions | None = None,
                 default_version: str | None = None,
                 payload_store: PayloadStore | None = None):

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
        # Resiliency state must be initialised BEFORE the interceptor is
        # constructed because the interceptor receives a bound reference to
        # ``self._schedule_recreate``; any failure handled during construction
        # of the underlying channel could otherwise observe a half-built
        # client.
        self._closing = False
        self._recreate_lock = asyncio.Lock()
        self._last_recreate_time = 0.0
        self._retired_channels: list[grpc.aio.Channel] = []
        self._retired_channel_close_tasks: set[asyncio.Task[None]] = set()
        self._recreate_task: asyncio.Task[None] | None = None
        # Test seam: set after each fire-and-forget recreate attempt finishes
        # (whether it actually recreated the channel or short-circuited on
        # close / cooldown). Lets tests synchronise without polling and lets
        # ``close()`` await an in-flight recreate deterministically.
        self._recreate_done_event = asyncio.Event()
        self._client_failure_tracker = FailureTracker(
            threshold=self._resiliency_options.channel_recreate_failure_threshold,
        )
        self._resiliency_interceptor = AsyncClientResiliencyInterceptor(
            self._client_failure_tracker,
            self._schedule_recreate,
        )
        resolved_interceptors = (
            prepare_async_interceptors(metadata, interceptors) if channel is None else interceptors
        )
        # Defensive copy so the caller cannot later mutate the list we hold and
        # break the resiliency wiring on a subsequent channel recreate.
        user_interceptors = (
            list(resolved_interceptors)
            if resolved_interceptors is not None
            else None
        )
        self._interceptors = self._compose_interceptors(user_interceptors)
        if channel is None:
            channel = shared.get_async_grpc_channel(
                host_address=self._host_address,
                secure_channel=secure_channel,
                interceptors=self._interceptors,
                channel_options=channel_options,
            )
        # Caller-owned channels cannot be retroactively wrapped with our
        # interceptor (``grpc.aio`` exposes no public equivalent of
        # ``grpc.intercept_channel``). We document this in :meth:`__init__` and
        # leave the failure-tracking opt-out implicit: callers wanting full
        # resiliency should let us create the channel.
        self._channel = channel
        self._stub = cast(_AsyncTaskHubSidecarServiceStub, stubs.TaskHubSidecarServiceStub(channel))
        self._logger = shared.get_logger("async_client", log_handler, log_formatter)
        self.default_version = default_version
        self._payload_store = payload_store

    def _compose_interceptors(
            self,
            user_interceptors: list[shared.AsyncClientInterceptor] | None,
    ) -> list[shared.AsyncClientInterceptor]:
        """Prepend the resiliency interceptor so user interceptors run after it."""
        composed: list[shared.AsyncClientInterceptor] = [self._resiliency_interceptor]
        if user_interceptors:
            composed.extend(user_interceptors)
        return composed

    async def close(self) -> None:
        """Closes the underlying gRPC channel.

        Only closes channels created internally. If a pre-configured channel
        was passed via the ``channel`` constructor parameter, this method is
        a no-op — the caller retains ownership and is responsible for closing
        it.
        """
        if self._owns_channel:
            # Signal early so any in-flight recreate task bails out of
            # ``_maybe_recreate_channel`` before we tear the channel down.
            self._closing = True
            recreate_task = self._recreate_task
            if recreate_task is not None and not recreate_task.done():
                try:
                    await recreate_task
                except Exception:
                    # Already logged by ``_run_recreate``; suppressing here
                    # ensures close() always tears down cleanly.
                    pass
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

    async def __aenter__(self) -> "AsyncTaskHubGrpcClient":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    def _schedule_recreate(self) -> None:
        """Schedule a fire-and-forget channel recreate on the event loop.

        Called from the resiliency interceptor when a unary RPC fails with a
        transport error. Single-flight: if ``_recreate_task`` is still
        pending, the trigger is dropped — the in-flight recreate will pick up
        the latest channel state on completion. asyncio is single-threaded
        so ``done()`` is race-free; no extra lock is required.
        """
        try:
            if self._closing:
                return
            existing = self._recreate_task
            if existing is not None and not existing.done():
                return
            self._recreate_done_event.clear()
            self._recreate_task = asyncio.create_task(self._run_recreate())
        except Exception:
            self._logger.exception("Failed to schedule channel recreate")

    async def _run_recreate(self) -> None:
        try:
            await self._maybe_recreate_channel()
        except Exception:
            self._logger.exception("Channel recreate failed")
        finally:
            self._recreate_done_event.set()

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
            self._stub = cast(_AsyncTaskHubSidecarServiceStub, stubs.TaskHubSidecarServiceStub(self._channel))
            self._last_recreate_time = now
            self._client_failure_tracker.record_success()
            self._retired_channels.append(old_channel)
            close_task = asyncio.create_task(self._close_retired_channel(old_channel))
            self._retired_channel_close_tasks.add(close_task)
            close_task.add_done_callback(self._retired_channel_close_tasks.discard)

    async def _close_retired_channel(self, channel: grpc.aio.Channel) -> None:
        try:
            await asyncio.sleep(_RETIRED_CHANNEL_CLOSE_DELAY_SECONDS)
            await channel.close()
        finally:
            async with self._recreate_lock:
                if channel in self._retired_channels:
                    self._retired_channels.remove(channel)

    async def schedule_new_orchestration(self, orchestrator: task.Orchestrator[TInput, TOutput] | str, *,
                                         input: TInput | None = None,
                                         instance_id: str | None = None,
                                         start_at: datetime | None = None,
                                         reuse_id_policy: pb.OrchestrationIdReusePolicy | None = None,
                                         tags: dict[str, str] | None = None,
                                         version: str | None = None) -> str:

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
            res: pb.CreateInstanceResponse = await self._stub.StartInstance(req)
            return res.instanceId

    async def get_orchestration_state(self, instance_id: str, *,
                                      fetch_payloads: bool = True) -> OrchestrationState | None:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        res: pb.GetInstanceResponse = await self._stub.GetInstance(req)
        if self._payload_store is not None and res.exists:
            await payload_helpers.deexternalize_payloads_async(res, self._payload_store)
        return new_orchestration_state(req.instanceId, res)

    async def get_orchestration_history(self,
                                        instance_id: str, *,
                                        execution_id: str | None = None,
                                        for_work_item_processing: bool = False) -> list[history.HistoryEvent]:
        req = pb.StreamInstanceHistoryRequest(
            instanceId=instance_id,
            executionId=helpers.get_string_value(execution_id),
            forWorkItemProcessing=for_work_item_processing,
        )
        self._logger.info(f"Retrieving history for instance '{instance_id}'.")
        stream = self._stub.StreamInstanceHistory(req)
        return await history_helpers.collect_history_events_async(stream, self._payload_store)

    async def list_instance_ids(self,
                                runtime_status: list[OrchestrationStatus] | None = None,
                                completed_time_from: datetime | None = None,
                                completed_time_to: datetime | None = None,
                                page_size: int | None = None,
                                continuation_token: str | None = None) -> Page[str]:
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
        resp: pb.ListInstanceIdsResponse = await self._stub.ListInstanceIds(req)
        next_token = resp.lastInstanceKey.value if resp.HasField("lastInstanceKey") else None
        return Page(items=list(resp.instanceIds), continuation_token=next_token)

    async def get_all_orchestration_states(self,
                                           orchestration_query: OrchestrationQuery | None = None
                                           ) -> list[OrchestrationState]:
        if orchestration_query is None:
            orchestration_query = OrchestrationQuery()
        _continuation_token: wrappers_pb2.StringValue | None = None

        self._logger.info(f"Querying orchestration instances with query: {orchestration_query}")

        states: list[OrchestrationState] = []

        while True:
            req = build_query_instances_req(orchestration_query, _continuation_token)
            resp: pb.QueryInstancesResponse = await self._stub.QueryInstances(req)
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
                                           timeout: float = 60) -> OrchestrationState | None:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        try:
            self._logger.info(f"Waiting up to {timeout}s for instance '{instance_id}' to start.")
            res: pb.GetInstanceResponse = await self._stub.WaitForInstanceStart(
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
                                                timeout: float = 60) -> OrchestrationState | None:
        req = pb.GetInstanceRequest(instanceId=instance_id, getInputsAndOutputs=fetch_payloads)
        try:
            self._logger.info(f"Waiting {timeout}s for instance '{instance_id}' to complete.")
            res: pb.GetInstanceResponse = await self._stub.WaitForInstanceCompletion(
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
                                        data: Any | None = None) -> None:
        with tracing.start_raise_event_span(event_name, instance_id):
            req = build_raise_event_req(instance_id, event_name, data)
            self._logger.info(f"Raising event '{event_name}' for instance '{instance_id}'.")
            if self._payload_store is not None:
                await payload_helpers.externalize_payloads_async(
                    req, self._payload_store, instance_id=instance_id,
                )
            await self._stub.RaiseEvent(req)

    async def terminate_orchestration(self, instance_id: str, *,
                                      output: Any | None = None,
                                      recursive: bool = True) -> None:
        req = build_terminate_req(instance_id, output, recursive)

        self._logger.info(f"Terminating instance '{instance_id}'.")
        if self._payload_store is not None:
            await payload_helpers.externalize_payloads_async(
                req, self._payload_store, instance_id=instance_id,
            )
        await self._stub.TerminateInstance(req)

    async def suspend_orchestration(self, instance_id: str) -> None:
        req = pb.SuspendRequest(instanceId=instance_id)
        self._logger.info(f"Suspending instance '{instance_id}'.")
        await self._stub.SuspendInstance(req)

    async def resume_orchestration(self, instance_id: str) -> None:
        req = pb.ResumeRequest(instanceId=instance_id)
        self._logger.info(f"Resuming instance '{instance_id}'.")
        await self._stub.ResumeInstance(req)

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
        res: pb.RestartInstanceResponse = await self._stub.RestartInstance(req)
        return res.instanceId

    async def purge_orchestration(self, instance_id: str, recursive: bool = True) -> PurgeInstancesResult:
        req = pb.PurgeInstancesRequest(instanceId=instance_id, recursive=recursive)
        self._logger.info(f"Purging instance '{instance_id}'.")
        resp: pb.PurgeInstancesResponse = await self._stub.PurgeInstances(req)
        return PurgeInstancesResult(resp.deletedInstanceCount, resp.isComplete.value)

    async def purge_orchestrations_by(self,
                                      created_time_from: datetime | None = None,
                                      created_time_to: datetime | None = None,
                                      runtime_status: list[OrchestrationStatus] | None = None,
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
                            input: Any | None = None) -> None:
        req = build_signal_entity_req(entity_instance_id, operation_name, input)
        self._logger.info(f"Signaling entity '{entity_instance_id}' operation '{operation_name}'.")
        if self._payload_store is not None:
            await payload_helpers.externalize_payloads_async(
                req, self._payload_store, instance_id=str(entity_instance_id),
            )
        await self._stub.SignalEntity(req)

    async def get_entity(self,
                         entity_instance_id: EntityInstanceId,
                         include_state: bool = True
                         ) -> EntityMetadata | None:
        req = pb.GetEntityRequest(instanceId=str(entity_instance_id), includeState=include_state)
        self._logger.info(f"Getting entity '{entity_instance_id}'.")
        res: pb.GetEntityResponse = await self._stub.GetEntity(req)
        if not res.exists:
            return None
        if self._payload_store is not None:
            await payload_helpers.deexternalize_payloads_async(res, self._payload_store)
        return EntityMetadata.from_entity_metadata(res.entity, include_state)

    async def get_all_entities(self,
                               entity_query: EntityQuery | None = None) -> list[EntityMetadata]:
        if entity_query is None:
            entity_query = EntityQuery()
        _continuation_token: wrappers_pb2.StringValue | None = None

        self._logger.info(f"Retrieving entities by filter: {entity_query}")

        entities: list[EntityMetadata] = []

        while True:
            query_request = build_query_entities_req(entity_query, _continuation_token)
            resp: pb.QueryEntitiesResponse = await self._stub.QueryEntities(query_request)
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
        _continuation_token: wrappers_pb2.StringValue | None = None

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
