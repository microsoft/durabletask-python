# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
In-memory backend for durable orchestrations suitable for testing.

This backend stores all orchestration state in memory and processes
work items synchronously within the same process. It is designed for
unit testing and integration testing scenarios where a sidecar process
or external storage is not desired.
"""

import logging
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

import grpc
from concurrent import futures
from google.protobuf import empty_pb2, timestamp_pb2, wrappers_pb2

import durabletask.internal.orchestrator_service_pb2 as pb
import durabletask.internal.orchestrator_service_pb2_grpc as stubs
import durabletask.internal.helpers as helpers


@dataclass
class OrchestrationInstance:
    """Internal orchestration instance state stored by the in-memory backend."""
    instance_id: str
    name: str
    status: pb.OrchestrationStatus
    input: Optional[str] = None
    output: Optional[str] = None
    custom_status: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    failure_details: Optional[pb.TaskFailureDetails] = None
    history: list[pb.HistoryEvent] = field(default_factory=list)
    pending_events: list[pb.HistoryEvent] = field(default_factory=list)
    dispatched_events: list[pb.HistoryEvent] = field(default_factory=list)
    completion_token: int = 0
    tags: Optional[dict[str, str]] = None


@dataclass
class ActivityWorkItem:
    """Activity work item that needs to be executed."""
    instance_id: str
    name: str
    task_id: int
    input: Optional[str]
    completion_token: int


@dataclass
class EntityState:
    """Internal entity state stored by the in-memory backend."""
    instance_id: str
    serialized_state: Optional[str] = None
    last_modified_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    locked_by: Optional[str] = None
    pending_operations: list[pb.HistoryEvent] = field(default_factory=list)
    completion_token: int = 0


@dataclass
class PendingLockRequest:
    """Pending lock request from an orchestration."""
    critical_section_id: str
    parent_instance_id: str
    lock_set: list[str]


@dataclass
class EntityWorkItem:
    """Entity work item that needs to be executed."""
    instance_id: str
    entity_state: Optional[str]
    operations: list[pb.HistoryEvent]
    completion_token: int


@dataclass
class StateWaiter:
    """Promise resolver for waiting on orchestration state changes."""
    predicate: Callable[[OrchestrationInstance], bool]
    event: threading.Event = field(default_factory=threading.Event)
    result: Optional[OrchestrationInstance] = None


class InMemoryOrchestrationBackend(stubs.TaskHubSidecarServiceServicer):
    """
    In-memory backend for durable orchestrations suitable for testing.

    This backend stores all orchestration state in memory and processes
    work items synchronously within the same process. It is designed for
    unit testing and integration testing scenarios where a sidecar process
    or external storage is not desired.

    Thread-safety: All state mutations are performed with locks to ensure
    thread-safe operations. The backend uses queues to manage work items
    for orchestrations and activities.
    """

    def __init__(self, max_history_size: int = 10000, port: int = 50051):
        """
        Creates a new in-memory backend.

        Args:
            max_history_size: Maximum number of history events per orchestration (default 10000)
            port: Port to listen on for gRPC connections (default 50051)
        """
        self._lock = threading.RLock()
        self._instances: dict[str, OrchestrationInstance] = {}
        self._orchestration_queue: deque[str] = deque()
        self._orchestration_queue_set: set[str] = set()
        self._activity_queue: deque[ActivityWorkItem] = deque()
        self._entities: dict[str, EntityState] = {}
        self._entity_queue: deque[str] = deque()
        self._entity_queue_set: set[str] = set()
        self._entity_in_flight: set[str] = set()
        self._pending_lock_requests: list[PendingLockRequest] = []
        self._orchestration_in_flight: set[str] = set()
        self._state_waiters: dict[str, list[StateWaiter]] = {}
        self._next_completion_token: int = 1
        self._max_history_size = max_history_size
        self._port = port
        self._server: Optional[grpc.Server] = None
        self._logger = logging.getLogger(__name__)
        self._shutdown_event = threading.Event()
        self._work_available = threading.Event()

    def start(self) -> str:
        """
        Starts the gRPC server on the configured port.

        Returns:
            The address the server is listening on (e.g., "localhost:50051")
        """
        self._shutdown_event.clear()
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        stubs.add_TaskHubSidecarServiceServicer_to_server(self, self._server)
        self._server.add_insecure_port(f'[::]:{self._port}')
        self._server.start()
        self._logger.info(f"In-memory backend started on port {self._port}")
        return f"localhost:{self._port}"

    def stop(self, grace: Optional[float] = None):
        """
        Stops the gRPC server.

        Args:
            grace: Grace period in seconds for graceful shutdown
        """
        self._shutdown_event.set()
        self._work_available.set()  # Unblock GetWorkItems loops
        if self._server:
            self._server.stop(grace)
            self._logger.info("In-memory backend stopped")

    def reset(self):
        """Resets the backend, clearing all state."""
        with self._lock:
            self._instances.clear()
            self._orchestration_queue.clear()
            self._orchestration_queue_set.clear()
            self._activity_queue.clear()
            self._entities.clear()
            self._entity_queue.clear()
            self._entity_queue_set.clear()
            self._entity_in_flight.clear()
            self._pending_lock_requests.clear()
            self._orchestration_in_flight.clear()
            for waiters in self._state_waiters.values():
                for waiter in waiters:
                    waiter.event.set()
            self._state_waiters.clear()
            self._shutdown_event.clear()
            self._work_available.clear()

    # gRPC Service Methods

    def Hello(self, request, context):
        """Sends a hello request to the sidecar service."""
        return empty_pb2.Empty()

    def StartInstance(self, request: pb.CreateInstanceRequest, context):
        """Starts a new orchestration instance."""
        instance_id = request.instanceId if request.instanceId else uuid.uuid4().hex

        with self._lock:
            if instance_id in self._instances:
                existing = self._instances[instance_id]
                policy = request.orchestrationIdReusePolicy
                replaceable = list(policy.replaceableStatus) if policy else []

                if replaceable:
                    # If the existing status is in the replaceable list,
                    # we can replace the instance
                    if existing.status in replaceable:
                        # Remove existing to allow re-creation
                        del self._instances[instance_id]
                        self._orchestration_queue_set.discard(instance_id)
                    else:
                        # Status not replaceable - reject
                        context.abort(
                            grpc.StatusCode.ALREADY_EXISTS,
                            f"Orchestration instance '{instance_id}' already exists "
                            f"with non-replaceable status")
                        return pb.CreateInstanceResponse(instanceId=instance_id)
                else:
                    context.abort(
                        grpc.StatusCode.ALREADY_EXISTS,
                        f"Orchestration instance '{instance_id}' already exists")
                    return pb.CreateInstanceResponse(instanceId=instance_id)

            now = datetime.now(timezone.utc)
            start_time = request.scheduledStartTimestamp.ToDatetime(tzinfo=timezone.utc) \
                if request.HasField("scheduledStartTimestamp") else now

            instance = OrchestrationInstance(
                instance_id=instance_id,
                name=request.name,
                status=pb.ORCHESTRATION_STATUS_PENDING,
                input=request.input.value if request.input else None,
                created_at=now,
                last_updated_at=now,
                completion_token=self._next_completion_token,
                tags=dict(request.tags) if request.tags else None,
            )
            self._next_completion_token += 1

            # Add initial events to start the orchestration
            orchestrator_started = helpers.new_orchestrator_started_event(start_time)
            execution_started = helpers.new_execution_started_event(
                request.name, instance_id,
                request.input.value if request.input else None,
                dict(request.tags) if request.tags else None
            )

            instance.pending_events.append(orchestrator_started)
            instance.pending_events.append(execution_started)

            self._instances[instance_id] = instance
            self._enqueue_orchestration(instance_id)

            self._logger.info(f"Created orchestration instance '{instance_id}' for '{request.name}'")

        return pb.CreateInstanceResponse(instanceId=instance_id)

    def GetInstance(self, request: pb.GetInstanceRequest, context):
        """Gets the status of an existing orchestration instance."""
        with self._lock:
            instance = self._instances.get(request.instanceId)
            if not instance:
                return pb.GetInstanceResponse(exists=False)

            return self._build_instance_response(instance, request.getInputsAndOutputs)

    def WaitForInstanceStart(self, request: pb.GetInstanceRequest, context):
        """Waits for an orchestration instance to reach a running or completion state."""
        def predicate(inst: OrchestrationInstance) -> bool:
            return inst.status != pb.ORCHESTRATION_STATUS_PENDING

        instance = self._wait_for_state(request.instanceId, predicate, timeout=context.time_remaining())

        if not instance:
            return pb.GetInstanceResponse(exists=False)

        return self._build_instance_response(instance, request.getInputsAndOutputs)

    def WaitForInstanceCompletion(self, request: pb.GetInstanceRequest, context):
        """Waits for an orchestration instance to reach a completion state."""
        instance = self._wait_for_state(
            request.instanceId,
            self._is_terminal_status_check,
            timeout=context.time_remaining()
        )

        if not instance:
            return pb.GetInstanceResponse(exists=False)

        return self._build_instance_response(instance, request.getInputsAndOutputs)

    def RaiseEvent(self, request: pb.RaiseEventRequest, context):
        """Raises an event to a running orchestration instance."""
        with self._lock:
            instance = self._instances.get(request.instanceId)
            if not instance:
                context.abort(grpc.StatusCode.NOT_FOUND,
                              f"Orchestration instance '{request.instanceId}' not found")
                return pb.RaiseEventResponse()

            event = helpers.new_event_raised_event(
                request.name,
                request.input.value if request.input else None
            )
            instance.pending_events.append(event)
            instance.last_updated_at = datetime.now(timezone.utc)
            self._enqueue_orchestration(instance.instance_id)

        self._logger.info(f"Raised event '{request.name}' for instance '{request.instanceId}'")
        return pb.RaiseEventResponse()

    def TerminateInstance(self, request: pb.TerminateRequest, context):
        """Terminates a running orchestration instance."""
        with self._lock:
            self._terminate_instance_internal(
                request.instanceId,
                request.output.value if request.output else None,
                request.recursive
            )

        return pb.TerminateResponse()

    def SuspendInstance(self, request: pb.SuspendRequest, context):
        """Suspends a running orchestration instance."""
        with self._lock:
            instance = self._instances.get(request.instanceId)
            if not instance:
                context.abort(grpc.StatusCode.NOT_FOUND,
                              f"Orchestration instance '{request.instanceId}' not found")
                return pb.SuspendResponse()

            if instance.status == pb.ORCHESTRATION_STATUS_SUSPENDED:
                return pb.SuspendResponse()

            event = helpers.new_suspend_event()
            instance.pending_events.append(event)
            instance.last_updated_at = datetime.now(timezone.utc)
            self._enqueue_orchestration(instance.instance_id)

        self._logger.info(f"Suspended instance '{request.instanceId}'")
        return pb.SuspendResponse()

    def ResumeInstance(self, request: pb.ResumeRequest, context):
        """Resumes a suspended orchestration instance."""
        with self._lock:
            instance = self._instances.get(request.instanceId)
            if not instance:
                context.abort(grpc.StatusCode.NOT_FOUND,
                              f"Orchestration instance '{request.instanceId}' not found")
                return pb.ResumeResponse()

            event = helpers.new_resume_event()
            instance.pending_events.append(event)
            instance.last_updated_at = datetime.now(timezone.utc)
            self._enqueue_orchestration(instance.instance_id)

        self._logger.info(f"Resumed instance '{request.instanceId}'")
        return pb.ResumeResponse()

    def PurgeInstances(self, request: pb.PurgeInstancesRequest, context):
        """Purges orchestration instances from the store."""
        purged_count = 0

        with self._lock:
            instance_id = request.instanceId
            if instance_id:
                # Single instance purge
                instance = self._instances.get(instance_id)
                if instance and self._is_terminal_status(instance.status):
                    del self._instances[instance_id]
                    self._state_waiters.pop(instance_id, None)
                    purged_count = 1
            elif request.HasField("purgeInstanceFilter"):
                # Filter-based purge
                pf = request.purgeInstanceFilter
                to_purge = []
                for iid, inst in self._instances.items():
                    if not self._is_terminal_status(inst.status):
                        continue
                    if pf.runtimeStatus and inst.status not in pf.runtimeStatus:
                        continue
                    if pf.HasField("createdTimeFrom") and inst.created_at < pf.createdTimeFrom.ToDatetime(timezone.utc):
                        continue
                    if pf.HasField("createdTimeTo") and inst.created_at >= pf.createdTimeTo.ToDatetime(timezone.utc):
                        continue
                    to_purge.append(iid)
                for iid in to_purge:
                    del self._instances[iid]
                    self._state_waiters.pop(iid, None)
                purged_count = len(to_purge)

        self._logger.info(f"Purged {purged_count} instance(s)")
        return pb.PurgeInstancesResponse(
            deletedInstanceCount=purged_count,
            isComplete=wrappers_pb2.BoolValue(value=True),
        )

    def GetWorkItems(self, request: pb.GetWorkItemsRequest, context):
        """Streams work items to the worker (orchestration and activity work items)."""
        self._logger.info("Worker connected and requesting work items")

        try:
            while context.is_active() and not self._shutdown_event.is_set():
                work_item = None

                with self._lock:
                    # Check for orchestration work
                    while self._orchestration_queue:
                        instance_id = self._orchestration_queue.popleft()
                        self._orchestration_queue_set.discard(instance_id)
                        instance = self._instances.get(instance_id)

                        if not instance or not instance.pending_events:
                            continue

                        if instance_id in self._orchestration_in_flight:
                            # Already being processed — re-add to queue
                            if instance_id not in self._orchestration_queue_set:
                                self._orchestration_queue.append(instance_id)
                                self._orchestration_queue_set.add(instance_id)
                            break

                        # Move pending events to dispatched_events
                        instance.dispatched_events = list(instance.pending_events)
                        instance.pending_events.clear()

                        # Add OrchestratorStarted for re-dispatches so that
                        # ctx.current_utc_datetime advances correctly
                        if instance.history:
                            now = datetime.now(timezone.utc)
                            orch_started = helpers.new_orchestrator_started_event(now)
                            instance.dispatched_events.insert(0, orch_started)

                        self._orchestration_in_flight.add(instance_id)

                        # Create orchestrator work item
                        work_item = pb.WorkItem(
                            completionToken=str(instance.completion_token),
                            orchestratorRequest=pb.OrchestratorRequest(
                                instanceId=instance.instance_id,
                                pastEvents=list(instance.history),
                                newEvents=list(instance.dispatched_events),
                            )
                        )
                        break

                    # Check for activity work
                    if not work_item and self._activity_queue:
                        activity = self._activity_queue.popleft()
                        work_item = pb.WorkItem(
                            completionToken=str(activity.completion_token),
                            activityRequest=pb.ActivityRequest(
                                name=activity.name,
                                taskId=activity.task_id,
                                input=wrappers_pb2.StringValue(value=activity.input) if activity.input else None,
                                orchestrationInstance=pb.OrchestrationInstance(instanceId=activity.instance_id)
                            )
                        )

                    # Check for entity work
                    if not work_item:
                        while self._entity_queue:
                            entity_id = self._entity_queue.popleft()
                            self._entity_queue_set.discard(entity_id)
                            entity = self._entities.get(entity_id)

                            if entity and entity.pending_operations:
                                # Skip if this entity is already being processed
                                if entity_id in self._entity_in_flight:
                                    continue

                                # Mark as in-flight to prevent duplicate dispatch
                                self._entity_in_flight.add(entity_id)

                                # Drain all pending operations into a batch
                                operations = list(entity.pending_operations)
                                entity.pending_operations.clear()

                                # Use V2 EntityRequest format so the worker
                                # can properly build operation_infos
                                work_item = pb.WorkItem(
                                    completionToken=str(entity.completion_token),
                                    entityRequestV2=pb.EntityRequest(
                                        instanceId=entity.instance_id,
                                        entityState=wrappers_pb2.StringValue(
                                            value=entity.serialized_state
                                        ) if entity.serialized_state else None,
                                        operationRequests=operations,
                                    ),
                                )
                                break

                if work_item:
                    yield work_item
                else:
                    # Wait for work to become available (with timeout for shutdown checks)
                    self._work_available.wait(timeout=0.1)
                    self._work_available.clear()

        except Exception:
            self._logger.exception("Error in GetWorkItems stream")

    def CompleteOrchestratorTask(self, request: pb.OrchestratorResponse, context):
        """Completes an orchestration execution with the given actions."""
        with self._lock:
            instance = self._instances.get(request.instanceId)
            if not instance:
                self._logger.warning(f"Instance '{request.instanceId}' not found for completion")
                self._orchestration_in_flight.discard(request.instanceId)
                return pb.CompleteTaskResponse()

            if str(instance.completion_token) != request.completionToken:
                self._logger.warning(
                    f"Stale completion for instance '{request.instanceId}' - ignoring"
                )
                self._orchestration_in_flight.discard(request.instanceId)
                return pb.CompleteTaskResponse()

            # Check history size limit
            projected_size = len(instance.history) + len(instance.dispatched_events)
            if projected_size > self._max_history_size:
                self._orchestration_in_flight.discard(request.instanceId)
                context.abort(
                    grpc.StatusCode.RESOURCE_EXHAUSTED,
                    f"Orchestration '{request.instanceId}' would exceed maximum history size"
                )
                return pb.CompleteTaskResponse()

            # Move dispatched events to history
            new_events = list(instance.dispatched_events)
            instance.history.extend(new_events)
            instance.dispatched_events.clear()
            instance.last_updated_at = datetime.now(timezone.utc)

            if request.customStatus:
                instance.custom_status = request.customStatus.value

            # Transition to RUNNING once processed for the first time
            if instance.status == pb.ORCHESTRATION_STATUS_PENDING:
                instance.status = pb.ORCHESTRATION_STATUS_RUNNING

            # Check for suspend/resume events and update status
            for evt in new_events:
                if evt.HasField("executionSuspended"):
                    instance.status = pb.ORCHESTRATION_STATUS_SUSPENDED
                elif evt.HasField("executionResumed"):
                    instance.status = pb.ORCHESTRATION_STATUS_RUNNING

            # Process actions
            for action in request.actions:
                self._process_action(instance, action)

            # Update completion token for next execution
            instance.completion_token = self._next_completion_token
            self._next_completion_token += 1

            # Remove from in-flight before notifying or re-enqueuing
            self._orchestration_in_flight.discard(request.instanceId)

            # Notify waiters
            self._notify_waiters(request.instanceId)

            # Re-enqueue if new events arrived while the orchestration was
            # in-flight (between dispatch and completion)
            not_terminal = not self._is_terminal_status(instance.status)
            not_suspended = instance.status != pb.ORCHESTRATION_STATUS_SUSPENDED
            if instance.pending_events and not_terminal and not_suspended:
                self._enqueue_orchestration(request.instanceId)

        return pb.CompleteTaskResponse()

    def CompleteActivityTask(self, request: pb.ActivityResponse, context):
        """Completes an activity execution."""
        with self._lock:
            instance = self._instances.get(request.instanceId)
            if not instance:
                self._logger.warning(f"Instance '{request.instanceId}' not found for activity completion")
                return pb.CompleteTaskResponse()

            if request.failureDetails and request.failureDetails.errorMessage:
                # Activity failed
                event = pb.HistoryEvent(
                    eventId=-1,
                    timestamp=timestamp_pb2.Timestamp(),
                    taskFailed=pb.TaskFailedEvent(
                        taskScheduledId=request.taskId,
                        failureDetails=request.failureDetails
                    )
                )
            else:
                # Activity succeeded
                event = pb.HistoryEvent(
                    eventId=-1,
                    timestamp=timestamp_pb2.Timestamp(),
                    taskCompleted=pb.TaskCompletedEvent(
                        taskScheduledId=request.taskId,
                        result=request.result
                    )
                )

            instance.pending_events.append(event)
            instance.last_updated_at = datetime.now(timezone.utc)
            self._enqueue_orchestration(request.instanceId)

        return pb.CompleteTaskResponse()

    def CompleteEntityTask(self, request: pb.EntityBatchResult, context):
        """Completes an entity batch execution."""
        with self._lock:
            # Find entity by completion token
            entity = None
            for e in self._entities.values():
                if str(e.completion_token) == request.completionToken:
                    entity = e
                    break

            if not entity:
                self._logger.warning(
                    f"No entity found for completion token '{request.completionToken}'"
                )
                return pb.CompleteTaskResponse()

            # Update entity state
            if request.entityState and request.entityState.value:
                entity.serialized_state = request.entityState.value
            entity.last_modified_at = datetime.now(timezone.utc)

            # Update completion token for next batch
            entity.completion_token = self._next_completion_token
            self._next_completion_token += 1

            # Clear the in-flight flag
            self._entity_in_flight.discard(entity.instance_id)

            # Deliver operation results to calling orchestrations
            for i, op_info in enumerate(request.operationInfos):
                dest = op_info.responseDestination
                if dest and dest.instanceId:
                    parent_instance_id = op_info.responseDestination.instanceId
                    parent_instance = self._instances.get(parent_instance_id)
                    if parent_instance:
                        result = request.results[i] if i < len(request.results) else None
                        if result and result.HasField("success"):
                            event = pb.HistoryEvent(
                                eventId=-1,
                                timestamp=timestamp_pb2.Timestamp(),
                                entityOperationCompleted=pb.EntityOperationCompletedEvent(
                                    requestId=op_info.requestId,
                                    output=result.success.result,
                                )
                            )
                        elif result and result.HasField("failure"):
                            event = pb.HistoryEvent(
                                eventId=-1,
                                timestamp=timestamp_pb2.Timestamp(),
                                entityOperationFailed=pb.EntityOperationFailedEvent(
                                    requestId=op_info.requestId,
                                    failureDetails=result.failure.failureDetails,
                                )
                            )
                        else:
                            continue

                        parent_instance.pending_events.append(event)
                        parent_instance.last_updated_at = datetime.now(timezone.utc)
                        self._enqueue_orchestration(parent_instance_id)

            # Process side-effect actions (signals to other entities, new orchestrations)
            for action in request.actions:
                if action.HasField("sendSignal"):
                    signal = action.sendSignal
                    self._signal_entity_internal(
                        signal.instanceId, signal.name,
                        signal.input.value if signal.input else None
                    )
                elif action.HasField("startNewOrchestration"):
                    start_orch = action.startNewOrchestration
                    orch_input = start_orch.input.value if start_orch.input else None
                    instance_id = start_orch.instanceId or uuid.uuid4().hex
                    try:
                        self._create_instance_internal(
                            instance_id, start_orch.name, orch_input
                        )
                    except Exception:
                        self._logger.warning(
                            f"Failed to create orchestration '{instance_id}' from entity action"
                        )

            # If the entity has more pending operations, re-enqueue
            if entity.pending_operations:
                self._enqueue_entity(entity.instance_id)

        return pb.CompleteTaskResponse()

    def SignalEntity(self, request: pb.SignalEntityRequest, context):
        """Signals an entity, queueing an operation for processing."""
        with self._lock:
            entity_id = request.instanceId
            entity = self._entities.get(entity_id)
            if not entity:
                entity = EntityState(
                    instance_id=entity_id,
                    completion_token=self._next_completion_token,
                )
                self._next_completion_token += 1
                self._entities[entity_id] = entity

            # Create a signaled operation event
            event = pb.HistoryEvent(
                eventId=-1,
                timestamp=timestamp_pb2.Timestamp(),
                entityOperationSignaled=pb.EntityOperationSignaledEvent(
                    requestId=request.requestId,
                    operation=request.name,
                    input=request.input if request.input else None,
                    targetInstanceId=wrappers_pb2.StringValue(value=entity_id),
                )
            )
            entity.pending_operations.append(event)
            self._enqueue_entity(entity_id)

        self._logger.info(f"Signaled entity '{entity_id}' operation '{request.name}'")
        return pb.SignalEntityResponse()

    def GetEntity(self, request: pb.GetEntityRequest, context):
        """Gets entity state."""
        with self._lock:
            entity = self._entities.get(request.instanceId)
            if not entity:
                return pb.GetEntityResponse(exists=False)

            last_modified_ts = timestamp_pb2.Timestamp()
            last_modified_ts.FromDatetime(entity.last_modified_at)

            metadata = pb.EntityMetadata(
                instanceId=entity.instance_id,
                lastModifiedTime=last_modified_ts,
                backlogQueueSize=len(entity.pending_operations),
                lockedBy=wrappers_pb2.StringValue(value=entity.locked_by) if entity.locked_by else None,
                serializedState=wrappers_pb2.StringValue(
                    value=entity.serialized_state) if request.includeState and entity.serialized_state else None,
            )

            return pb.GetEntityResponse(exists=True, entity=metadata)

    def QueryInstances(self, request: pb.QueryInstancesRequest, context):
        """Query orchestration instances with filtering support."""
        with self._lock:
            query = request.query
            start_index = 0
            if query.HasField("continuationToken") and query.continuationToken.value:
                try:
                    start_index = int(query.continuationToken.value)
                except ValueError:
                    start_index = 0

            matching = []
            for instance in self._instances.values():
                # Filter by runtime status
                if query.runtimeStatus and instance.status not in query.runtimeStatus:
                    continue
                # Filter by created time range
                if query.HasField("createdTimeFrom") and instance.created_at < query.createdTimeFrom.ToDatetime(timezone.utc):
                    continue
                if query.HasField("createdTimeTo") and instance.created_at >= query.createdTimeTo.ToDatetime(timezone.utc):
                    continue
                # Filter by instance ID prefix
                if query.HasField("instanceIdPrefix") and query.instanceIdPrefix.value:
                    if not instance.instance_id.startswith(query.instanceIdPrefix.value):
                        continue
                matching.append(instance)

            # Sort by created time for deterministic pagination
            matching.sort(key=lambda i: i.created_at)

            # Apply pagination
            page_size = query.maxInstanceCount if query.maxInstanceCount > 0 else len(matching)
            page = matching[start_index:start_index + page_size]

            states = []
            for inst in page:
                created_ts = timestamp_pb2.Timestamp()
                created_ts.FromDatetime(inst.created_at)
                updated_ts = timestamp_pb2.Timestamp()
                updated_ts.FromDatetime(inst.last_updated_at)

                include = query.fetchInputsAndOutputs
                state = pb.OrchestrationState(
                    instanceId=inst.instance_id,
                    name=inst.name,
                    orchestrationStatus=inst.status,
                    createdTimestamp=created_ts,
                    lastUpdatedTimestamp=updated_ts,
                    input=wrappers_pb2.StringValue(value=inst.input) if include and inst.input else None,
                    output=wrappers_pb2.StringValue(value=inst.output) if include and inst.output else None,
                    customStatus=wrappers_pb2.StringValue(
                        value=inst.custom_status) if inst.custom_status else None,
                    failureDetails=inst.failure_details if inst.failure_details else None,
                )
                states.append(state)

            # Compute continuation token
            next_index = start_index + page_size
            continuation_token = None
            if next_index < len(matching):
                continuation_token = wrappers_pb2.StringValue(value=str(next_index))

        return pb.QueryInstancesResponse(
            orchestrationState=states,
            continuationToken=continuation_token,
        )

    def QueryEntities(self, request: pb.QueryEntitiesRequest, context):
        """Query entities with filtering support."""
        with self._lock:
            query = request.query
            start_index = 0
            if query.HasField("continuationToken") and query.continuationToken.value:
                try:
                    start_index = int(query.continuationToken.value)
                except ValueError:
                    start_index = 0

            matching = []
            for entity in self._entities.values():
                # Filter by instance ID prefix
                if query.HasField("instanceIdStartsWith") and query.instanceIdStartsWith.value:
                    if not entity.instance_id.startswith(query.instanceIdStartsWith.value):
                        continue
                # Filter by last modified time range
                if query.HasField("lastModifiedFrom") and entity.last_modified_at < query.lastModifiedFrom.ToDatetime(timezone.utc):
                    continue
                if query.HasField("lastModifiedTo") and entity.last_modified_at >= query.lastModifiedTo.ToDatetime(timezone.utc):
                    continue
                # Filter transient (entities with pending operations)
                if not query.includeTransient and entity.pending_operations:
                    continue
                matching.append(entity)

            # Sort by instance_id for deterministic pagination
            matching.sort(key=lambda e: e.instance_id)

            # Apply pagination
            page_size = query.pageSize.value if query.HasField("pageSize") and query.pageSize.value > 0 else len(matching)
            page = matching[start_index:start_index + page_size]

            entities = []
            for ent in page:
                last_modified_ts = timestamp_pb2.Timestamp()
                last_modified_ts.FromDatetime(ent.last_modified_at)

                metadata = pb.EntityMetadata(
                    instanceId=ent.instance_id,
                    lastModifiedTime=last_modified_ts,
                    backlogQueueSize=len(ent.pending_operations),
                    lockedBy=wrappers_pb2.StringValue(value=ent.locked_by) if ent.locked_by else None,
                    serializedState=wrappers_pb2.StringValue(
                        value=ent.serialized_state
                    ) if query.includeState and ent.serialized_state else None,
                )
                entities.append(metadata)

            # Compute continuation token
            next_index = start_index + page_size
            continuation_token = None
            if next_index < len(matching):
                continuation_token = wrappers_pb2.StringValue(value=str(next_index))

        return pb.QueryEntitiesResponse(
            entities=entities,
            continuationToken=continuation_token,
        )

    def CleanEntityStorage(self, request: pb.CleanEntityStorageRequest, context):
        """Clean entity storage: remove empty entities and release orphaned locks."""
        empty_removed = 0
        locks_released = 0

        with self._lock:
            if request.removeEmptyEntities:
                to_remove = [
                    eid for eid, ent in self._entities.items()
                    if ent.serialized_state is None and not ent.pending_operations
                ]
                for eid in to_remove:
                    del self._entities[eid]
                    self._entity_queue_set.discard(eid)
                empty_removed = len(to_remove)

            if request.releaseOrphanedLocks:
                for ent in self._entities.values():
                    if ent.locked_by and ent.locked_by not in self._instances:
                        ent.locked_by = None
                        locks_released += 1

        return pb.CleanEntityStorageResponse(
            emptyEntitiesRemoved=empty_removed,
            orphanedLocksReleased=locks_released,
        )

    def StreamInstanceHistory(self, request: pb.StreamInstanceHistoryRequest, context):
        """Streams instance history (not implemented)."""
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "StreamInstanceHistory not implemented")

    def CreateTaskHub(self, request: pb.CreateTaskHubRequest, context):
        """Creates task hub resources (no-op for in-memory)."""
        return pb.CreateTaskHubResponse()

    def DeleteTaskHub(self, request: pb.DeleteTaskHubRequest, context):
        """Deletes task hub resources (no-op for in-memory)."""
        return pb.DeleteTaskHubResponse()

    def RewindInstance(self, request: pb.RewindInstanceRequest, context):
        """Rewinds an orchestration instance (not implemented)."""
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "RewindInstance not implemented")

    def AbandonTaskActivityWorkItem(self, request: pb.AbandonActivityTaskRequest, context):
        """Abandons an activity work item."""
        return pb.AbandonActivityTaskResponse()

    def AbandonTaskOrchestratorWorkItem(self, request: pb.AbandonOrchestrationTaskRequest, context):
        """Abandons an orchestration work item."""
        return pb.AbandonOrchestrationTaskResponse()

    def AbandonTaskEntityWorkItem(self, request: pb.AbandonEntityTaskRequest, context):
        """Abandons an entity work item."""
        return pb.AbandonEntityTaskResponse()

    # Internal helper methods

    def _enqueue_orchestration(self, instance_id: str):
        """Enqueues an orchestration for processing."""
        if instance_id not in self._orchestration_queue_set:
            self._orchestration_queue.append(instance_id)
            self._orchestration_queue_set.add(instance_id)
            self._work_available.set()

    def _is_terminal_status(self, status: pb.OrchestrationStatus) -> bool:
        """Checks if a status is terminal."""
        return status in (
            pb.ORCHESTRATION_STATUS_COMPLETED,
            pb.ORCHESTRATION_STATUS_FAILED,
            pb.ORCHESTRATION_STATUS_TERMINATED
        )

    def _is_terminal_status_check(self, instance: OrchestrationInstance) -> bool:
        """Predicate to check if instance is in terminal status."""
        return self._is_terminal_status(instance.status)

    def _create_instance_internal(self, instance_id: str, name: str,
                                  encoded_input: Optional[str] = None):
        """Creates a new instance directly in internal state (no gRPC context needed)."""
        existing = self._instances.get(instance_id)
        if existing:
            if self._is_terminal_status(existing.status):
                # Allow recreation of terminated instances (e.g., retry)
                del self._instances[instance_id]
                self._orchestration_queue_set.discard(instance_id)
            else:
                raise ValueError(f"Orchestration instance '{instance_id}' already exists")

        now = datetime.now(timezone.utc)
        instance = OrchestrationInstance(
            instance_id=instance_id,
            name=name,
            status=pb.ORCHESTRATION_STATUS_PENDING,
            input=encoded_input,
            created_at=now,
            last_updated_at=now,
            completion_token=self._next_completion_token,
        )
        self._next_completion_token += 1

        orchestrator_started = helpers.new_orchestrator_started_event(now)
        execution_started = helpers.new_execution_started_event(name, instance_id, encoded_input)
        instance.pending_events.append(orchestrator_started)
        instance.pending_events.append(execution_started)

        self._instances[instance_id] = instance
        self._enqueue_orchestration(instance_id)

    def _raise_event_internal(self, instance_id: str, event_name: str,
                              event_data: Optional[str] = None):
        """Raises an event directly in internal state (no gRPC context needed)."""
        instance = self._instances.get(instance_id)
        if not instance:
            raise ValueError(f"Orchestration instance '{instance_id}' not found")

        event = helpers.new_event_raised_event(event_name, event_data)
        instance.pending_events.append(event)
        instance.last_updated_at = datetime.now(timezone.utc)
        self._enqueue_orchestration(instance.instance_id)

    def _terminate_instance_internal(self, instance_id: str, output: Optional[str],
                                     recursive: bool = False):
        """Internal method to terminate an instance."""
        if recursive:
            self._logger.warning(
                "Recursive termination is not supported in the in-memory backend")

        instance = self._instances.get(instance_id)
        if not instance:
            return

        if self._is_terminal_status(instance.status):
            return  # Already terminated

        event = helpers.new_terminated_event(encoded_output=output)
        instance.pending_events.append(event)
        instance.last_updated_at = datetime.now(timezone.utc)
        self._enqueue_orchestration(instance.instance_id)

        self._logger.info(f"Terminated instance '{instance_id}'")

    def _build_instance_response(self, instance: OrchestrationInstance,
                                 include_payloads: bool) -> pb.GetInstanceResponse:
        """Builds a GetInstanceResponse from an instance."""
        created_ts = timestamp_pb2.Timestamp()
        created_ts.FromDatetime(instance.created_at)

        updated_ts = timestamp_pb2.Timestamp()
        updated_ts.FromDatetime(instance.last_updated_at)

        state = pb.OrchestrationState(
            instanceId=instance.instance_id,
            name=instance.name,
            orchestrationStatus=instance.status,
            createdTimestamp=created_ts,
            lastUpdatedTimestamp=updated_ts,
            input=wrappers_pb2.StringValue(value=instance.input) if include_payloads and instance.input else None,
            output=wrappers_pb2.StringValue(value=instance.output) if include_payloads and instance.output else None,
            customStatus=wrappers_pb2.StringValue(value=instance.custom_status) if instance.custom_status else None,
            failureDetails=instance.failure_details if instance.failure_details else None,
        )

        return pb.GetInstanceResponse(exists=True, orchestrationState=state)

    def _wait_for_state(self, instance_id: str,
                        predicate: Callable[[OrchestrationInstance], bool],
                        timeout: Optional[float]) -> Optional[OrchestrationInstance]:
        """Waits for an orchestration to reach a state matching the predicate."""
        with self._lock:
            instance = self._instances.get(instance_id)
            if instance and predicate(instance):
                return instance

            waiter = StateWaiter(predicate=predicate)
            if instance_id not in self._state_waiters:
                self._state_waiters[instance_id] = []
            self._state_waiters[instance_id].append(waiter)

        # Wait outside the lock
        wait_result = waiter.event.wait(timeout=timeout if timeout else 30.0)

        if wait_result:
            return waiter.result
        else:
            # Timeout - remove waiter
            with self._lock:
                waiters = self._state_waiters.get(instance_id)
                if waiters and waiter in waiters:
                    waiters.remove(waiter)
                    if not waiters:
                        self._state_waiters.pop(instance_id, None)
            return None

    def _notify_waiters(self, instance_id: str):
        """Notifies all waiters for an instance."""
        instance = self._instances.get(instance_id)
        waiters = self._state_waiters.get(instance_id)

        if not waiters or not instance:
            return

        # Find and notify matching waiters
        matching_waiters = [w for w in waiters if w.predicate(instance)]
        for waiter in matching_waiters:
            waiter.result = instance
            waiter.event.set()

        # Remove notified waiters
        remaining = [w for w in waiters if w not in matching_waiters]
        if remaining:
            self._state_waiters[instance_id] = remaining
        else:
            self._state_waiters.pop(instance_id, None)

    def _process_action(self, instance: OrchestrationInstance, action: pb.OrchestratorAction):
        """Processes an orchestrator action."""
        if action.HasField("completeOrchestration"):
            self._process_complete_orchestration_action(instance, action.completeOrchestration)
        elif action.HasField("scheduleTask"):
            self._process_schedule_task_action(instance, action)
        elif action.HasField("createTimer"):
            self._process_create_timer_action(instance, action)
        elif action.HasField("createSubOrchestration"):
            self._process_create_sub_orchestration_action(instance, action)
        elif action.HasField("sendEvent"):
            self._process_send_event_action(action.sendEvent)
        elif action.HasField("sendEntityMessage"):
            self._process_send_entity_message_action(instance, action)

    def _process_complete_orchestration_action(self, instance: OrchestrationInstance,
                                               complete_action: pb.CompleteOrchestrationAction):
        """Processes a complete orchestration action."""
        status = complete_action.orchestrationStatus
        instance.status = status
        instance.output = complete_action.result.value if complete_action.result else None
        instance.failure_details = complete_action.failureDetails if complete_action.failureDetails else None

        if status == pb.ORCHESTRATION_STATUS_CONTINUED_AS_NEW:
            # Handle continue-as-new
            new_input = complete_action.result.value if complete_action.result else None
            carryover_events = list(complete_action.carryoverEvents)

            # Reset instance state
            instance.history.clear()
            instance.input = new_input
            instance.output = None
            instance.failure_details = None
            instance.status = pb.ORCHESTRATION_STATUS_PENDING

            # Save any events that arrived during the in-flight dispatch so
            # they can be appended AFTER the new execution started events.
            new_arrivals = list(instance.pending_events)
            instance.pending_events.clear()

            # Build the new pending events in the correct order:
            # OrchestratorStarted, ExecutionStarted, carryover, new arrivals
            now = datetime.now(timezone.utc)
            orchestrator_started = helpers.new_orchestrator_started_event(now)
            execution_started = helpers.new_execution_started_event(
                instance.name, instance.instance_id, new_input
            )
            instance.pending_events.append(orchestrator_started)
            instance.pending_events.append(execution_started)
            instance.pending_events.extend(carryover_events)
            instance.pending_events.extend(new_arrivals)

            self._enqueue_orchestration(instance.instance_id)

    def _process_schedule_task_action(self, instance: OrchestrationInstance,
                                      action: pb.OrchestratorAction):
        """Processes a schedule task action."""
        schedule_task = action.scheduleTask
        task_id = action.id
        task_name = schedule_task.name
        input_value = schedule_task.input.value if schedule_task.input else None

        # Add TaskScheduled event to history
        event = helpers.new_task_scheduled_event(task_id, task_name, input_value)
        instance.history.append(event)

        # Mark instance as running
        if instance.status == pb.ORCHESTRATION_STATUS_PENDING:
            instance.status = pb.ORCHESTRATION_STATUS_RUNNING

        # Queue activity for execution
        self._activity_queue.append(ActivityWorkItem(
            instance_id=instance.instance_id,
            name=task_name,
            task_id=task_id,
            input=input_value,
            completion_token=instance.completion_token
        ))
        self._work_available.set()

    def _process_create_timer_action(self, instance: OrchestrationInstance,
                                     action: pb.OrchestratorAction):
        """Processes a create timer action."""
        create_timer = action.createTimer
        timer_id = action.id
        fire_at = create_timer.fireAt.ToDatetime(tzinfo=timezone.utc)

        # Add TimerCreated event to history
        timer_created_event = helpers.new_timer_created_event(timer_id, fire_at)
        instance.history.append(timer_created_event)

        # Mark instance as running
        if instance.status == pb.ORCHESTRATION_STATUS_PENDING:
            instance.status = pb.ORCHESTRATION_STATUS_RUNNING

        # Schedule timer firing
        now = datetime.now(timezone.utc)
        delay = max(0, (fire_at - now).total_seconds())

        def fire_timer():
            time.sleep(delay)
            with self._lock:
                current_instance = self._instances.get(instance.instance_id)
                if current_instance and not self._is_terminal_status(current_instance.status):
                    timer_fired_event = helpers.new_timer_fired_event(timer_id, fire_at)
                    current_instance.pending_events.append(timer_fired_event)
                    current_instance.last_updated_at = datetime.now(timezone.utc)
                    self._enqueue_orchestration(instance.instance_id)

        timer_thread = threading.Thread(target=fire_timer, daemon=True)
        timer_thread.start()

    def _process_create_sub_orchestration_action(self, instance: OrchestrationInstance,
                                                 action: pb.OrchestratorAction):
        """Processes a create sub-orchestration action."""
        create_sub_orch = action.createSubOrchestration
        task_id = action.id
        name = create_sub_orch.name
        sub_instance_id = create_sub_orch.instanceId
        input_value = create_sub_orch.input.value if create_sub_orch.input else None

        # Add SubOrchestrationInstanceCreated event to history
        event = helpers.new_sub_orchestration_created_event(task_id, name, sub_instance_id, input_value)
        instance.history.append(event)

        # Mark instance as running
        if instance.status == pb.ORCHESTRATION_STATUS_PENDING:
            instance.status = pb.ORCHESTRATION_STATUS_RUNNING

        # Create the sub-orchestration directly via internal state
        try:
            self._create_instance_internal(sub_instance_id, name, input_value)

            # Watch for sub-orchestration completion
            self._watch_sub_orchestration(instance.instance_id, sub_instance_id, task_id)
        except Exception as ex:
            # Sub-orchestration creation failed
            failure_event = helpers.new_sub_orchestration_failed_event(task_id, ex)
            instance.pending_events.append(failure_event)
            self._enqueue_orchestration(instance.instance_id)

    def _watch_sub_orchestration(self, parent_instance_id: str, sub_instance_id: str, task_id: int):
        """Watches a sub-orchestration for completion and delivers the result to the parent."""
        def watch():
            # Wait for sub-orchestration to complete
            sub_instance = self._wait_for_state(
                sub_instance_id,
                self._is_terminal_status_check,
                timeout=None  # No timeout
            )

            with self._lock:
                parent_instance = self._instances.get(parent_instance_id)

                if not sub_instance or not parent_instance:
                    return

                # If parent already terminated, don't deliver the completion event
                if self._is_terminal_status(parent_instance.status):
                    return

                # Deliver the sub-orchestration completion/failure event to parent
                if sub_instance.status == pb.ORCHESTRATION_STATUS_COMPLETED:
                    event = helpers.new_sub_orchestration_completed_event(task_id, sub_instance.output)
                else:
                    error_msg = (sub_instance.failure_details.errorMessage
                                 if sub_instance.failure_details else "Sub-orchestration failed")
                    event = helpers.new_sub_orchestration_failed_event(task_id, Exception(error_msg))

                parent_instance.pending_events.append(event)
                parent_instance.last_updated_at = datetime.now(timezone.utc)
                self._enqueue_orchestration(parent_instance_id)

        watcher_thread = threading.Thread(target=watch, daemon=True)
        watcher_thread.start()

    def _process_send_event_action(self, send_event: pb.SendEventAction):
        """Processes a send event action."""
        target_instance_id = send_event.instance.instanceId if send_event.instance else None
        event_name = send_event.name
        event_data = send_event.data.value if send_event.data else None

        if target_instance_id:
            try:
                self._raise_event_internal(target_instance_id, event_name, event_data)
            except Exception:
                # Target instance may not exist - ignore
                pass

    def _process_send_entity_message_action(self, instance: OrchestrationInstance,
                                            action: pb.OrchestratorAction):
        """Processes a send entity message action from an orchestrator."""
        msg = action.sendEntityMessage
        action_id = action.id

        if msg.HasField("entityOperationSignaled"):
            signaled = msg.entityOperationSignaled
            target_id = signaled.targetInstanceId.value if signaled.targetInstanceId else None

            # Add confirmation event to orchestration history
            history_event = pb.HistoryEvent(
                eventId=action_id,
                timestamp=timestamp_pb2.Timestamp(),
                entityOperationSignaled=signaled,
            )
            instance.history.append(history_event)

            if target_id:
                self._queue_entity_operation(target_id, pb.HistoryEvent(
                    eventId=-1,
                    timestamp=timestamp_pb2.Timestamp(),
                    entityOperationSignaled=signaled,
                ))

        elif msg.HasField("entityOperationCalled"):
            called = msg.entityOperationCalled
            target_id = called.targetInstanceId.value if called.targetInstanceId else None

            # Add confirmation event to orchestration history
            history_event = pb.HistoryEvent(
                eventId=action_id,
                timestamp=timestamp_pb2.Timestamp(),
                entityOperationCalled=called,
            )
            instance.history.append(history_event)

            # Mark instance as running
            if instance.status == pb.ORCHESTRATION_STATUS_PENDING:
                instance.status = pb.ORCHESTRATION_STATUS_RUNNING

            if target_id:
                self._queue_entity_operation(target_id, pb.HistoryEvent(
                    eventId=-1,
                    timestamp=timestamp_pb2.Timestamp(),
                    entityOperationCalled=called,
                ))

        elif msg.HasField("entityLockRequested"):
            lock_req = msg.entityLockRequested
            parent_id = lock_req.parentInstanceId.value if lock_req.parentInstanceId else None

            # Add confirmation event to orchestration history
            history_event = pb.HistoryEvent(
                eventId=action_id,
                timestamp=timestamp_pb2.Timestamp(),
                entityLockRequested=lock_req,
            )
            instance.history.append(history_event)

            # Mark instance as running
            if instance.status == pb.ORCHESTRATION_STATUS_PENDING:
                instance.status = pb.ORCHESTRATION_STATUS_RUNNING

            if parent_id:
                lock_set = list(lock_req.lockSet)
                pending = PendingLockRequest(
                    critical_section_id=lock_req.criticalSectionId,
                    parent_instance_id=parent_id,
                    lock_set=lock_set,
                )
                self._try_grant_lock(pending)

        elif msg.HasField("entityUnlockSent"):
            unlock = msg.entityUnlockSent
            target_id = unlock.targetInstanceId.value if unlock.targetInstanceId else None

            # Add confirmation event to orchestration history
            history_event = pb.HistoryEvent(
                eventId=action_id,
                timestamp=timestamp_pb2.Timestamp(),
                entityUnlockSent=unlock,
            )
            instance.history.append(history_event)

            if target_id:
                entity = self._entities.get(target_id)
                if entity and entity.locked_by == unlock.criticalSectionId:
                    entity.locked_by = None

            # Try to grant any pending lock requests
            self._try_grant_pending_locks()

    def _can_grant_lock(self, pending: PendingLockRequest) -> bool:
        """Checks if all entities in the lock set are available for locking."""
        for entity_id in pending.lock_set:
            entity = self._entities.get(entity_id)
            if entity and entity.locked_by is not None:
                return False
        return True

    def _grant_lock(self, pending: PendingLockRequest):
        """Grants a lock to entities and notifies the parent orchestration.

        Assumes lock availability has already been verified via _can_grant_lock.
        """
        for entity_id in pending.lock_set:
            entity = self._entities.get(entity_id)
            if not entity:
                entity = EntityState(
                    instance_id=entity_id,
                    completion_token=self._next_completion_token,
                )
                self._next_completion_token += 1
                self._entities[entity_id] = entity
            entity.locked_by = pending.critical_section_id

        parent = self._instances.get(pending.parent_instance_id)
        if parent:
            grant_event = pb.HistoryEvent(
                eventId=-1,
                timestamp=timestamp_pb2.Timestamp(),
                entityLockGranted=pb.EntityLockGrantedEvent(
                    criticalSectionId=pending.critical_section_id,
                ),
            )
            parent.pending_events.append(grant_event)
            parent.last_updated_at = datetime.now(timezone.utc)
            self._enqueue_orchestration(pending.parent_instance_id)

    def _try_grant_lock(self, pending: PendingLockRequest) -> bool:
        """Tries to grant a lock request. Returns True if granted, False if queued."""
        if not self._can_grant_lock(pending):
            self._pending_lock_requests.append(pending)
            return False
        self._grant_lock(pending)
        return True

    def _try_grant_pending_locks(self):
        """Attempts to grant any pending lock requests that can now be fulfilled."""
        still_pending = []
        for pending in self._pending_lock_requests:
            if self._can_grant_lock(pending):
                self._grant_lock(pending)
            else:
                still_pending.append(pending)
        self._pending_lock_requests = still_pending

    def _queue_entity_operation(self, entity_id: str, event: pb.HistoryEvent):
        """Queues an operation event for an entity."""
        entity = self._entities.get(entity_id)
        if not entity:
            entity = EntityState(
                instance_id=entity_id,
                completion_token=self._next_completion_token,
            )
            self._next_completion_token += 1
            self._entities[entity_id] = entity

        entity.pending_operations.append(event)
        self._enqueue_entity(entity_id)

    def _signal_entity_internal(self, entity_id: str, operation: str,
                                input_value: Optional[str] = None):
        """Internal method to signal an entity (from entity side-effect actions)."""
        event = pb.HistoryEvent(
            eventId=-1,
            timestamp=timestamp_pb2.Timestamp(),
            entityOperationSignaled=pb.EntityOperationSignaledEvent(
                requestId=uuid.uuid4().hex,
                operation=operation,
                input=wrappers_pb2.StringValue(value=input_value) if input_value else None,
                targetInstanceId=wrappers_pb2.StringValue(value=entity_id),
            )
        )
        self._queue_entity_operation(entity_id, event)

    def _enqueue_entity(self, entity_id: str):
        """Enqueues an entity for processing."""
        if entity_id not in self._entity_queue_set:
            self._entity_queue.append(entity_id)
            self._entity_queue_set.add(entity_id)
            self._work_available.set()


def create_test_backend(port: int = 50051, max_history_size: int = 10000) -> InMemoryOrchestrationBackend:
    """
    Factory function to create and start an in-memory backend for testing.

    Args:
        port: Port to listen on for gRPC connections (default 50051)
        max_history_size: Maximum number of history events per orchestration (default 10000)

    Returns:
        A started InMemoryOrchestrationBackend instance

    Example:
        ```python
        import pytest
        from durabletask.testing.in_memory_backend import create_test_backend
        from durabletask import TaskHubGrpcClient, TaskHubGrpcWorker

        @pytest.fixture
        def backend():
            backend = create_test_backend(port=50051)
            yield backend
            backend.stop()
            backend.reset()

        def test_orchestration(backend):
            # Create client connected to the test backend
            client = TaskHubGrpcClient(host_address="localhost:50051")

            # Create worker connected to the test backend
            worker = TaskHubGrpcWorker(host_address="localhost:50051")

            # Register orchestrators and activities
            @worker.orchestrator()
            def my_orchestrator(ctx):
                result = yield ctx.call_activity(my_activity, input="hello")
                return result

            @worker.activity()
            def my_activity(ctx, input: str):
                return f"processed: {input}"

            # Start the worker
            worker.start()

            try:
                # Schedule and wait for orchestration
                instance_id = client.schedule_new_orchestration(my_orchestrator, input=None)
                state = client.wait_for_orchestration_completion(instance_id, timeout=10)

                assert state.runtime_status == OrchestrationStatus.COMPLETED
                # Add more assertions...
            finally:
                worker.stop()
        ```
    """
    backend = InMemoryOrchestrationBackend(max_history_size=max_history_size, port=port)
    backend.start()
    return backend
