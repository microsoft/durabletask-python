# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import asyncio
import inspect
import json
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Event, Thread
from types import GeneratorType
from enum import Enum
from typing import Any, Generator, Optional, Sequence, Tuple, TypeVar, Union
import uuid
from packaging.version import InvalidVersion, parse

import grpc
from google.protobuf import empty_pb2

from durabletask.entities.entity_operation_failed_exception import EntityOperationFailedException
from durabletask.internal import helpers
from durabletask.internal.entity_state_shim import StateShim
from durabletask.internal.helpers import new_timestamp
from durabletask.entities import DurableEntity, EntityLock, EntityInstanceId, EntityContext
from durabletask.internal.json_encode_output_exception import JsonEncodeOutputException
from durabletask.internal.orchestration_entity_context import OrchestrationEntityContext
from durabletask.internal.proto_task_hub_sidecar_service_stub import ProtoTaskHubSidecarServiceStub
import durabletask.internal.helpers as ph
import durabletask.internal.exceptions as pe
import durabletask.internal.orchestrator_service_pb2 as pb
import durabletask.internal.orchestrator_service_pb2_grpc as stubs
import durabletask.internal.shared as shared
import durabletask.internal.tracing as tracing
from durabletask import task
from durabletask.internal.grpc_interceptor import DefaultClientInterceptorImpl

TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")
DATETIME_STRING_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
DEFAULT_MAXIMUM_TIMER_INTERVAL = timedelta(days=3)


class ConcurrencyOptions:
    """Configuration options for controlling concurrency of different work item types and the thread pool size.

    This class provides fine-grained control over concurrent processing limits for
    activities, orchestrations and the thread pool size.
    """

    def __init__(
            self,
            maximum_concurrent_activity_work_items: Optional[int] = None,
            maximum_concurrent_orchestration_work_items: Optional[int] = None,
            maximum_concurrent_entity_work_items: Optional[int] = None,
            maximum_thread_pool_workers: Optional[int] = None,
    ):
        """Initialize concurrency options.

        Args:
            maximum_concurrent_activity_work_items: Maximum number of activity work items
                that can be processed concurrently. Defaults to 100 * processor_count.
            maximum_concurrent_orchestration_work_items: Maximum number of orchestration work items
                that can be processed concurrently. Defaults to 100 * processor_count.
            maximum_thread_pool_workers: Maximum number of thread pool workers to use.
        """
        processor_count = os.cpu_count() or 1
        default_concurrency = 100 * processor_count
        # see https://docs.python.org/3/library/concurrent.futures.html
        default_max_workers = processor_count + 4

        self.maximum_concurrent_activity_work_items = (
            maximum_concurrent_activity_work_items
            if maximum_concurrent_activity_work_items is not None
            else default_concurrency
        )

        self.maximum_concurrent_orchestration_work_items = (
            maximum_concurrent_orchestration_work_items
            if maximum_concurrent_orchestration_work_items is not None
            else default_concurrency
        )

        self.maximum_concurrent_entity_work_items = (
            maximum_concurrent_entity_work_items
            if maximum_concurrent_entity_work_items is not None
            else default_concurrency
        )

        self.maximum_thread_pool_workers = (
            maximum_thread_pool_workers
            if maximum_thread_pool_workers is not None
            else default_max_workers
        )


class VersionMatchStrategy(Enum):
    """Enumeration for version matching strategies."""

    NONE = 1
    STRICT = 2
    CURRENT_OR_OLDER = 3


class VersionFailureStrategy(Enum):
    """Enumeration for version failure strategies."""

    REJECT = 1
    FAIL = 2


class VersioningOptions:
    """Configuration options for orchestrator and activity versioning.

    This class provides options to control how versioning is handled for orchestrators
    and activities, including whether to use the default version and how to compare versions.
    """

    version: Optional[str] = None
    default_version: Optional[str] = None
    match_strategy: Optional[VersionMatchStrategy] = None
    failure_strategy: Optional[VersionFailureStrategy] = None

    def __init__(self, version: Optional[str] = None,
                 default_version: Optional[str] = None,
                 match_strategy: Optional[VersionMatchStrategy] = None,
                 failure_strategy: Optional[VersionFailureStrategy] = None
                 ):
        """Initialize versioning options.

        Args:
            version: The version of orchestrations that the worker can work on.
            default_version: The default version that will be used for starting new sub-orchestrations.
            match_strategy: The versioning strategy for the Durable Task worker.
            failure_strategy: The versioning failure strategy for the Durable Task worker.
        """
        self.version = version
        self.default_version = default_version
        self.match_strategy = match_strategy
        self.failure_strategy = failure_strategy


class _Registry:
    orchestrators: dict[str, task.Orchestrator]
    activities: dict[str, task.Activity]
    entities: dict[str, task.Entity]
    versioning: Optional[VersioningOptions] = None

    def __init__(self):
        self.orchestrators = {}
        self.activities = {}
        self.entities = {}

    def add_orchestrator(self, fn: task.Orchestrator[TInput, TOutput]) -> str:
        if fn is None:
            raise ValueError("An orchestrator function argument is required.")

        name = task.get_name(fn)
        self.add_named_orchestrator(name, fn)
        return name

    def add_named_orchestrator(self, name: str, fn: task.Orchestrator[TInput, TOutput]) -> None:
        if not name:
            raise ValueError("A non-empty orchestrator name is required.")
        if name in self.orchestrators:
            raise ValueError(f"A '{name}' orchestrator already exists.")

        self.orchestrators[name] = fn

    def get_orchestrator(self, name: str) -> Optional[task.Orchestrator[Any, Any]]:
        return self.orchestrators.get(name)

    def add_activity(self, fn: task.Activity[TInput, TOutput]) -> str:
        if fn is None:
            raise ValueError("An activity function argument is required.")

        name = task.get_name(fn)
        self.add_named_activity(name, fn)
        return name

    def add_named_activity(self, name: str, fn: task.Activity[TInput, TOutput]) -> None:
        if not name:
            raise ValueError("A non-empty activity name is required.")
        if name in self.activities:
            raise ValueError(f"A '{name}' activity already exists.")

        self.activities[name] = fn

    def get_activity(self, name: str) -> Optional[task.Activity[Any, Any]]:
        return self.activities.get(name)

    def add_entity(self, fn: task.Entity, name: Optional[str] = None) -> str:
        if fn is None:
            raise ValueError("An entity function argument is required.")

        if name is None:
            name = task.get_entity_name(fn)

        self.add_named_entity(name, fn)
        return name

    def add_named_entity(self, name: str, fn: task.Entity) -> None:
        name = name.lower()
        EntityInstanceId.validate_entity_name(name)
        if name in self.entities:
            raise ValueError(f"A '{name}' entity already exists.")

        self.entities[name] = fn

    def get_entity(self, name: str) -> Optional[task.Entity]:
        return self.entities.get(name)


class OrchestratorNotRegisteredError(ValueError):
    """Raised when attempting to start an orchestration that is not registered"""

    pass


class ActivityNotRegisteredError(ValueError):
    """Raised when attempting to call an activity that is not registered"""

    pass


class EntityNotRegisteredError(ValueError):
    """Raised when attempting to call an entity that is not registered"""

    pass


class TaskHubGrpcWorker:
    """A gRPC-based worker for processing durable task orchestrations and activities.

    This worker connects to a Durable Task backend service via gRPC to receive and process
    work items including orchestration functions and activity functions. It provides
    concurrent execution capabilities with configurable limits and automatic retry handling.

    The worker manages the complete lifecycle:
    - Registers orchestrator and activity functions
    - Connects to the gRPC backend service
    - Receives work items and executes them concurrently
    - Handles failures, retries, and state management
    - Provides logging and monitoring capabilities

    Args:
        host_address (Optional[str], optional): The gRPC endpoint address of the backend service.
            Defaults to the value from environment variables or localhost.
        metadata (Optional[list[tuple[str, str]]], optional): gRPC metadata to include with
            requests. Used for authentication and routing. Defaults to None.
        log_handler (optional[logging.Handler]): Custom logging handler for worker logs. Defaults to None.
        log_formatter (Optional[logging.Formatter], optional): Custom log formatter.
            Defaults to None.
        secure_channel (bool, optional): Whether to use a secure gRPC channel (TLS).
            Defaults to False.
        interceptors (Optional[Sequence[shared.ClientInterceptor]], optional): Custom gRPC
            interceptors to apply to the channel. Defaults to None.
        concurrency_options (Optional[ConcurrencyOptions], optional): Configuration for
            controlling worker concurrency limits. If None, default settings are used.

    Attributes:
        concurrency_options (ConcurrencyOptions): The current concurrency configuration.

    Example:
        Basic worker setup:

        >>> from durabletask.worker import TaskHubGrpcWorker, ConcurrencyOptions
        >>>
        >>> # Create worker with custom concurrency settings
        >>> concurrency = ConcurrencyOptions(
        ...     maximum_concurrent_activity_work_items=50,
        ...     maximum_concurrent_orchestration_work_items=20
        ... )
        >>> worker = TaskHubGrpcWorker(
        ...     host_address="localhost:4001",
        ...     concurrency_options=concurrency
        ... )
        >>>
        >>> # Register functions
        >>> @worker.add_orchestrator
        ... def my_orchestrator(context, input):
        ...     result = yield context.call_activity("my_activity", input="hello")
        ...     return result
        >>>
        >>> @worker.add_activity
        ... def my_activity(context, input):
        ...     return f"Processed: {input}"
        >>>
        >>> # Start the worker
        >>> worker.start()
        >>> # ... worker runs in background thread
        >>> worker.stop()

        Using as context manager:

        >>> with TaskHubGrpcWorker() as worker:
        ...     worker.add_orchestrator(my_orchestrator)
        ...     worker.add_activity(my_activity)
        ...     worker.start()
        ...     # Worker automatically stops when exiting context

    Raises:
        RuntimeError: If attempting to add orchestrators/activities while the worker is running,
            or if starting a worker that is already running.
        OrchestratorNotRegisteredError: If an orchestration work item references an
            unregistered orchestrator function.
        ActivityNotRegisteredError: If an activity work item references an unregistered
            activity function.
    """

    _response_stream: Optional[Any] = None
    _interceptors: Optional[list[shared.ClientInterceptor]] = None

    def __init__(
            self,
            *,
            host_address: Optional[str] = None,
            metadata: Optional[list[tuple[str, str]]] = None,
            log_handler: Optional[logging.Handler] = None,
            log_formatter: Optional[logging.Formatter] = None,
            secure_channel: bool = False,
            interceptors: Optional[Sequence[shared.ClientInterceptor]] = None,
            concurrency_options: Optional[ConcurrencyOptions] = None,
            maximum_timer_interval: Optional[timedelta] = DEFAULT_MAXIMUM_TIMER_INTERVAL
    ):
        self._registry = _Registry()
        self._host_address = (
            host_address if host_address else shared.get_default_host_address()
        )
        self._logger = shared.get_logger("worker", log_handler, log_formatter)
        self._shutdown = Event()
        self._is_running = False
        self._secure_channel = secure_channel

        # Use provided concurrency options or create default ones
        self._concurrency_options = (
            concurrency_options
            if concurrency_options is not None
            else ConcurrencyOptions()
        )

        # Determine the interceptors to use
        if interceptors is not None:
            self._interceptors = list(interceptors)
            if metadata:
                self._interceptors.append(DefaultClientInterceptorImpl(metadata))
        elif metadata:
            self._interceptors = [DefaultClientInterceptorImpl(metadata)]
        else:
            self._interceptors = None

        self._async_worker_manager = _AsyncWorkerManager(self._concurrency_options, self._logger)
        self._maximum_timer_interval = maximum_timer_interval

    @property
    def concurrency_options(self) -> ConcurrencyOptions:
        """Get the current concurrency options for this worker."""
        return self._concurrency_options

    @property
    def maximum_timer_interval(self) -> Optional[timedelta]:
        """Get the configured maximum timer interval for long timer chunking."""
        return self._maximum_timer_interval

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def add_orchestrator(self, fn: task.Orchestrator[TInput, TOutput]) -> str:
        """Registers an orchestrator function with the worker."""
        if self._is_running:
            raise RuntimeError(
                "Orchestrators cannot be added while the worker is running."
            )
        return self._registry.add_orchestrator(fn)

    def add_activity(self, fn: task.Activity) -> str:
        """Registers an activity function with the worker."""
        if self._is_running:
            raise RuntimeError(
                "Activities cannot be added while the worker is running."
            )
        return self._registry.add_activity(fn)

    def add_entity(self, fn: task.Entity, name: Optional[str] = None) -> str:
        """Registers an entity function with the worker."""
        if self._is_running:
            raise RuntimeError(
                "Entities cannot be added while the worker is running."
            )
        return self._registry.add_entity(fn, name)

    def use_versioning(self, version: VersioningOptions) -> None:
        """Initializes versioning options for sub-orchestrators and activities."""
        if self._is_running:
            raise RuntimeError("Cannot set default version while the worker is running.")
        self._registry.versioning = version

    def start(self):
        """Starts the worker on a background thread and begins listening for work items."""
        if self._is_running:
            raise RuntimeError("The worker is already running.")

        def run_loop():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._async_run_loop())

        self._logger.info(f"Starting gRPC worker that connects to {self._host_address}")
        self._runLoop = Thread(target=run_loop)
        self._runLoop.start()
        self._is_running = True

    async def _async_run_loop(self):
        worker_task = asyncio.create_task(self._async_worker_manager.run())
        # Connection state management for retry fix
        current_channel = None
        current_stub = None
        current_reader_thread = None
        conn_retry_count = 0
        conn_max_retry_delay = 60

        def create_fresh_connection():
            nonlocal current_channel, current_stub, conn_retry_count
            if current_channel:
                try:
                    current_channel.close()
                except Exception:
                    pass
            current_channel = None
            current_stub = None
            try:
                current_channel = shared.get_grpc_channel(
                    self._host_address, self._secure_channel, self._interceptors
                )
                current_stub = stubs.TaskHubSidecarServiceStub(current_channel)
                current_stub.Hello(empty_pb2.Empty())
                conn_retry_count = 0
                self._logger.info(f"Created fresh connection to {self._host_address}")
            except Exception as e:
                self._logger.warning(f"Failed to create connection: {e}")
                current_channel = None
                current_stub = None
                raise

        def invalidate_connection():
            nonlocal current_channel, current_stub, current_reader_thread
            # Cancel the response stream first to signal the reader thread to stop
            if self._response_stream is not None:
                try:
                    self._response_stream.cancel()
                except Exception:
                    pass
                self._response_stream = None

            # Wait for the reader thread to finish
            if current_reader_thread is not None:
                try:
                    current_reader_thread.join(timeout=2)
                    if current_reader_thread.is_alive():
                        self._logger.warning("Stream reader thread did not shut down gracefully")
                except Exception:
                    pass
                current_reader_thread = None

            # Close the channel
            if current_channel:
                try:
                    current_channel.close()
                except Exception:
                    pass
            current_channel = None
            current_stub = None

        def should_invalidate_connection(rpc_error):
            error_code = rpc_error.code()  # type: ignore
            connection_level_errors = {
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.CANCELLED,
                grpc.StatusCode.UNAUTHENTICATED,
                grpc.StatusCode.ABORTED,
            }
            return error_code in connection_level_errors

        while not self._shutdown.is_set():
            if current_stub is None:
                try:
                    create_fresh_connection()
                except Exception:
                    conn_retry_count += 1
                    delay = min(
                        conn_max_retry_delay,
                        (2 ** min(conn_retry_count, 6)) + random.uniform(0, 1),
                    )
                    self._logger.warning(
                        f"Connection failed, retrying in {delay:.2f} seconds (attempt {conn_retry_count})"
                    )
                    if self._shutdown.wait(delay):
                        break
                    continue
            try:
                assert current_stub is not None
                stub = current_stub
                get_work_items_request = pb.GetWorkItemsRequest(
                    maxConcurrentOrchestrationWorkItems=self._concurrency_options.maximum_concurrent_orchestration_work_items,
                    maxConcurrentActivityWorkItems=self._concurrency_options.maximum_concurrent_activity_work_items,
                )
                self._response_stream = stub.GetWorkItems(get_work_items_request)
                self._logger.info(
                    f"Successfully connected to {self._host_address}. Waiting for work items..."
                )

                # Use a thread to read from the blocking gRPC stream and forward to asyncio
                import queue

                work_item_queue = queue.Queue()

                def stream_reader():
                    try:
                        response_stream = self._response_stream
                        if response_stream is None:
                            return

                        for work_item in response_stream:
                            work_item_queue.put(work_item)
                    except Exception as e:
                        work_item_queue.put(e)

                import threading

                current_reader_thread = threading.Thread(target=stream_reader, daemon=True)
                current_reader_thread.start()
                loop = asyncio.get_running_loop()
                while not self._shutdown.is_set():
                    try:
                        work_item = await loop.run_in_executor(
                            None, work_item_queue.get
                        )
                        if isinstance(work_item, Exception):
                            raise work_item
                        request_type = work_item.WhichOneof("request")
                        self._logger.debug(f'Received "{request_type}" work item')
                        if work_item.HasField("orchestratorRequest"):
                            self._async_worker_manager.submit_orchestration(
                                self._execute_orchestrator,
                                self._cancel_orchestrator,
                                work_item.orchestratorRequest,
                                stub,
                                work_item.completionToken,
                            )
                        elif work_item.HasField("activityRequest"):
                            self._async_worker_manager.submit_activity(
                                self._execute_activity,
                                self._cancel_activity,
                                work_item.activityRequest,
                                stub,
                                work_item.completionToken,
                            )
                        elif work_item.HasField("entityRequest"):
                            self._async_worker_manager.submit_entity_batch(
                                self._execute_entity_batch,
                                self._cancel_entity_batch,
                                work_item.entityRequest,
                                stub,
                                work_item.completionToken,
                            )
                        elif work_item.HasField("entityRequestV2"):
                            self._async_worker_manager.submit_entity_batch(
                                self._execute_entity_batch,
                                self._cancel_entity_batch,
                                work_item.entityRequestV2,
                                stub,
                                work_item.completionToken
                            )
                        elif work_item.HasField("healthPing"):
                            pass
                        else:
                            self._logger.warning(
                                f"Unexpected work item type: {request_type}"
                            )
                    except Exception as e:
                        self._logger.warning(f"Error in work item stream: {e}")
                        raise e
                current_reader_thread.join(timeout=1)
                self._logger.info("Work item stream ended normally")
            except grpc.RpcError as rpc_error:
                should_invalidate = should_invalidate_connection(rpc_error)
                if should_invalidate:
                    invalidate_connection()
                error_code = rpc_error.code()  # type: ignore
                error_details = str(rpc_error)

                if error_code == grpc.StatusCode.CANCELLED:
                    self._logger.info(f"Disconnected from {self._host_address}")
                    break
                elif error_code == grpc.StatusCode.UNAVAILABLE:
                    # Check if this is a connection timeout scenario
                    if "Timeout occurred" in error_details or "Failed to connect to remote host" in error_details:
                        self._logger.warning(
                            f"Connection timeout to {self._host_address}: {error_details} - will retry with fresh connection"
                        )
                    else:
                        self._logger.warning(
                            f"The sidecar at address {self._host_address} is unavailable: {error_details} - will continue retrying"
                        )
                elif should_invalidate:
                    self._logger.warning(
                        f"Connection-level gRPC error ({error_code}): {rpc_error} - resetting connection"
                    )
                else:
                    self._logger.warning(
                        f"Application-level gRPC error ({error_code}): {rpc_error}"
                    )
                self._shutdown.wait(1)
            except Exception as ex:
                invalidate_connection()
                self._logger.warning(f"Unexpected error: {ex}")
                self._shutdown.wait(1)
        invalidate_connection()
        self._logger.info("No longer listening for work items")
        self._async_worker_manager.shutdown()
        await worker_task

    def stop(self):
        """Stops the worker and waits for any pending work items to complete."""
        if not self._is_running:
            return

        self._logger.info("Stopping gRPC worker...")
        self._shutdown.set()
        if self._response_stream is not None:
            self._response_stream.cancel()
        if self._runLoop is not None:
            self._runLoop.join(timeout=30)
        self._async_worker_manager.shutdown()
        self._logger.info("Worker shutdown completed")
        self._is_running = False

    def _execute_orchestrator(
            self,
            req: pb.OrchestratorRequest,
            stub: Union[stubs.TaskHubSidecarServiceStub, ProtoTaskHubSidecarServiceStub],
            completionToken,
    ):
        instance_id = req.instanceId

        # Extract parent trace context from executionStarted event
        parent_trace_ctx = None
        orchestration_name = "<unknown>"
        for e in list(req.pastEvents) + list(req.newEvents):
            if e.HasField("executionStarted"):
                orchestration_name = e.executionStarted.name
                if e.executionStarted.HasField("parentTraceContext"):
                    parent_trace_ctx = e.executionStarted.parentTraceContext
                break

        # Determine the orchestration start time: reuse persisted value
        # from a prior dispatch, or capture a new one.
        if (req.HasField("orchestrationTraceContext") and req.orchestrationTraceContext.HasField("spanStartTime")):
            start_time_ns = req.orchestrationTraceContext.spanStartTime.ToNanoseconds()
        else:
            start_time_ns = time.time_ns()

        # Extract persisted orchestration span ID from a prior dispatch
        persisted_orch_span_id = None
        if (req.HasField("orchestrationTraceContext") and req.orchestrationTraceContext.HasField("spanID") and req.orchestrationTraceContext.spanID.value):
            persisted_orch_span_id = req.orchestrationTraceContext.spanID.value

        try:
            executor = _OrchestrationExecutor(
                self._registry, self._logger,
                persisted_orch_span_id=persisted_orch_span_id,
                maximum_timer_interval=self.maximum_timer_interval)
            result = executor.execute(instance_id, req.pastEvents, req.newEvents)

            # Determine completion status for span
            is_complete = False
            is_failed = False
            failure_details = None
            for action in result.actions:
                if action.HasField("completeOrchestration"):
                    is_complete = True
                    orch_status = action.completeOrchestration.orchestrationStatus
                    if orch_status == pb.ORCHESTRATION_STATUS_FAILED:
                        is_failed = True
                        failure_details = action.completeOrchestration.failureDetails

            if is_complete:
                # Orchestration finished — emit a single span covering its lifetime
                tracing.emit_orchestration_span(
                    orchestration_name,
                    instance_id,
                    start_time_ns,
                    is_failed,
                    failure_details=failure_details,
                    parent_trace_context=parent_trace_ctx,
                    orchestration_trace_context=result._orchestration_trace_context,
                )

            # Include the span ID in the orchestration trace context
            # so it persists across dispatches.
            orch_span_id = None
            if result._orchestration_trace_context:
                orch_span_id = result._orchestration_trace_context.spanID
            orch_trace_ctx = tracing.build_orchestration_trace_context(
                start_time_ns, span_id=orch_span_id)

            res = pb.OrchestratorResponse(
                instanceId=instance_id,
                actions=result.actions,
                customStatus=ph.get_string_value(result.encoded_custom_status),
                completionToken=completionToken,
                orchestrationTraceContext=(
                    orch_trace_ctx if orch_trace_ctx
                    else req.orchestrationTraceContext
                ),
            )
        except pe.AbandonOrchestrationError:
            # Abandoned — no span needed
            self._logger.info(
                f"Abandoning orchestration. InstanceId = '{instance_id}'. Completion token = '{completionToken}'"
            )
            stub.AbandonTaskOrchestratorWorkItem(
                pb.AbandonOrchestrationTaskRequest(
                    completionToken=completionToken
                )
            )
            return
        except Exception as ex:
            # Unhandled error — emit a failed span
            tracing.emit_orchestration_span(
                orchestration_name,
                instance_id,
                start_time_ns,
                is_failed=True,
                failure_details=ex,
                parent_trace_context=parent_trace_ctx,
            )
            self._logger.exception(
                f"An error occurred while trying to execute instance '{instance_id}': {ex}"
            )
            failure_details = ph.new_failure_details(ex)
            actions = [
                ph.new_complete_orchestration_action(
                    -1, pb.ORCHESTRATION_STATUS_FAILED, "", failure_details
                )
            ]
            res = pb.OrchestratorResponse(
                instanceId=instance_id,
                actions=actions,
                completionToken=completionToken,
            )

        try:
            stub.CompleteOrchestratorTask(res)
        except Exception as ex:
            self._logger.exception(
                f"Failed to deliver orchestrator response for '{req.instanceId}' to sidecar: {ex}"
            )

    def _cancel_orchestrator(
            self,
            req: pb.OrchestratorRequest,
            stub: Union[stubs.TaskHubSidecarServiceStub, ProtoTaskHubSidecarServiceStub],
            completionToken,
    ):
        stub.AbandonTaskOrchestratorWorkItem(
            pb.AbandonOrchestrationTaskRequest(
                completionToken=completionToken
            )
        )
        self._logger.info(f"Cancelled orchestration task for invocation ID: {req.instanceId}")

    def _execute_activity(
            self,
            req: pb.ActivityRequest,
            stub: Union[stubs.TaskHubSidecarServiceStub, ProtoTaskHubSidecarServiceStub],
            completionToken,
    ):
        instance_id = req.orchestrationInstance.instanceId
        try:
            executor = _ActivityExecutor(self._registry, self._logger)
            with tracing.start_span(
                tracing.create_span_name("activity", req.name),
                trace_context=req.parentTraceContext,
                kind=tracing.SpanKind.SERVER,
                attributes={
                    tracing.ATTR_TASK_TYPE: "activity",
                    tracing.ATTR_TASK_INSTANCE_ID: instance_id,
                    tracing.ATTR_TASK_NAME: req.name,
                    tracing.ATTR_TASK_TASK_ID: str(req.taskId),
                },
            ) as span:
                try:
                    result = executor.execute(
                        instance_id, req.name, req.taskId, req.input.value
                    )
                except Exception as ex:
                    tracing.set_span_error(span, ex)
                    raise
            res = pb.ActivityResponse(
                instanceId=instance_id,
                taskId=req.taskId,
                result=ph.get_string_value(result),
                completionToken=completionToken,
            )
        except Exception as ex:
            res = pb.ActivityResponse(
                instanceId=instance_id,
                taskId=req.taskId,
                failureDetails=ph.new_failure_details(ex),
                completionToken=completionToken,
            )

        try:
            stub.CompleteActivityTask(res)
        except Exception as ex:
            self._logger.exception(
                f"Failed to deliver activity response for '{req.name}#{req.taskId}' of orchestration ID '{instance_id}' to sidecar: {ex}"
            )

    def _cancel_activity(
            self,
            req: pb.ActivityRequest,
            stub: Union[stubs.TaskHubSidecarServiceStub, ProtoTaskHubSidecarServiceStub],
            completionToken,
    ):
        stub.AbandonTaskActivityWorkItem(
            pb.AbandonActivityTaskRequest(
                completionToken=completionToken
            )
        )
        self._logger.info(f"Cancelled activity task for task ID: {req.taskId} on orchestration ID: {req.orchestrationInstance.instanceId}")

    def _execute_entity_batch(
            self,
            req: Union[pb.EntityBatchRequest, pb.EntityRequest],
            stub: Union[stubs.TaskHubSidecarServiceStub, ProtoTaskHubSidecarServiceStub],
            completionToken,
    ):
        operation_infos: list[pb.OperationInfo] = []
        if isinstance(req, pb.EntityRequest):
            req, operation_infos = helpers.convert_to_entity_batch_request(req)

        entity_state = StateShim(shared.from_json(req.entityState.value) if req.entityState.value else None)

        instance_id = req.instanceId
        try:
            entity_instance_id = EntityInstanceId.parse(instance_id)
        except ValueError:
            raise RuntimeError(f"Invalid entity instance ID '{instance_id}' in entity operation request.")

        results: list[pb.OperationResult] = []
        for operation in req.operations:
            start_time = datetime.now(timezone.utc)
            executor = _EntityExecutor(self._registry, self._logger)

            operation_result = None

            # Get the trace context for this operation, if available
            op_trace_ctx = operation.traceContext if operation.HasField("traceContext") else None

            with tracing.start_span(
                tracing.create_span_name("entity", f"{entity_instance_id.entity}:{operation.operation}"),
                trace_context=op_trace_ctx,
                kind=tracing.SpanKind.SERVER,
                attributes={
                    tracing.ATTR_TASK_TYPE: "entity",
                    tracing.ATTR_TASK_INSTANCE_ID: instance_id,
                    tracing.ATTR_TASK_NAME: entity_instance_id.entity,
                    "durabletask.entity.operation": operation.operation,
                },
            ) as span:
                try:
                    entity_result = executor.execute(
                        instance_id, entity_instance_id, operation.operation, entity_state, operation.input.value
                    )

                    entity_result = ph.get_string_value_or_empty(entity_result)
                    operation_result = pb.OperationResult(success=pb.OperationResultSuccess(
                        result=entity_result,
                        startTimeUtc=new_timestamp(start_time),
                        endTimeUtc=new_timestamp(datetime.now(timezone.utc))
                    ))
                    results.append(operation_result)

                    entity_state.commit()
                except Exception as ex:
                    tracing.set_span_error(span, ex)
                    self._logger.exception(ex)
                    operation_result = pb.OperationResult(failure=pb.OperationResultFailure(
                        failureDetails=ph.new_failure_details(ex),
                        startTimeUtc=new_timestamp(start_time),
                        endTimeUtc=new_timestamp(datetime.now(timezone.utc))
                    ))
                    results.append(operation_result)

                    entity_state.rollback()

        batch_result = pb.EntityBatchResult(
            results=results,
            actions=entity_state.get_operation_actions(),
            entityState=helpers.get_string_value(shared.to_json(entity_state._current_state)) if entity_state._current_state else None,
            failureDetails=None,
            completionToken=completionToken,
            operationInfos=operation_infos,
        )

        try:
            stub.CompleteEntityTask(batch_result)
        except Exception as ex:
            self._logger.exception(
                f"Failed to deliver entity response for '{entity_instance_id}' of orchestration ID '{instance_id}' to sidecar: {ex}"
            )

        # TODO: Reset context

        return batch_result

    def _cancel_entity_batch(
            self,
            req: Union[pb.EntityBatchRequest, pb.EntityRequest],
            stub: Union[stubs.TaskHubSidecarServiceStub, ProtoTaskHubSidecarServiceStub],
            completionToken,
    ):
        stub.AbandonTaskEntityWorkItem(
            pb.AbandonEntityTaskRequest(
                completionToken=completionToken
            )
        )
        self._logger.info(f"Cancelled entity batch task for instance ID: {req.instanceId}")


class _RuntimeOrchestrationContext(task.OrchestrationContext):
    _generator: Optional[Generator[task.Task, Any, Any]]
    _previous_task: Optional[task.Task]

    def __init__(self,
                 instance_id: str,
                 registry: _Registry,
                 maximum_timer_interval: Optional[timedelta] = DEFAULT_MAXIMUM_TIMER_INTERVAL,
                 ):
        self._generator = None
        self._is_replaying = True
        self._is_complete = False
        self._result = None
        self._pending_actions: dict[int, pb.OrchestratorAction] = {}
        self._pending_tasks: dict[int, task.CompletableTask] = {}
        # Maps entity ID to task ID
        self._entity_task_id_map: dict[str, tuple[EntityInstanceId, str, int]] = {}
        self._entity_lock_task_id_map: dict[str, tuple[EntityInstanceId, int]] = {}
        # Maps criticalSectionId to task ID
        self._entity_lock_id_map: dict[str, int] = {}
        self._sequence_number = 0
        self._new_uuid_counter = 0
        self._current_utc_datetime = datetime(1000, 1, 1)
        self._instance_id = instance_id
        self._registry = registry
        self._entity_context = OrchestrationEntityContext(instance_id)
        self._version: Optional[str] = None
        self._completion_status: Optional[pb.OrchestrationStatus] = None
        self._received_events: dict[str, list[Any]] = {}
        self._pending_events: dict[str, list[task.CancellableTask]] = {}
        self._new_input: Optional[Any] = None
        self._save_events = False
        self._encoded_custom_status: Optional[str] = None
        self._parent_trace_context: Optional[pb.TraceContext] = None
        self._orchestration_trace_context: Optional[pb.TraceContext] = None
        self._maximum_timer_interval = maximum_timer_interval

    def run(self, generator: Generator[task.Task, Any, Any]):
        self._generator = generator
        # TODO: Do something with this task
        task = next(generator)  # this starts the generator
        # TODO: Check if the task is null?
        self._previous_task = task

    def resume(self):
        if self._generator is None:
            # This is never expected unless maybe there's an issue with the history
            raise TypeError(
                "The orchestrator generator is not initialized! Was the orchestration history corrupted?"
            )

        # We can resume the generator only if the previously yielded task
        # has reached a completed state. The only time this won't be the
        # case is if the user yielded on a WhenAll task and there are still
        # outstanding child tasks that need to be completed.
        while self._previous_task is not None and self._previous_task.is_complete:
            next_task = None
            if self._previous_task.is_failed:
                # Raise the failure as an exception to the generator.
                # The orchestrator can then either handle the exception or allow it to fail the orchestration.
                next_task = self._generator.throw(self._previous_task.get_exception())
            else:
                # Resume the generator with the previous result.
                # This will either return a Task or raise StopIteration if it's done.
                next_task = self._generator.send(self._previous_task.get_result())

            if not isinstance(next_task, task.Task):
                raise TypeError("The orchestrator generator yielded a non-Task object")
            self._previous_task = next_task

    def set_complete(
            self,
            result: Any,
            status: pb.OrchestrationStatus,
            is_result_encoded: bool = False,
    ):
        if self._is_complete:
            return

        # If the user code returned without yielding the entity unlock, do that now
        if self._entity_context.is_inside_critical_section:
            self._exit_critical_section()

        self._is_complete = True
        self._completion_status = status
        # This is probably a bug - an orchestrator may complete with some actions remaining that the user still
        # wants to execute - for example, signaling an entity. So we shouldn't clear the pending actions here.
        # self._pending_actions.clear()  # Cancel any pending actions

        self._result = result
        result_json: Optional[str] = None
        if result is not None:
            try:
                result_json = result if is_result_encoded else shared.to_json(result)
            except (ValueError, TypeError):
                self._is_complete = False
                self._result = None
                self.set_failed(JsonEncodeOutputException(result))
                return
        action = ph.new_complete_orchestration_action(
            self.next_sequence_number(), status, result_json
        )
        self._pending_actions[action.id] = action

    def set_failed(self, ex: Union[Exception, pb.TaskFailureDetails]):
        if self._is_complete:
            return

        # If the user code crashed inside a critical section, or did not exit it, do that now
        if self._entity_context.is_inside_critical_section:
            self._exit_critical_section()

        self._is_complete = True
        # We also cannot cancel the pending actions in the failure case - if the user code had released an entity
        # lock, we *must* send that action to the sidecar.
        # self._pending_actions.clear()  # Cancel any pending actions
        self._completion_status = pb.ORCHESTRATION_STATUS_FAILED

        action = ph.new_complete_orchestration_action(
            self.next_sequence_number(),
            pb.ORCHESTRATION_STATUS_FAILED,
            None,
            ph.new_failure_details(ex) if isinstance(ex, Exception) else ex,
        )
        self._pending_actions[action.id] = action

    def set_continued_as_new(self, new_input: Any, save_events: bool):
        if self._is_complete:
            return

        # If the user code called continue_as_new while holding an entity lock, unlock it now
        if self._entity_context.is_inside_critical_section:
            self._exit_critical_section()

        self._is_complete = True
        # We also cannot cancel the pending actions in the continue as new case - if the user code had released an
        # entity lock, we *must* send that action to the sidecar.
        # self._pending_actions.clear()  # Cancel any pending actions
        self._completion_status = pb.ORCHESTRATION_STATUS_CONTINUED_AS_NEW
        self._new_input = new_input
        self._save_events = save_events

    def get_actions(self) -> list[pb.OrchestratorAction]:
        current_actions = list(self._pending_actions.values())
        if self._completion_status == pb.ORCHESTRATION_STATUS_CONTINUED_AS_NEW:
            # When continuing-as-new, we only return a single completion action.
            carryover_events: Optional[list[pb.HistoryEvent]] = None
            if self._save_events:
                carryover_events = []
                # We need to save the current set of pending events so that they can be
                # replayed when the new instance starts.
                for event_name, values in self._received_events.items():
                    for event_value in values:
                        encoded_value = (
                            shared.to_json(event_value) if event_value else None
                        )
                        carryover_events.append(
                            ph.new_event_raised_event(event_name, encoded_value)
                        )
            action = ph.new_complete_orchestration_action(
                self.next_sequence_number(),
                pb.ORCHESTRATION_STATUS_CONTINUED_AS_NEW,
                result=shared.to_json(self._new_input)
                if self._new_input is not None
                else None,
                failure_details=None,
                carryover_events=carryover_events,
            )
            # We must return the existing tasks as well, to capture entity unlocks
            current_actions.append(action)
        return current_actions

    def next_sequence_number(self) -> int:
        self._sequence_number += 1
        return self._sequence_number

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def version(self) -> Optional[str]:
        return self._version

    @property
    def current_utc_datetime(self) -> datetime:
        return self._current_utc_datetime

    @current_utc_datetime.setter
    def current_utc_datetime(self, value: datetime):
        self._current_utc_datetime = value

    @property
    def is_replaying(self) -> bool:
        return self._is_replaying

    def set_custom_status(self, custom_status: Any) -> None:
        self._encoded_custom_status = (
            shared.to_json(custom_status) if custom_status is not None else None
        )

    def create_timer(self, fire_at: Union[datetime, timedelta]) -> task.CancellableTask:
        return self.create_timer_internal(fire_at)

    def create_timer_internal(
            self,
            fire_at: Union[datetime, timedelta],
            retryable_task: Optional[task.RetryableTask] = None,
    ) -> task.TimerTask:
        id = self.next_sequence_number()
        if isinstance(fire_at, timedelta):
            final_fire_at = self.current_utc_datetime + fire_at
        else:
            final_fire_at = fire_at

        next_fire_at: datetime = final_fire_at

        if (
            self._maximum_timer_interval is not None
            and self._maximum_timer_interval > timedelta(0)
            and self.current_utc_datetime + self._maximum_timer_interval < final_fire_at
        ):
            timer_task = task.LongTimerTask(final_fire_at, self._maximum_timer_interval)
            next_fire_at = timer_task.start(self.current_utc_datetime)
        else:
            timer_task = task.TimerTask()

        action = ph.new_create_timer_action(id, next_fire_at)
        self._pending_actions[id] = action

        def _cancel_timer() -> None:
            self._pending_actions.pop(id, None)
            self._pending_tasks.pop(id, None)

        timer_task.set_cancel_handler(_cancel_timer)
        if retryable_task is not None:
            timer_task.set_retryable_parent(retryable_task)
        self._pending_tasks[id] = timer_task
        return timer_task

    def call_activity(
            self,
            activity: Union[task.Activity[TInput, TOutput], str],
            *,
            input: Optional[TInput] = None,
            retry_policy: Optional[task.RetryPolicy] = None,
            tags: Optional[dict[str, str]] = None,
    ) -> task.CompletableTask[TOutput]:
        id = self.next_sequence_number()

        self.call_activity_function_helper(
            id, activity, input=input, retry_policy=retry_policy, is_sub_orch=False, tags=tags
        )
        return self._pending_tasks.get(id, task.CompletableTask())

    def call_entity(
            self,
            entity: EntityInstanceId,
            operation: str,
            input: Optional[TInput] = None,
    ) -> task.CompletableTask[Any]:
        id = self.next_sequence_number()

        self.call_entity_function_helper(
            id, entity, operation, input=input
        )

        return self._pending_tasks.get(id, task.CompletableTask())

    def signal_entity(
            self,
            entity_id: EntityInstanceId,
            operation_name: str,
            input: Optional[TInput] = None
    ) -> None:
        id = self.next_sequence_number()

        self.signal_entity_function_helper(
            id, entity_id, operation_name, input
        )

    def lock_entities(self, entities: list[EntityInstanceId]) -> task.CompletableTask[EntityLock]:
        id = self.next_sequence_number()

        self.lock_entities_function_helper(
            id, entities
        )
        return self._pending_tasks.get(id, task.CompletableTask())

    def call_sub_orchestrator(
            self,
            orchestrator: Union[task.Orchestrator[TInput, TOutput], str],
            *,
            input: Optional[TInput] = None,
            instance_id: Optional[str] = None,
            retry_policy: Optional[task.RetryPolicy] = None,
            version: Optional[str] = None,
    ) -> task.CompletableTask[TOutput]:
        id = self.next_sequence_number()
        if isinstance(orchestrator, str):
            orchestrator_name = orchestrator
        else:
            orchestrator_name = task.get_name(orchestrator)
        default_version = self._registry.versioning.default_version if self._registry.versioning else None
        orchestrator_version = version if version else default_version
        self.call_activity_function_helper(
            id,
            orchestrator_name,
            input=input,
            retry_policy=retry_policy,
            is_sub_orch=True,
            instance_id=instance_id,
            version=orchestrator_version
        )
        return self._pending_tasks.get(id, task.CompletableTask())

    def call_activity_function_helper(
            self,
            id: Optional[int],
            activity_function: Union[task.Activity[TInput, TOutput], str],
            *,
            input: Optional[TInput] = None,
            retry_policy: Optional[task.RetryPolicy] = None,
            tags: Optional[dict[str, str]] = None,
            is_sub_orch: bool = False,
            instance_id: Optional[str] = None,
            fn_task: Optional[task.CompletableTask[TOutput]] = None,
            version: Optional[str] = None,
    ):
        if id is None:
            id = self.next_sequence_number()

        if fn_task is None:
            encoded_input = shared.to_json(input) if input is not None else None
        else:
            # Here, we don't need to convert the input to JSON because it is already converted.
            # We just need to take string representation of it.
            encoded_input = str(input)
        if not is_sub_orch:
            name = (
                activity_function
                if isinstance(activity_function, str)
                else task.get_name(activity_function)
            )
            # Generate a trace context for the deferred CLIENT span.
            # The actual span is emitted later with proper timestamps
            # when the taskCompleted/taskFailed event arrives.
            orch_ctx = self._orchestration_trace_context or self._parent_trace_context
            parent_ctx = orch_ctx
            if not self._is_replaying:
                client_ctx = tracing.generate_client_trace_context(
                    parent_trace_context=orch_ctx)
                if client_ctx is not None:
                    parent_ctx = client_ctx
            action = ph.new_schedule_task_action(
                id, name, encoded_input, tags,
                parent_trace_context=parent_ctx)
        else:
            if instance_id is None:
                # Create a deteministic instance ID based on the parent instance ID
                instance_id = f"{self.instance_id}:{id:04x}"
            if not isinstance(activity_function, str):
                raise ValueError("Orchestrator function name must be a string")
            # Generate a trace context for the deferred CLIENT span.
            # The actual span is emitted later with proper timestamps
            # when the sub-orchestration completes or fails.
            orch_ctx = self._orchestration_trace_context or self._parent_trace_context
            parent_ctx = orch_ctx
            if not self._is_replaying:
                client_ctx = tracing.generate_client_trace_context(
                    parent_trace_context=orch_ctx)
                if client_ctx is not None:
                    parent_ctx = client_ctx
            action = ph.new_create_sub_orchestration_action(
                id, activity_function, instance_id, encoded_input, version,
                parent_trace_context=parent_ctx
            )
        self._pending_actions[id] = action

        if fn_task is None:
            if retry_policy is None:
                fn_task = task.CompletableTask[TOutput]()
            else:
                fn_task = task.RetryableTask[TOutput](
                    retry_policy=retry_policy,
                    action=action,
                    start_time=self.current_utc_datetime,
                    is_sub_orch=is_sub_orch,
                )
        self._pending_tasks[id] = fn_task

    def call_entity_function_helper(
            self,
            id: Optional[int],
            entity_id: EntityInstanceId,
            operation: str,
            *,
            input: Optional[TInput] = None,
    ):
        if id is None:
            id = self.next_sequence_number()

        transition_valid, error_message = self._entity_context.validate_operation_transition(entity_id, False)
        if not transition_valid:
            raise RuntimeError(error_message)

        encoded_input = shared.to_json(input) if input is not None else None
        action = ph.new_call_entity_action(id, self.instance_id, entity_id, operation, encoded_input, self.new_uuid())
        self._pending_actions[id] = action

        fn_task = task.CompletableTask()
        self._pending_tasks[id] = fn_task

    def signal_entity_function_helper(
            self,
            id: Optional[int],
            entity_id: EntityInstanceId,
            operation: str,
            input: Optional[TInput]
    ) -> None:
        if id is None:
            id = self.next_sequence_number()

        transition_valid, error_message = self._entity_context.validate_operation_transition(entity_id, True)

        if not transition_valid:
            raise RuntimeError(error_message)

        encoded_input = shared.to_json(input) if input is not None else None

        action = ph.new_signal_entity_action(id, entity_id, operation, encoded_input, self.new_uuid())
        self._pending_actions[id] = action

    def lock_entities_function_helper(self, id: int, entities: list[EntityInstanceId]) -> None:
        if id is None:
            id = self.next_sequence_number()

        transition_valid, error_message = self._entity_context.validate_acquire_transition()
        if not transition_valid:
            raise RuntimeError(error_message)

        critical_section_id = self.new_uuid()

        request, target = self._entity_context.emit_acquire_message(critical_section_id, entities)

        if not request or not target:
            raise RuntimeError("Failed to create entity lock request.")

        action = ph.new_lock_entities_action(id, request)
        self._pending_actions[id] = action

        fn_task = task.CompletableTask[EntityLock]()
        self._pending_tasks[id] = fn_task

    def _exit_critical_section(self) -> None:
        if not self._entity_context.is_inside_critical_section:
            # Possible if the user calls continue_as_new inside the lock - in the success case, we will call
            # _exit_critical_section both from the EntityLock and the continue_as_new logic. We must keep both calls in
            # case the user code crashes after calling continue_as_new but before the EntityLock object is exited.
            return
        for entity_unlock_message in self._entity_context.emit_lock_release_messages():
            task_id = self.next_sequence_number()
            action = pb.OrchestratorAction(id=task_id, sendEntityMessage=entity_unlock_message)
            self._pending_actions[task_id] = action

    def wait_for_external_event(self, name: str) -> task.CancellableTask:
        # Check to see if this event has already been received, in which case we
        # can return it immediately. Otherwise, record out intent to receive an
        # event with the given name so that we can resume the generator when it
        # arrives. If there are multiple events with the same name, we return
        # them in the order they were received.
        external_event_task: task.CancellableTask = task.CancellableTask()
        event_name = name.casefold()
        event_list = self._received_events.get(event_name, None)
        if event_list:
            event_data = event_list.pop(0)
            if not event_list:
                del self._received_events[event_name]
            external_event_task.complete(event_data)
        else:
            task_list = self._pending_events.get(event_name, None)
            if not task_list:
                task_list = []
                self._pending_events[event_name] = task_list
            task_list.append(external_event_task)

            def _cancel_wait() -> None:
                waiting_tasks = self._pending_events.get(event_name)
                if waiting_tasks is None:
                    return
                try:
                    waiting_tasks.remove(external_event_task)
                except ValueError:
                    return
                if not waiting_tasks:
                    del self._pending_events[event_name]

            external_event_task.set_cancel_handler(_cancel_wait)
        return external_event_task

    def continue_as_new(self, new_input, *, save_events: bool = False) -> None:
        if self._is_complete:
            return

        self.set_continued_as_new(new_input, save_events)

    def new_uuid(self) -> str:
        NAMESPACE_UUID: str = "9e952958-5e33-4daf-827f-2fa12937b875"

        uuid_name_value = \
            f"{self._instance_id}" \
            f"_{self.current_utc_datetime.strftime(DATETIME_STRING_FORMAT)}" \
            f"_{self._new_uuid_counter}"
        self._new_uuid_counter += 1
        namespace_uuid = uuid.uuid5(uuid.NAMESPACE_OID, NAMESPACE_UUID)
        return str(uuid.uuid5(namespace_uuid, uuid_name_value))


class ExecutionResults:
    actions: list[pb.OrchestratorAction]
    encoded_custom_status: Optional[str]
    _orchestration_trace_context: Optional[pb.TraceContext]

    def __init__(
            self, actions: list[pb.OrchestratorAction], encoded_custom_status: Optional[str],
            orchestration_trace_context: Optional[pb.TraceContext] = None,
    ):
        self.actions = actions
        self.encoded_custom_status = encoded_custom_status
        self._orchestration_trace_context = orchestration_trace_context


class _OrchestrationExecutor:
    _generator: Optional[task.Orchestrator] = None

    def __init__(
        self,
        registry: _Registry,
        logger: logging.Logger,
        persisted_orch_span_id: Optional[str] = None,
        maximum_timer_interval: Optional[timedelta] = DEFAULT_MAXIMUM_TIMER_INTERVAL,
    ):
        self._registry = registry
        self._logger = logger
        self._maximum_timer_interval = maximum_timer_interval
        self._is_suspended = False
        self._suspended_events: list[pb.HistoryEvent] = []
        self._persisted_orch_span_id = persisted_orch_span_id
        # Maps timer_id -> (fire_at, created_time_ns)
        self._timer_fire_at: dict[int, tuple[datetime, Optional[int]]] = {}
        # Maps task_id -> (task_type, name, instance_id, scheduled_ns,
        #                  client_trace_ctx, version)
        # Used to reconstruct CLIENT spans with proper timestamps.
        self._task_scheduled_info: dict[
            int, tuple[str, str, str, Optional[int], pb.TraceContext, Optional[str]]
        ] = {}

    def execute(
            self,
            instance_id: str,
            old_events: Sequence[pb.HistoryEvent],
            new_events: Sequence[pb.HistoryEvent],
    ) -> ExecutionResults:
        orchestration_name = "<unknown>"
        orchestration_started_events = [e for e in old_events if e.HasField("executionStarted")]
        if len(orchestration_started_events) >= 1:
            orchestration_name = orchestration_started_events[0].executionStarted.name
        self._orchestration_name = orchestration_name

        self._logger.debug(
            f"{instance_id}: Beginning replay for orchestrator {orchestration_name}..."
        )

        if not new_events:
            raise task.OrchestrationStateError(
                "The new history event list must have at least one event in it."
            )

        ctx = _RuntimeOrchestrationContext(
            instance_id,
            self._registry,
            maximum_timer_interval=self._maximum_timer_interval,
        )
        try:
            # Rebuild local state by replaying old history into the orchestrator function
            self._logger.debug(
                f"{instance_id}: Rebuilding local state with {len(old_events)} history event..."
            )
            ctx._is_replaying = True
            for old_event in old_events:
                self.process_event(ctx, old_event)

            # Get new actions by executing newly received events into the orchestrator function
            if self._logger.level <= logging.DEBUG:
                summary = _get_new_event_summary(new_events)
                self._logger.debug(
                    f"{instance_id}: Processing {len(new_events)} new event(s): {summary}"
                )
            ctx._is_replaying = False
            for new_event in new_events:
                self.process_event(ctx, new_event)

        except pe.VersionFailureException as ex:
            if self._registry.versioning and self._registry.versioning.failure_strategy == VersionFailureStrategy.FAIL:
                if ex.error_details:
                    ctx.set_failed(ex.error_details)
                else:
                    ctx.set_failed(ex)
            elif self._registry.versioning and self._registry.versioning.failure_strategy == VersionFailureStrategy.REJECT:
                raise pe.AbandonOrchestrationError

        except Exception as ex:
            # Unhandled exceptions fail the orchestration
            self._logger.debug(f"{instance_id}: Orchestration {orchestration_name} failed")
            ctx.set_failed(ex)

        if not ctx._is_complete:
            task_count = len(ctx._pending_tasks)
            event_count = len(ctx._pending_events)
            self._logger.info(
                f"{instance_id}: Orchestrator {orchestration_name} yielded with {task_count} task(s) "
                f"and {event_count} event(s) outstanding."
            )
        elif (
                ctx._completion_status and ctx._completion_status is not pb.ORCHESTRATION_STATUS_CONTINUED_AS_NEW
        ):
            completion_status_str = ph.get_orchestration_status_str(
                ctx._completion_status
            )
            self._logger.info(
                f"{instance_id}: Orchestration {orchestration_name} completed with status: {completion_status_str}"
            )

        actions = ctx.get_actions()
        if self._logger.level <= logging.DEBUG:
            self._logger.debug(
                f"{instance_id}: Returning {len(actions)} action(s): {_get_action_summary(actions)}"
            )
        return ExecutionResults(
            actions=actions, encoded_custom_status=ctx._encoded_custom_status,
            orchestration_trace_context=ctx._orchestration_trace_context,
        )

    def process_event(
            self, ctx: _RuntimeOrchestrationContext, event: pb.HistoryEvent
    ) -> None:
        if self._is_suspended and _is_suspendable(event):
            # We are suspended, so we need to buffer this event until we are resumed
            self._suspended_events.append(event)
            return

        try:
            if event.HasField("orchestratorStarted"):
                ctx.current_utc_datetime = event.timestamp.ToDatetime()
            elif event.HasField("executionStarted"):
                fn = self._registry.get_orchestrator(event.executionStarted.name)
                if fn is None:
                    raise OrchestratorNotRegisteredError(
                        f"A '{event.executionStarted.name}' orchestrator was not registered."
                    )

                if event.executionStarted.version:
                    ctx._version = event.executionStarted.version.value

                # Store the parent trace context for propagation to child tasks
                if event.executionStarted.HasField("parentTraceContext"):
                    ctx._parent_trace_context = event.executionStarted.parentTraceContext
                    # Reuse a persisted span ID from a prior dispatch so
                    # activities/timers/sub-orchestrations across all
                    # dispatches share the same parent.  On the first
                    # dispatch, generate a new random span ID.
                    if self._persisted_orch_span_id:
                        ctx._orchestration_trace_context = tracing.reconstruct_trace_context(
                            ctx._parent_trace_context,
                            self._persisted_orch_span_id)
                    else:
                        ctx._orchestration_trace_context = tracing.generate_client_trace_context(
                            parent_trace_context=ctx._parent_trace_context)

                if self._registry.versioning:
                    version_failure = self.evaluate_orchestration_versioning(
                        self._registry.versioning,
                        ctx.version
                    )
                    if version_failure:
                        self._logger.warning(
                            f"Orchestration version did not meet worker versioning requirements. "
                            f"Error action = '{self._registry.versioning.failure_strategy}'. "
                            f"Version error = '{version_failure}'"
                        )
                        raise pe.VersionFailureException(version_failure)

                # deserialize the input, if any
                input = None
                if (
                        event.executionStarted.input is not None and event.executionStarted.input.value != ""
                ):
                    input = shared.from_json(event.executionStarted.input.value)

                result = fn(
                    ctx, input
                )  # this does not execute the generator, only creates it
                if isinstance(result, GeneratorType):
                    # Start the orchestrator's generator function
                    ctx.run(result)
                else:
                    # This is an orchestrator that doesn't schedule any tasks
                    ctx.set_complete(result, pb.ORCHESTRATION_STATUS_COMPLETED)
            elif event.HasField("timerCreated"):
                # This history event confirms that the timer was successfully scheduled.
                # Remove the timerCreated event from the pending action list so we don't schedule it again.
                timer_id = event.eventId
                action = ctx._pending_actions.pop(timer_id, None)
                if not action:
                    raise _get_non_determinism_error(
                        timer_id, task.get_name(ctx.create_timer)
                    )
                elif not action.HasField("createTimer"):
                    expected_method_name = task.get_name(ctx.create_timer)
                    raise _get_wrong_action_type_error(
                        timer_id, expected_method_name, action
                    )
                # Track timer fire_at and creation timestamp for span emission
                if action.createTimer.HasField("fireAt"):
                    created_ns = (event.timestamp.ToNanoseconds()
                                  if event.HasField("timestamp") else None)
                    self._timer_fire_at[timer_id] = (
                        action.createTimer.fireAt.ToDatetime(), created_ns,
                    )
            elif event.HasField("timerFired"):
                timer_id = event.timerFired.timerId
                timer_task = ctx._pending_tasks.pop(timer_id, None)
                if not timer_task:
                    # Unexpected event for unknown timer; log and skip.
                    if not ctx.is_replaying:
                        self._logger.warning(
                            f"{ctx.instance_id}: Ignoring unexpected timerFired event with ID = {timer_id}."
                        )
                    return
                if not (isinstance(timer_task, task.TimerTask) or isinstance(timer_task, task.LongTimerTask)):
                    if not ctx._is_replaying:
                        self._logger.warning(
                            f"{ctx.instance_id}: Ignoring timerFired event with non-timer task ID = {timer_id}."
                        )
                    return
                # Emit timer span with backdated start time (skip during replay)
                if not ctx.is_replaying:
                    timer_info = self._timer_fire_at.get(timer_id)
                    if timer_info is not None:
                        fire_at, created_ns = timer_info
                        tracing.emit_timer_span(
                            self._orchestration_name, ctx.instance_id,
                            timer_id, fire_at,
                            scheduled_time_ns=created_ns,
                            parent_trace_context=ctx._orchestration_trace_context or ctx._parent_trace_context,
                        )
                next_fire_at = timer_task.complete(event.timerFired.fireAt.ToDatetime())
                if next_fire_at is not None:
                    id = ctx.next_sequence_number()
                    new_action = ph.new_create_timer_action(id, next_fire_at)
                    ctx._pending_tasks[id] = timer_task
                    ctx._pending_actions[id] = new_action

                    def _cancel_timer() -> None:
                        ctx._pending_actions.pop(id, None)
                        ctx._pending_tasks.pop(id, None)

                    timer_task.set_cancel_handler(_cancel_timer)
                else:
                    if timer_task._retryable_parent is not None:
                        activity_action = timer_task._retryable_parent._action

                        if not timer_task._retryable_parent._is_sub_orch:
                            cur_task = activity_action.scheduleTask
                            instance_id = None
                        else:
                            cur_task = activity_action.createSubOrchestration
                            instance_id = cur_task.instanceId
                        ctx.call_activity_function_helper(
                            id=activity_action.id,
                            activity_function=cur_task.name,
                            input=cur_task.input.value,
                            retry_policy=timer_task._retryable_parent._retry_policy,
                            is_sub_orch=timer_task._retryable_parent._is_sub_orch,
                            instance_id=instance_id,
                            fn_task=timer_task._retryable_parent,
                        )
                    else:
                        ctx.resume()
            elif event.HasField("taskScheduled"):
                # This history event confirms that the activity execution was successfully scheduled.
                # Remove the taskScheduled event from the pending action list so we don't schedule it again.
                task_id = event.eventId
                action = ctx._pending_actions.pop(task_id, None)
                activity_task = ctx._pending_tasks.get(task_id, None)
                if not action:
                    raise _get_non_determinism_error(
                        task_id, task.get_name(ctx.call_activity)
                    )
                elif not action.HasField("scheduleTask"):
                    expected_method_name = task.get_name(ctx.call_activity)
                    raise _get_wrong_action_type_error(
                        task_id, expected_method_name, action
                    )
                elif action.scheduleTask.name != event.taskScheduled.name:
                    raise _get_wrong_action_name_error(
                        task_id,
                        method_name=task.get_name(ctx.call_activity),
                        expected_task_name=event.taskScheduled.name,
                        actual_task_name=action.scheduleTask.name,
                    )
                # Store info for deferred CLIENT span reconstruction
                ts_evt = event.taskScheduled
                if ts_evt.HasField("parentTraceContext") and ts_evt.parentTraceContext.traceParent:
                    sched_ns = event.timestamp.ToNanoseconds() if event.HasField("timestamp") else None
                    ver_str = ts_evt.version.value if ts_evt.HasField("version") else None
                    self._task_scheduled_info[task_id] = (
                        "activity", ts_evt.name, ctx.instance_id,
                        sched_ns, ts_evt.parentTraceContext, ver_str,
                    )
            elif event.HasField("taskCompleted"):
                # This history event contains the result of a completed activity task.
                task_id = event.taskCompleted.taskScheduledId
                activity_task = ctx._pending_tasks.pop(task_id, None)
                if not activity_task:
                    # Unexpected completion for unknown task; log and skip.
                    if not ctx.is_replaying:
                        self._logger.warning(
                            f"{ctx.instance_id}: Ignoring unexpected taskCompleted event with ID = {task_id}."
                        )
                    return
                # Emit deferred CLIENT span with proper timestamps
                if not ctx.is_replaying:
                    info = self._task_scheduled_info.pop(task_id, None)
                    if info is not None:
                        t_type, t_name, t_iid, s_ns, c_ctx, t_ver = info
                        e_ns = event.timestamp.ToNanoseconds() if event.HasField("timestamp") else None
                        tracing.emit_client_span(
                            t_type, t_name, t_iid, task_id,
                            client_trace_context=c_ctx,
                            parent_trace_context=ctx._orchestration_trace_context or ctx._parent_trace_context,
                            start_time_ns=s_ns, end_time_ns=e_ns,
                            version=t_ver,
                        )
                result = None
                if not ph.is_empty(event.taskCompleted.result):
                    result = shared.from_json(event.taskCompleted.result.value)
                activity_task.complete(result)
                ctx.resume()
            elif event.HasField("taskFailed"):
                task_id = event.taskFailed.taskScheduledId
                activity_task = ctx._pending_tasks.pop(task_id, None)
                if not activity_task:
                    # Unexpected failure for unknown task; log and skip.
                    if not ctx.is_replaying:
                        self._logger.warning(
                            f"{ctx.instance_id}: Ignoring unexpected taskFailed event with ID = {task_id}."
                        )
                    return

                # Emit deferred CLIENT span with error status
                if not ctx.is_replaying:
                    info = self._task_scheduled_info.pop(task_id, None)
                    if info is not None:
                        t_type, t_name, t_iid, s_ns, c_ctx, t_ver = info
                        e_ns = event.timestamp.ToNanoseconds() if event.HasField("timestamp") else None
                        tracing.emit_client_span(
                            t_type, t_name, t_iid, task_id,
                            client_trace_context=c_ctx,
                            parent_trace_context=ctx._orchestration_trace_context or ctx._parent_trace_context,
                            start_time_ns=s_ns, end_time_ns=e_ns,
                            is_error=True,
                            error_message=str(event.taskFailed.failureDetails.errorMessage),
                            version=t_ver,
                        )

                if isinstance(activity_task, task.RetryableTask):
                    if activity_task._retry_policy is not None:
                        next_delay = activity_task.compute_next_delay()
                        if next_delay is None:
                            activity_task.fail(
                                f"{ctx.instance_id}: Activity task #{task_id} failed: {event.taskFailed.failureDetails.errorMessage}",
                                event.taskFailed.failureDetails,
                            )
                            ctx.resume()
                        else:
                            activity_task.increment_attempt_count()
                            ctx.create_timer_internal(next_delay, activity_task)
                elif isinstance(activity_task, task.CompletableTask):
                    activity_task.fail(
                        f"{ctx.instance_id}: Activity task #{task_id} failed: {event.taskFailed.failureDetails.errorMessage}",
                        event.taskFailed.failureDetails,
                    )
                    ctx.resume()
                else:
                    raise TypeError("Unexpected task type")
            elif event.HasField("subOrchestrationInstanceCreated"):
                # This history event confirms that the sub-orchestration execution was successfully scheduled.
                # Remove the subOrchestrationInstanceCreated event from the pending action list so we don't schedule it again.
                task_id = event.eventId
                action = ctx._pending_actions.pop(task_id, None)
                if not action:
                    raise _get_non_determinism_error(
                        task_id, task.get_name(ctx.call_sub_orchestrator)
                    )
                elif not action.HasField("createSubOrchestration"):
                    expected_method_name = task.get_name(ctx.call_sub_orchestrator)
                    raise _get_wrong_action_type_error(
                        task_id, expected_method_name, action
                    )
                elif (
                        action.createSubOrchestration.name != event.subOrchestrationInstanceCreated.name
                ):
                    raise _get_wrong_action_name_error(
                        task_id,
                        method_name=task.get_name(ctx.call_sub_orchestrator),
                        expected_task_name=event.subOrchestrationInstanceCreated.name,
                        actual_task_name=action.createSubOrchestration.name,
                    )
                # Store info for deferred CLIENT span reconstruction
                sub_evt = event.subOrchestrationInstanceCreated
                if sub_evt.HasField("parentTraceContext") and sub_evt.parentTraceContext.traceParent:
                    sched_ns = event.timestamp.ToNanoseconds() if event.HasField("timestamp") else None
                    ver_str = sub_evt.version.value if sub_evt.HasField("version") else None
                    self._task_scheduled_info[task_id] = (
                        "orchestration", sub_evt.name, sub_evt.instanceId,
                        sched_ns, sub_evt.parentTraceContext, ver_str,
                    )
            elif event.HasField("subOrchestrationInstanceCompleted"):
                task_id = event.subOrchestrationInstanceCompleted.taskScheduledId
                sub_orch_task = ctx._pending_tasks.pop(task_id, None)
                if not sub_orch_task:
                    # Unexpected completion for unknown sub-orchestration; log and skip.
                    if not ctx.is_replaying:
                        self._logger.warning(
                            f"{ctx.instance_id}: Ignoring unexpected subOrchestrationInstanceCompleted event with ID = {task_id}."
                        )
                    return
                # Emit deferred CLIENT span with proper timestamps
                if not ctx.is_replaying:
                    info = self._task_scheduled_info.pop(task_id, None)
                    if info is not None:
                        t_type, t_name, t_iid, s_ns, c_ctx, t_ver = info
                        e_ns = event.timestamp.ToNanoseconds() if event.HasField("timestamp") else None
                        tracing.emit_client_span(
                            t_type, t_name, t_iid, task_id,
                            client_trace_context=c_ctx,
                            parent_trace_context=ctx._orchestration_trace_context or ctx._parent_trace_context,
                            start_time_ns=s_ns, end_time_ns=e_ns,
                            version=t_ver,
                        )
                result = None
                if not ph.is_empty(event.subOrchestrationInstanceCompleted.result):
                    result = shared.from_json(
                        event.subOrchestrationInstanceCompleted.result.value
                    )
                sub_orch_task.complete(result)
                ctx.resume()
            elif event.HasField("subOrchestrationInstanceFailed"):
                failedEvent = event.subOrchestrationInstanceFailed
                task_id = failedEvent.taskScheduledId
                sub_orch_task = ctx._pending_tasks.pop(task_id, None)
                if not sub_orch_task:
                    # Unexpected failure for unknown sub-orchestration; log and skip.
                    if not ctx.is_replaying:
                        self._logger.warning(
                            f"{ctx.instance_id}: Ignoring unexpected subOrchestrationInstanceFailed event with ID = {task_id}."
                        )
                    return
                # Emit deferred CLIENT span with error status
                if not ctx.is_replaying:
                    info = self._task_scheduled_info.pop(task_id, None)
                    if info is not None:
                        t_type, t_name, t_iid, s_ns, c_ctx, t_ver = info
                        e_ns = event.timestamp.ToNanoseconds() if event.HasField("timestamp") else None
                        tracing.emit_client_span(
                            t_type, t_name, t_iid, task_id,
                            client_trace_context=c_ctx,
                            parent_trace_context=ctx._orchestration_trace_context or ctx._parent_trace_context,
                            start_time_ns=s_ns, end_time_ns=e_ns,
                            is_error=True,
                            error_message=str(failedEvent.failureDetails.errorMessage),
                            version=t_ver,
                        )
                if isinstance(sub_orch_task, task.RetryableTask):
                    if sub_orch_task._retry_policy is not None:
                        next_delay = sub_orch_task.compute_next_delay()
                        if next_delay is None:
                            sub_orch_task.fail(
                                f"Sub-orchestration task #{task_id} failed: {failedEvent.failureDetails.errorMessage}",
                                failedEvent.failureDetails,
                            )
                            ctx.resume()
                        else:
                            sub_orch_task.increment_attempt_count()
                            ctx.create_timer_internal(next_delay, sub_orch_task)
                elif isinstance(sub_orch_task, task.CompletableTask):
                    sub_orch_task.fail(
                        f"Sub-orchestration task #{task_id} failed: {failedEvent.failureDetails.errorMessage}",
                        failedEvent.failureDetails,
                    )
                    ctx.resume()
                else:
                    raise TypeError("Unexpected sub-orchestration task type")
            elif event.HasField("eventRaised"):
                if event.eventRaised.name in ctx._entity_task_id_map:
                    entity_id, operation, task_id = ctx._entity_task_id_map.get(event.eventRaised.name, (None, None, None))
                    self._handle_entity_event_raised(ctx, event, entity_id, task_id, False)
                elif event.eventRaised.name in ctx._entity_lock_task_id_map:
                    entity_id, task_id = ctx._entity_lock_task_id_map.get(event.eventRaised.name, (None, None))
                    self._handle_entity_event_raised(ctx, event, entity_id, task_id, True)
                else:
                    # event names are case-insensitive
                    event_name = event.eventRaised.name.casefold()
                    if not ctx.is_replaying:
                        self._logger.info(f"{ctx.instance_id} Event raised: {event_name}")
                    task_list = ctx._pending_events.get(event_name, None)
                    decoded_result: Optional[Any] = None
                    if task_list:
                        event_task = task_list.pop(0)
                        if not ph.is_empty(event.eventRaised.input):
                            decoded_result = shared.from_json(event.eventRaised.input.value)
                        event_task.complete(decoded_result)
                        if not task_list:
                            del ctx._pending_events[event_name]
                        ctx.resume()
                    else:
                        # buffer the event
                        event_list = ctx._received_events.get(event_name, None)
                        if not event_list:
                            event_list = []
                            ctx._received_events[event_name] = event_list
                        if not ph.is_empty(event.eventRaised.input):
                            decoded_result = shared.from_json(event.eventRaised.input.value)
                        event_list.append(decoded_result)
                        if not ctx.is_replaying:
                            self._logger.info(
                                f"{ctx.instance_id}: Event '{event_name}' has been buffered as there are no tasks waiting for it."
                            )
            elif event.HasField("executionSuspended"):
                if not self._is_suspended and not ctx.is_replaying:
                    self._logger.info(f"{ctx.instance_id}: Execution suspended.")
                self._is_suspended = True
            elif event.HasField("executionResumed") and self._is_suspended:
                if not ctx.is_replaying:
                    self._logger.info(f"{ctx.instance_id}: Resuming execution.")
                self._is_suspended = False
                for e in self._suspended_events:
                    self.process_event(ctx, e)
                self._suspended_events = []
            elif event.HasField("executionTerminated"):
                if not ctx.is_replaying:
                    self._logger.info(f"{ctx.instance_id}: Execution terminating.")
                encoded_output = (
                    event.executionTerminated.input.value
                    if not ph.is_empty(event.executionTerminated.input)
                    else None
                )
                ctx.set_complete(
                    encoded_output,
                    pb.ORCHESTRATION_STATUS_TERMINATED,
                    is_result_encoded=True,
                )
            elif event.HasField("entityOperationCalled"):
                # This history event confirms that the entity operation was successfully scheduled.
                # Remove the entityOperationCalled event from the pending action list so we don't schedule it again
                entity_call_id = event.eventId
                action = ctx._pending_actions.pop(entity_call_id, None)
                entity_task = ctx._pending_tasks.get(entity_call_id, None)
                if not action:
                    raise _get_non_determinism_error(
                        entity_call_id, task.get_name(ctx.call_entity)
                    )
                elif not action.HasField("sendEntityMessage") or not action.sendEntityMessage.HasField("entityOperationCalled"):
                    expected_method_name = task.get_name(ctx.call_entity)
                    raise _get_wrong_action_type_error(
                        entity_call_id, expected_method_name, action
                    )
                try:
                    entity_id = EntityInstanceId.parse(event.entityOperationCalled.targetInstanceId.value)
                    operation = event.entityOperationCalled.operation
                except ValueError:
                    raise RuntimeError(f"Could not parse entity ID from targetInstanceId '{event.entityOperationCalled.targetInstanceId.value}'")
                ctx._entity_task_id_map[event.entityOperationCalled.requestId] = (entity_id, operation, entity_call_id)
            elif event.HasField("entityOperationSignaled"):
                # This history event confirms that the entity signal was successfully scheduled.
                # Remove the entityOperationSignaled event from the pending action list so we don't schedule it
                entity_signal_id = event.eventId
                action = ctx._pending_actions.pop(entity_signal_id, None)
                if not action:
                    raise _get_non_determinism_error(
                        entity_signal_id, task.get_name(ctx.signal_entity)
                    )
                elif not action.HasField("sendEntityMessage") or not action.sendEntityMessage.HasField("entityOperationSignaled"):
                    expected_method_name = task.get_name(ctx.signal_entity)
                    raise _get_wrong_action_type_error(
                        entity_signal_id, expected_method_name, action
                    )
            elif event.HasField("entityLockRequested"):
                section_id = event.entityLockRequested.criticalSectionId
                task_id = event.eventId
                action = ctx._pending_actions.pop(task_id, None)
                entity_task = ctx._pending_tasks.get(task_id, None)
                if not action:
                    raise _get_non_determinism_error(
                        task_id, task.get_name(ctx.lock_entities)
                    )
                elif not action.HasField("sendEntityMessage") or not action.sendEntityMessage.HasField("entityLockRequested"):
                    expected_method_name = task.get_name(ctx.lock_entities)
                    raise _get_wrong_action_type_error(
                        task_id, expected_method_name, action
                    )
                ctx._entity_lock_id_map[section_id] = task_id
            elif event.HasField("entityUnlockSent"):
                # Remove the unlock tasks as they have already been processed
                tasks_to_remove = []
                for task_id, action in ctx._pending_actions.items():
                    if action.HasField("sendEntityMessage") and action.sendEntityMessage.HasField("entityUnlockSent"):
                        if action.sendEntityMessage.entityUnlockSent.criticalSectionId == event.entityUnlockSent.criticalSectionId:
                            tasks_to_remove.append(task_id)
                for task_to_remove in tasks_to_remove:
                    ctx._pending_actions.pop(task_to_remove, None)
            elif event.HasField("entityLockGranted"):
                section_id = event.entityLockGranted.criticalSectionId
                task_id = ctx._entity_lock_id_map.pop(section_id, None)
                if not task_id:
                    # Unexpected lock grant for unknown section; log and skip.
                    if not ctx.is_replaying:
                        self._logger.warning(
                            f"{ctx.instance_id}: Ignoring unexpected entityLockGranted event for criticalSectionId '{section_id}'."
                        )
                    return
                entity_task = ctx._pending_tasks.pop(task_id, None)
                if not entity_task:
                    if not ctx.is_replaying:
                        self._logger.warning(
                            f"{ctx.instance_id}: Ignoring unexpected entityLockGranted event for criticalSectionId '{section_id}'."
                        )
                    return
                ctx._entity_context.complete_acquire(section_id)
                entity_task.complete(EntityLock(ctx))
                ctx.resume()
            elif event.HasField("entityOperationCompleted"):
                request_id = event.entityOperationCompleted.requestId
                entity_id, operation, task_id = ctx._entity_task_id_map.pop(request_id, (None, None, None))
                if not entity_id:
                    raise RuntimeError(f"Could not parse entity ID from request ID '{request_id}'")
                if not task_id:
                    raise RuntimeError(f"Could not find matching task ID for entity operation with request ID '{request_id}'")
                entity_task = ctx._pending_tasks.pop(task_id, None)
                if not entity_task:
                    if not ctx.is_replaying:
                        self._logger.warning(
                            f"{ctx.instance_id}: Ignoring unexpected entityOperationCompleted event with request ID = {request_id}."
                        )
                    return
                result = None
                if not ph.is_empty(event.entityOperationCompleted.output):
                    result = shared.from_json(event.entityOperationCompleted.output.value)
                ctx._entity_context.recover_lock_after_call(entity_id)
                entity_task.complete(result)
                ctx.resume()
            elif event.HasField("entityOperationFailed"):
                request_id = event.entityOperationFailed.requestId
                entity_id, operation, task_id = ctx._entity_task_id_map.pop(request_id, (None, None, None))
                if not entity_id:
                    raise RuntimeError(f"Could not parse entity ID from request ID '{request_id}'")
                if operation is None:
                    raise RuntimeError(f"Could not parse operation name from request ID '{request_id}'")
                if not task_id:
                    raise RuntimeError(f"Could not find matching task ID for entity operation with request ID '{request_id}'")
                entity_task = ctx._pending_tasks.pop(task_id, None)
                if not entity_task:
                    if not ctx.is_replaying:
                        self._logger.warning(
                            f"{ctx.instance_id}: Ignoring unexpected entityOperationFailed event with request ID = {request_id}."
                        )
                    return
                failure = EntityOperationFailedException(
                    entity_id,
                    operation,
                    event.entityOperationFailed.failureDetails
                )
                ctx._entity_context.recover_lock_after_call(entity_id)
                entity_task.fail(str(failure), failure)
                ctx.resume()
            elif event.HasField("orchestratorCompleted"):
                # Added in Functions only (for some reason) and does not affect orchestrator flow
                pass
            elif event.HasField("eventSent"):
                # Check if this eventSent corresponds to an entity operation call after being translated to the old
                # entity protocol by the Durable WebJobs extension. If so, treat this message similarly to
                # entityOperationCalled and remove the pending action. Also store the entity id and event id for later
                action = ctx._pending_actions.pop(event.eventId, None)
                if action and action.HasField("sendEntityMessage"):
                    if action.sendEntityMessage.HasField("entityOperationCalled"):
                        entity_id, event_id = self._parse_entity_event_sent_input(event)
                        ctx._entity_task_id_map[event_id] = (entity_id, action.sendEntityMessage.entityOperationCalled.operation, event.eventId)
                    elif action.sendEntityMessage.HasField("entityLockRequested"):
                        entity_id, event_id = self._parse_entity_event_sent_input(event)
                        ctx._entity_lock_task_id_map[event_id] = (entity_id, event.eventId)
            else:
                eventType = event.WhichOneof("eventType")
                raise task.OrchestrationStateError(
                    f"Don't know how to handle event of type '{eventType}'"
                )
        except StopIteration as generatorStopped:
            # The orchestrator generator function completed
            ctx.set_complete(generatorStopped.value, pb.ORCHESTRATION_STATUS_COMPLETED)

    def _parse_entity_event_sent_input(self, event: pb.HistoryEvent) -> Tuple[EntityInstanceId, str]:
        try:
            entity_id = EntityInstanceId.parse(event.eventSent.instanceId)
        except ValueError:
            raise RuntimeError(f"Could not parse entity ID from instanceId '{event.eventSent.instanceId}'")
        try:
            event_id = json.loads(event.eventSent.input.value)["id"]
        except (json.JSONDecodeError, KeyError, TypeError) as ex:
            raise RuntimeError(f"Could not parse event ID from eventSent input '{event.eventSent.input.value}'") from ex
        return entity_id, event_id

    def _handle_entity_event_raised(self,
                                    ctx: _RuntimeOrchestrationContext,
                                    event: pb.HistoryEvent,
                                    entity_id: Optional[EntityInstanceId],
                                    task_id: Optional[int],
                                    is_lock_event: bool):
        # This eventRaised represents the result of an entity operation after being translated to the old
        # entity protocol by the Durable WebJobs extension
        if entity_id is None:
            raise RuntimeError(f"Could not retrieve entity ID for entity-related eventRaised with ID '{event.eventId}'")
        if task_id is None:
            raise RuntimeError(f"Could not retrieve task ID for entity-related eventRaised with ID '{event.eventId}'")
        entity_task = ctx._pending_tasks.pop(task_id, None)
        if not entity_task:
            raise RuntimeError(f"Could not retrieve entity task for entity-related eventRaised with ID '{event.eventId}'")
        result = None
        if not ph.is_empty(event.eventRaised.input):
            # TODO: Investigate why the event result is wrapped in a dict with "result" key
            result = shared.from_json(event.eventRaised.input.value)["result"]
        if is_lock_event:
            ctx._entity_context.complete_acquire(event.eventRaised.name)
            entity_task.complete(EntityLock(ctx))
        else:
            ctx._entity_context.recover_lock_after_call(entity_id)
            entity_task.complete(result)
        ctx.resume()

    def evaluate_orchestration_versioning(self, versioning: Optional[VersioningOptions], orchestration_version: Optional[str]) -> Optional[pb.TaskFailureDetails]:
        if versioning is None:
            return None
        version_comparison = self.compare_versions(orchestration_version, versioning.version)
        if versioning.match_strategy == VersionMatchStrategy.NONE:
            return None
        elif versioning.match_strategy == VersionMatchStrategy.STRICT:
            if version_comparison != 0:
                return pb.TaskFailureDetails(
                    errorType="VersionMismatch",
                    errorMessage=f"The orchestration version '{orchestration_version}' does not match the worker version '{versioning.version}'.",
                    isNonRetriable=True,
                )
        elif versioning.match_strategy == VersionMatchStrategy.CURRENT_OR_OLDER:
            if version_comparison > 0:
                return pb.TaskFailureDetails(
                    errorType="VersionMismatch",
                    errorMessage=f"The orchestration version '{orchestration_version}' is greater than the worker version '{versioning.version}'.",
                    isNonRetriable=True,
                )
        else:
            # If there is a type of versioning we don't understand, it is better to treat it as a versioning failure.
            return pb.TaskFailureDetails(
                errorType="VersionMismatch",
                errorMessage=f"The version match strategy '{versioning.match_strategy}' is unknown.",
                isNonRetriable=True,
            )

    def compare_versions(self, source_version: Optional[str], default_version: Optional[str]) -> int:
        if not source_version and not default_version:
            return 0
        if not source_version:
            return -1
        if not default_version:
            return 1
        try:
            source_version_parsed = parse(source_version)
            default_version_parsed = parse(default_version)
            return (source_version_parsed > default_version_parsed) - (source_version_parsed < default_version_parsed)
        except InvalidVersion:
            return (source_version > default_version) - (source_version < default_version)


class _ActivityExecutor:
    def __init__(self, registry: _Registry, logger: logging.Logger):
        self._registry = registry
        self._logger = logger

    def execute(
            self,
            orchestration_id: str,
            name: str,
            task_id: int,
            encoded_input: Optional[str],
    ) -> Optional[str]:
        """Executes an activity function and returns the serialized result, if any."""
        self._logger.debug(
            f"{orchestration_id}/{task_id}: Executing activity '{name}'..."
        )
        fn = self._registry.get_activity(name)
        if not fn:
            raise ActivityNotRegisteredError(
                f"Activity function named '{name}' was not registered!"
            )

        activity_input = shared.from_json(encoded_input) if encoded_input else None
        ctx = task.ActivityContext(orchestration_id, task_id)

        # Execute the activity function
        activity_output = fn(ctx, activity_input)

        encoded_output = (
            shared.to_json(activity_output) if activity_output is not None else None
        )
        chars = len(encoded_output) if encoded_output else 0
        self._logger.debug(
            f"{orchestration_id}/{task_id}: Activity '{name}' completed successfully with {chars} char(s) of encoded output."
        )
        return encoded_output


class _EntityExecutor:
    def __init__(self, registry: _Registry, logger: logging.Logger):
        self._registry = registry
        self._logger = logger
        self._entity_method_cache: dict[tuple[type, str], bool] = {}

    def execute(
            self,
            orchestration_id: str,
            entity_id: EntityInstanceId,
            operation: str,
            state: StateShim,
            encoded_input: Optional[str],
    ) -> Optional[str]:
        """Executes an entity function and returns the serialized result, if any."""
        self._logger.debug(
            f"{orchestration_id}: Executing entity '{entity_id}'..."
        )
        fn = self._registry.get_entity(entity_id.entity)
        if not fn:
            raise EntityNotRegisteredError(
                f"Entity function named '{entity_id.entity}' was not registered!"
            )

        entity_input = shared.from_json(encoded_input) if encoded_input else None
        ctx = EntityContext(orchestration_id, operation, state, entity_id)

        if isinstance(fn, type) and issubclass(fn, DurableEntity):
            entity_instance = fn()
            if not hasattr(entity_instance, operation):
                raise AttributeError(f"Entity '{entity_id}' does not have operation '{operation}'")
            method = getattr(entity_instance, operation)
            if not callable(method):
                raise TypeError(f"Entity operation '{operation}' is not callable")
            # Execute the entity method
            entity_instance._initialize_entity_context(ctx)
            cache_key = (type(entity_instance), operation)
            has_required_param = self._entity_method_cache.get(cache_key)
            if has_required_param is None:
                sig = inspect.signature(method)
                has_required_param = any(
                    p.default == inspect.Parameter.empty
                    for p in sig.parameters.values()
                    if p.kind not in (inspect.Parameter.VAR_POSITIONAL,
                                      inspect.Parameter.VAR_KEYWORD)
                )
                self._entity_method_cache[cache_key] = has_required_param
            if has_required_param or entity_input is not None:
                entity_output = method(entity_input)
            else:
                entity_output = method()
        else:
            # Execute the entity function
            entity_output = fn(ctx, entity_input)

        encoded_output = (
            shared.to_json(entity_output) if entity_output is not None else None
        )
        chars = len(encoded_output) if encoded_output else 0
        self._logger.debug(
            f"{orchestration_id}: Entity '{entity_id}' completed successfully with {chars} char(s) of encoded output."
        )
        return encoded_output


def _get_non_determinism_error(
        task_id: int, action_name: str
) -> task.NonDeterminismError:
    return task.NonDeterminismError(
        f"A previous execution called {action_name} with ID={task_id}, but the current "
        f"execution doesn't have this action with this ID. This problem occurs when either "
        f"the orchestration has non-deterministic logic or if the code was changed after an "
        f"instance of this orchestration already started running."
    )


def _get_wrong_action_type_error(
        task_id: int, expected_method_name: str, action: pb.OrchestratorAction
) -> task.NonDeterminismError:
    unexpected_method_name = _get_method_name_for_action(action)
    return task.NonDeterminismError(
        f"Failed to restore orchestration state due to a history mismatch: A previous execution called "
        f"{expected_method_name} with ID={task_id}, but the current execution is instead trying to call "
        f"{unexpected_method_name} as part of rebuilding it's history. This kind of mismatch can happen if an "
        f"orchestration has non-deterministic logic or if the code was changed after an instance of this "
        f"orchestration already started running."
    )


def _get_wrong_action_name_error(
        task_id: int, method_name: str, expected_task_name: str, actual_task_name: str
) -> task.NonDeterminismError:
    return task.NonDeterminismError(
        f"Failed to restore orchestration state due to a history mismatch: A previous execution called "
        f"{method_name} with name='{expected_task_name}' and sequence number {task_id}, but the current "
        f"execution is instead trying to call {actual_task_name} as part of rebuilding it's history. "
        f"This kind of mismatch can happen if an orchestration has non-deterministic logic or if the code "
        f"was changed after an instance of this orchestration already started running."
    )


def _get_method_name_for_action(action: pb.OrchestratorAction) -> str:
    action_type = action.WhichOneof("orchestratorActionType")
    if action_type == "scheduleTask":
        return task.get_name(task.OrchestrationContext.call_activity)
    elif action_type == "createTimer":
        return task.get_name(task.OrchestrationContext.create_timer)
    elif action_type == "createSubOrchestration":
        return task.get_name(task.OrchestrationContext.call_sub_orchestrator)
    # elif action_type == "sendEvent":
    #    return task.get_name(task.OrchestrationContext.send_event)
    else:
        raise NotImplementedError(f"Action type '{action_type}' not supported!")


def _get_new_event_summary(new_events: Sequence[pb.HistoryEvent]) -> str:
    """Returns a summary of the new events that can be used for logging."""
    if not new_events:
        return "[]"
    elif len(new_events) == 1:
        return f"[{new_events[0].WhichOneof('eventType')}]"
    else:
        counts: dict[str, int] = {}
        for event in new_events:
            event_type = event.WhichOneof("eventType")
            counts[event_type] = counts.get(event_type, 0) + 1
        return f"[{', '.join(f'{name}={count}' for name, count in counts.items())}]"


def _get_action_summary(new_actions: Sequence[pb.OrchestratorAction]) -> str:
    """Returns a summary of the new actions that can be used for logging."""
    if not new_actions:
        return "[]"
    elif len(new_actions) == 1:
        return f"[{new_actions[0].WhichOneof('orchestratorActionType')}]"
    else:
        counts: dict[str, int] = {}
        for action in new_actions:
            action_type = action.WhichOneof("orchestratorActionType")
            counts[action_type] = counts.get(action_type, 0) + 1
        return f"[{', '.join(f'{name}={count}' for name, count in counts.items())}]"


def _is_suspendable(event: pb.HistoryEvent) -> bool:
    """Returns true if the event is one that can be suspended and resumed."""
    return event.WhichOneof("eventType") not in [
        "executionResumed",
        "executionTerminated",
    ]


class _AsyncWorkerManager:
    def __init__(self, concurrency_options: ConcurrencyOptions, logger: logging.Logger):
        self.concurrency_options = concurrency_options
        self._logger = logger

        self.activity_semaphore = None
        self.orchestration_semaphore = None
        self.entity_semaphore = None
        # Don't create queues here - defer until we have an event loop
        self.activity_queue: Optional[asyncio.Queue] = None
        self.orchestration_queue: Optional[asyncio.Queue] = None
        self.entity_batch_queue: Optional[asyncio.Queue] = None
        self._queue_event_loop: Optional[asyncio.AbstractEventLoop] = None
        # Store work items when no event loop is available
        self._pending_activity_work: list = []
        self._pending_orchestration_work: list = []
        self._pending_entity_batch_work: list = []
        self.thread_pool = ThreadPoolExecutor(
            max_workers=concurrency_options.maximum_thread_pool_workers,
            thread_name_prefix="DurableTask",
        )
        self._shutdown = False

    def _ensure_queues_for_current_loop(self):
        """Ensure queues are bound to the current event loop."""
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            # No event loop running, can't create queues
            return

        # Check if queues are already properly set up for current loop
        if self._queue_event_loop is current_loop:
            if self.activity_queue is not None and self.orchestration_queue is not None and self.entity_batch_queue is not None:
                # Queues are already bound to the current loop and exist
                return

        # Need to recreate queues for the current event loop
        # First, preserve any existing work items
        existing_activity_items = []
        existing_orchestration_items = []
        existing_entity_batch_items = []

        if self.activity_queue is not None:
            try:
                while not self.activity_queue.empty():
                    existing_activity_items.append(self.activity_queue.get_nowait())
            except Exception:
                pass

        if self.orchestration_queue is not None:
            try:
                while not self.orchestration_queue.empty():
                    existing_orchestration_items.append(
                        self.orchestration_queue.get_nowait()
                    )
            except Exception:
                pass

        if self.entity_batch_queue is not None:
            try:
                while not self.entity_batch_queue.empty():
                    existing_entity_batch_items.append(
                        self.entity_batch_queue.get_nowait()
                    )
            except Exception:
                pass

        # Create fresh queues for the current event loop
        self.activity_queue = asyncio.Queue()
        self.orchestration_queue = asyncio.Queue()
        self.entity_batch_queue = asyncio.Queue()
        self._queue_event_loop = current_loop

        # Restore the work items to the new queues
        for item in existing_activity_items:
            self.activity_queue.put_nowait(item)
        for item in existing_orchestration_items:
            self.orchestration_queue.put_nowait(item)
        for item in existing_entity_batch_items:
            self.entity_batch_queue.put_nowait(item)

        # Move pending work items to the queues
        for item in self._pending_activity_work:
            self.activity_queue.put_nowait(item)
        for item in self._pending_orchestration_work:
            self.orchestration_queue.put_nowait(item)
        for item in self._pending_entity_batch_work:
            self.entity_batch_queue.put_nowait(item)

        # Clear the pending work lists
        self._pending_activity_work.clear()
        self._pending_orchestration_work.clear()
        self._pending_entity_batch_work.clear()

    async def run(self):
        # Reset shutdown flag in case this manager is being reused
        self._shutdown = False

        # Ensure queues are properly bound to the current event loop
        self._ensure_queues_for_current_loop()

        # Create semaphores in the current event loop
        self.activity_semaphore = asyncio.Semaphore(
            self.concurrency_options.maximum_concurrent_activity_work_items
        )
        self.orchestration_semaphore = asyncio.Semaphore(
            self.concurrency_options.maximum_concurrent_orchestration_work_items
        )
        self.entity_semaphore = asyncio.Semaphore(
            self.concurrency_options.maximum_concurrent_entity_work_items
        )

        # Start background consumers for each work type
        try:
            if self.activity_queue is not None and self.orchestration_queue is not None \
                    and self.entity_batch_queue is not None:
                await asyncio.gather(
                    self._consume_queue(self.activity_queue, self.activity_semaphore),
                    self._consume_queue(
                        self.orchestration_queue, self.orchestration_semaphore
                    ),
                    self._consume_queue(
                        self.entity_batch_queue, self.entity_semaphore
                    )
                )
        except Exception as queue_exception:
            self._logger.error(f"Shutting down worker - Uncaught error in worker manager: {queue_exception}")
            while self.activity_queue is not None and not self.activity_queue.empty():
                try:
                    func, cancellation_func, args, kwargs = self.activity_queue.get_nowait()
                    await self._run_func(cancellation_func, *args, **kwargs)
                    self._logger.error(f"Activity work item args: {args}, kwargs: {kwargs}")
                except asyncio.QueueEmpty:
                    # Queue was empty, no cancellation needed
                    pass
                except Exception as cancellation_exception:
                    self._logger.error(f"Uncaught error while cancelling activity work item: {cancellation_exception}")
            while self.orchestration_queue is not None and not self.orchestration_queue.empty():
                try:
                    func, cancellation_func, args, kwargs = self.orchestration_queue.get_nowait()
                    await self._run_func(cancellation_func, *args, **kwargs)
                    self._logger.error(f"Orchestration work item args: {args}, kwargs: {kwargs}")
                except asyncio.QueueEmpty:
                    # Queue was empty, no cancellation needed
                    pass
                except Exception as cancellation_exception:
                    self._logger.error(f"Uncaught error while cancelling orchestration work item: {cancellation_exception}")
            while self.entity_batch_queue is not None and not self.entity_batch_queue.empty():
                try:
                    func, cancellation_func, args, kwargs = self.entity_batch_queue.get_nowait()
                    await self._run_func(cancellation_func, *args, **kwargs)
                    self._logger.error(f"Entity batch work item args: {args}, kwargs: {kwargs}")
                except asyncio.QueueEmpty:
                    # Queue was empty, no cancellation needed
                    pass
                except Exception as cancellation_exception:
                    self._logger.error(f"Uncaught error while cancelling entity batch work item: {cancellation_exception}")
            self.shutdown()

    async def _consume_queue(self, queue: asyncio.Queue, semaphore: asyncio.Semaphore):
        # List to track running tasks
        running_tasks: set[asyncio.Task] = set()

        while True:
            # Clean up completed tasks
            done_tasks = {task for task in running_tasks if task.done()}
            running_tasks -= done_tasks

            # Exit if shutdown is set and the queue is empty and no tasks are running
            if self._shutdown and queue.empty() and not running_tasks:
                break

            try:
                work = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            func, cancellation_func, args, kwargs = work
            # Create a concurrent task for processing
            task = asyncio.create_task(
                self._process_work_item(semaphore, queue, func, cancellation_func, args, kwargs)
            )
            running_tasks.add(task)

    async def _process_work_item(
            self, semaphore: asyncio.Semaphore, queue: asyncio.Queue, func, cancellation_func, args, kwargs
    ):
        async with semaphore:
            try:
                await self._run_func(func, *args, **kwargs)
            except Exception as work_exception:
                self._logger.error(f"Uncaught error while processing work item, item will be abandoned: {work_exception}")
                await self._run_func(cancellation_func, *args, **kwargs)
            finally:
                queue.task_done()

    async def _run_func(self, func, *args, **kwargs):
        if inspect.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            loop = asyncio.get_running_loop()
            # Avoid submitting to executor after shutdown
            if (
                    getattr(self, "_shutdown", False) and getattr(self, "thread_pool", None) and getattr(
                        self.thread_pool, "_shutdown", False)
            ):
                return None
            return await loop.run_in_executor(
                self.thread_pool, lambda: func(*args, **kwargs)
            )

    def submit_activity(self, func, cancellation_func, *args, **kwargs):
        if self._shutdown:
            raise RuntimeError("Cannot submit new work items after shutdown has been initiated.")
        work_item = (func, cancellation_func, args, kwargs)
        self._ensure_queues_for_current_loop()
        if self.activity_queue is not None:
            self.activity_queue.put_nowait(work_item)
        else:
            # No event loop running, store in pending list
            self._pending_activity_work.append(work_item)

    def submit_orchestration(self, func, cancellation_func, *args, **kwargs):
        if self._shutdown:
            raise RuntimeError("Cannot submit new work items after shutdown has been initiated.")
        work_item = (func, cancellation_func, args, kwargs)
        self._ensure_queues_for_current_loop()
        if self.orchestration_queue is not None:
            self.orchestration_queue.put_nowait(work_item)
        else:
            # No event loop running, store in pending list
            self._pending_orchestration_work.append(work_item)

    def submit_entity_batch(self, func, cancellation_func, *args, **kwargs):
        if self._shutdown:
            raise RuntimeError("Cannot submit new work items after shutdown has been initiated.")
        work_item = (func, cancellation_func, args, kwargs)
        self._ensure_queues_for_current_loop()
        if self.entity_batch_queue is not None:
            self.entity_batch_queue.put_nowait(work_item)
        else:
            # No event loop running, store in pending list
            self._pending_entity_batch_work.append(work_item)

    def shutdown(self):
        self._shutdown = True
        self.thread_pool.shutdown(wait=True)

    async def reset_for_new_run(self):
        """Reset the manager state for a new run."""
        self._shutdown = False
        # Clear any existing queues - they'll be recreated when needed
        if self.activity_queue is not None:
            # Clear existing queue by creating a new one
            # This ensures no items from previous runs remain
            try:
                while not self.activity_queue.empty():
                    func, cancellation_func, args, kwargs = self.activity_queue.get_nowait()
                    await self._run_func(cancellation_func, *args, **kwargs)
            except Exception as reset_exception:
                self._logger.warning(f"Error while clearing activity queue during reset: {reset_exception}")
        if self.orchestration_queue is not None:
            try:
                while not self.orchestration_queue.empty():
                    func, cancellation_func, args, kwargs = self.orchestration_queue.get_nowait()
                    await self._run_func(cancellation_func, *args, **kwargs)
            except Exception as reset_exception:
                self._logger.warning(f"Error while clearing orchestration queue during reset: {reset_exception}")
        if self.entity_batch_queue is not None:
            try:
                while not self.entity_batch_queue.empty():
                    func, cancellation_func, args, kwargs = self.entity_batch_queue.get_nowait()
                    await self._run_func(cancellation_func, *args, **kwargs)
            except Exception as reset_exception:
                self._logger.warning(f"Error while clearing entity queue during reset: {reset_exception}")
        # Clear pending work lists
        self._pending_activity_work.clear()
        self._pending_orchestration_work.clear()
        self._pending_entity_batch_work.clear()


# Export public API
__all__ = ["ConcurrencyOptions", "TaskHubGrpcWorker"]
