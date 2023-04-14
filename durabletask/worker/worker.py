# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import concurrent.futures
import logging
from abc import ABC
from threading import Event, Thread

import grpc
from google.protobuf import empty_pb2

import durabletask.internal.shared as shared
import durabletask.protos.helpers as pbh
import durabletask.protos.orchestrator_service_pb2 as pb
import durabletask.task.execution as execution
from durabletask.protos.orchestrator_service_pb2_grpc import \
    TaskHubSidecarServiceStub
from durabletask.task.registry import Registry


class TaskHubWorker(ABC):
    pass


class TaskHubGrpcWorker(TaskHubWorker):
    _response_stream: grpc.Future | None

    def __init__(self, registry: Registry, *,
                 host_address: str | None = None,
                 log_handler=None,
                 log_formatter: logging.Formatter | None = None):
        self._registry = registry
        self._host_address = host_address if host_address else shared.get_default_host_address()
        self._logger = shared.get_logger(log_handler, log_formatter)
        self._shutdown = Event()
        self._response_stream = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def start(self):
        channel = shared.get_grpc_channel(self._host_address)
        stub = TaskHubSidecarServiceStub(channel)

        def run_loop():
            # TODO: Investigate whether asyncio could be used to enable greater concurrency for async activity
            #       functions. We'd need to know ahead of time whether a function is async or not.
            # TODO: Max concurrency configuration settings
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
                while not self._shutdown.is_set():
                    try:
                        # send a "Hello" message to the sidecar to ensure that it's listening
                        stub.Hello(empty_pb2.Empty())

                        # stream work items
                        self._response_stream = stub.GetWorkItems(pb.GetWorkItemsRequest())
                        self._logger.info(f'Successfully connected to {self._host_address}. Waiting for work items...')

                        # The stream blocks until either a work item is received or the stream is canceled
                        # by another thread (see the stop() method).
                        for work_item in self._response_stream:
                            self._logger.info(f'Got work item: {work_item}')
                            if work_item.HasField('orchestratorRequest'):
                                executor.submit(self._execute_orchestrator, work_item.orchestratorRequest, stub)
                            elif work_item.HasField('activityRequest'):
                                executor.submit(self._execute_activity, work_item.activityRequest, stub)
                            else:
                                request_type = work_item.WhichOneof('request')
                                self._logger.warning(f'Unexpected work item type: {request_type}')

                    except grpc.RpcError as rpc_error:
                        if rpc_error.code() == grpc.StatusCode.CANCELLED:  # type: ignore
                            self._logger.warning(f'Disconnected from {self._host_address}')
                        elif rpc_error.code() == grpc.StatusCode.UNAVAILABLE:  # type: ignore
                            self._logger.warning(
                                f'The sidecar at address {self._host_address} is unavailable - will continue retrying')
                        else:
                            self._logger.warning(f'Unexpected error: {rpc_error}')
                    except Exception as ex:
                        self._logger.warning(f'Unexpected error: {ex}')

                    # CONSIDER: exponential backoff
                    self._shutdown.wait(5)
                self._logger.info("No longer listening for work items")
                return

        self._logger.info(f"starting gRPC worker that connects to {self._host_address}")
        self._runLoop = Thread(target=run_loop)
        self._runLoop.start()

    def stop(self):
        self._logger.info("Stopping gRPC worker...")
        self._shutdown.set()
        if self._response_stream is not None:
            self._response_stream.cancel()
        if self._runLoop is not None:
            self._runLoop.join(timeout=30)
        self._logger.info("Worker shutdown completed")

    def _execute_orchestrator(self, req: pb.OrchestratorRequest, stub: TaskHubSidecarServiceStub):
        try:
            executor = execution.OrchestrationExecutor(self._registry, self._logger)
            actions = executor.execute(req.instanceId, req.pastEvents, req.newEvents)
            res = pb.OrchestratorResponse(instanceId=req.instanceId, actions=actions)
        except Exception as ex:
            self._logger.exception(f"An error occurred while trying to execute instance '{req.instanceId}': {ex}")
            failure_details = pbh.new_failure_details(ex)
            actions = [pbh.new_complete_orchestration_action(-1, pb.ORCHESTRATION_STATUS_FAILED, "", failure_details)]
            res = pb.OrchestratorResponse(instanceId=req.instanceId, actions=actions)

        try:
            stub.CompleteOrchestratorTask(res)
        except Exception as ex:
            self._logger.exception(f"Failed to deliver orchestrator response for '{req.instanceId}' to sidecar: {ex}")

    def _execute_activity(self, req: pb.ActivityRequest, stub: TaskHubSidecarServiceStub):
        instance_id = req.orchestrationInstance.instanceId
        try:
            executor = execution.ActivityExecutor(self._registry, self._logger)
            result = executor.execute(instance_id, req.name, req.taskId, req.input.value)
            res = pb.ActivityResponse(
                instanceId=instance_id,
                taskId=req.taskId,
                result=pbh.get_string_value(result))
        except Exception as ex:
            res = pb.ActivityResponse(
                instanceId=instance_id,
                taskId=req.taskId,
                failureDetails=pbh.new_failure_details(ex))

        try:
            stub.CompleteActivityTask(res)
        except Exception as ex:
            self._logger.exception(
                f"Failed to deliver activity response for '{req.name}#{req.taskId}' of orchestration ID '{instance_id}' to sidecar: {ex}")
