import concurrent.futures
import logging
import time
import grpc

from abc import ABC
from threading import Event, Thread

from google.protobuf import empty_pb2

from durabletask.protos.orchestrator_service_pb2_grpc import TaskHubSidecarServiceStub

import durabletask.protos.orchestrator_service_pb2 as pb
import durabletask.protos.helpers as pbh
import durabletask.internal.shared as shared


class TaskHubWorker(ABC):
    pass


class TaskHubGrpcWorker(TaskHubWorker):
    response_stream: grpc.Future

    def __init__(self, *,
                 host_address: str | None = None,
                 log_handler=None,
                 log_formatter: logging.Formatter | None = None):
        if host_address is None:
            host_address = shared.get_default_host_address()
        self._host_address = host_address
        self._logger = shared.get_logger(log_handler, log_formatter)
        self._shutdown = Event()

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
                        self.response_stream = stub.GetWorkItems(pb.GetWorkItemsRequest())
                        self._logger.info(f'Successfully connected to {self._host_address}. Waiting for work items...')

                        # The stream blocks until either a work item is received or the stream is canceled
                        # by another thread (see the stop() method).
                        for work_item in self.response_stream:
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
        self._logger.info(f"Stopping gRPC worker...")
        self._shutdown.set()
        if self.response_stream is not None:
            self.response_stream.cancel()
        if self._runLoop is not None:
            self._runLoop.join(timeout=30)
        self._logger.info("Worker shutdown completed")

    def _execute_orchestrator(self, req: pb.OrchestratorRequest, stub: TaskHubSidecarServiceStub):
        try:
            complete_action = pbh.new_complete_orchestration_action(0, pb.ORCHESTRATION_STATUS_COMPLETED)
            actions = [complete_action]
            res = pb.OrchestratorResponse(instanceId=req.instanceId, actions=actions)
            stub.CompleteOrchestratorTask(res)
        except Exception as ex:
            self._logger.exception(f"An error occurred while trying to execute instance '{req.instanceId}': {ex}")

    def _execute_activity(self, req: pb.ActivityRequest, stub: TaskHubSidecarServiceStub):
        raise NotImplementedError()
