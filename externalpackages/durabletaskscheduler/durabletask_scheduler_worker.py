import concurrent.futures
from threading import Thread
from google.protobuf import empty_pb2
import grpc
import durabletask.internal.orchestrator_service_pb2 as pb
import durabletask.internal.orchestrator_service_pb2_grpc as stubs
import durabletask.internal.shared as shared
from typing import Optional

from durabletask.worker import TaskHubGrpcWorker
from externalpackages.durabletaskscheduler.access_token_manager import AccessTokenManager

class DurableTaskSchedulerWorker(TaskHubGrpcWorker):
    def __init__(self, *args, 
                 metadata: Optional[list[tuple[str, str]]] = None,
                 client_id: Optional[str] = None,
                 taskhub: str,
                 **kwargs):
        if metadata is None:
            metadata = []  # Ensure metadata is initialized
        self._metadata = metadata
        self._client_id = client_id
        self._metadata.append(("taskhub", taskhub))
        self._access_token_manager = AccessTokenManager(client_id=self._client_id)
        self.__update_metadata_with_token()
        super().__init__(*args, metadata=self._metadata, **kwargs)


    def __update_metadata_with_token(self):
        """
        Add or update the `authorization` key in the metadata with the current access token.
        """
        token = self._access_token_manager.get_access_token()

        # Ensure that self._metadata is initialized
        if self._metadata is None:
            self._metadata = []  # Initialize it if it's still None
        
        # Check if "authorization" already exists in the metadata
        updated = False
        for i, (key, _) in enumerate(self._metadata):
            if key == "authorization":
                self._metadata[i] = ("authorization", token)
                updated = True
                break
        
        # If not updated, add a new entry
        if not updated:
            self._metadata.append(("authorization", token))

    def start(self):
        """Starts the worker on a background thread and begins listening for work items."""
        channel = shared.get_grpc_channel(self._host_address, self._metadata, self._secure_channel)
        stub = stubs.TaskHubSidecarServiceStub(channel)

        if self._is_running:
            raise RuntimeError('The worker is already running.')

        def run_loop():
            # TODO: Investigate whether asyncio could be used to enable greater concurrency for async activity
            #       functions. We'd need to know ahead of time whether a function is async or not.
            # TODO: Max concurrency configuration settings
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
                while not self._shutdown.is_set():
                    try:
                        self.__update_metadata_with_token()
                        # send a "Hello" message to the sidecar to ensure that it's listening
                        stub.Hello(empty_pb2.Empty())

                        # stream work items
                        self._response_stream = stub.GetWorkItems(pb.GetWorkItemsRequest())
                        self._logger.info(f'Successfully connected to {self._host_address}. Waiting for work items...')

                        # The stream blocks until either a work item is received or the stream is canceled
                        # by another thread (see the stop() method).
                        for work_item in self._response_stream:  # type: ignore
                            request_type = work_item.WhichOneof('request')
                            self._logger.debug(f'Received "{request_type}" work item')
                            if work_item.HasField('orchestratorRequest'):
                                executor.submit(self._execute_orchestrator, work_item.orchestratorRequest, stub, work_item.completionToken)
                            elif work_item.HasField('activityRequest'):
                                executor.submit(self._execute_activity, work_item.activityRequest, stub, work_item.completionToken)
                            elif work_item.HasField('healthPing'):
                                pass # no-op
                            else:
                                self._logger.warning(f'Unexpected work item type: {request_type}')

                    except grpc.RpcError as rpc_error:
                        if rpc_error.code() == grpc.StatusCode.CANCELLED:  # type: ignore
                            self._logger.info(f'Disconnected from {self._host_address}')
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

        self._logger.info(f"Starting gRPC worker that connects to {self._host_address}")
        self._runLoop = Thread(target=run_loop)
        self._runLoop.start()
        self._is_running = True
