# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import base64
from threading import Event
from typing import Optional
from durabletask import task
from durabletask.internal.orchestrator_service_pb2 import EntityBatchRequest, EntityBatchResult, OrchestratorRequest, OrchestratorResponse
from durabletask.worker import _Registry, ConcurrencyOptions
from durabletask.internal import shared
from durabletask.worker import TaskHubGrpcWorker
from durabletask.azurefunctions.internal.azurefunctions_null_stub import AzureFunctionsNullStub


# Worker class used for Durable Task Scheduler (DTS)
class DurableFunctionsWorker(TaskHubGrpcWorker):
    """A worker that can execute orchestrator and entity functions in the context of Azure Functions.

    Used internally by the Durable Functions Python SDK, and should not be visible to functionapps directly.
    See TaskHubGrpcWorker for base class documentation.
    """

    def __init__(self):
        # Don't call the parent constructor - we don't actually want to start an AsyncWorkerLoop
        # or recieve work items from anywhere but the method that is creating this worker
        self._registry = _Registry()
        self._host_address = ""
        self._logger = shared.get_logger("worker")
        self._shutdown = Event()
        self._is_running = False
        self._secure_channel = False

        self._concurrency_options = ConcurrencyOptions()

        self._interceptors = None

    def add_named_orchestrator(self, name: str, func: task.Orchestrator):
        self._registry.add_named_orchestrator(name, func)

    def _execute_orchestrator(self, func: task.Orchestrator, context) -> str:
        context_body = getattr(context, "body", None)
        if context_body is None:
            context_body = context
        orchestration_context = context_body
        request = OrchestratorRequest()
        request.ParseFromString(base64.b64decode(orchestration_context))
        stub = AzureFunctionsNullStub()
        response: Optional[OrchestratorResponse] = None

        def stub_complete(stub_response):
            nonlocal response
            response = stub_response
        stub.CompleteOrchestratorTask = stub_complete
        execution_started_events = []
        for e in request.pastEvents:
            if e.HasField("executionStarted"):
                execution_started_events.append(e)
        for e in request.newEvents:
            if e.HasField("executionStarted"):
                execution_started_events.append(e)
        if len(execution_started_events) == 0:
            raise Exception("No ExecutionStarted event found in orchestration request.")

        function_name = execution_started_events[-1].executionStarted.name
        self.add_named_orchestrator(function_name, func)
        super()._execute_orchestrator(request, stub, None)

        if response is None:
            raise Exception("Orchestrator execution did not produce a response.")
        # The Python worker returns the input as type "json", so double-encoding is necessary
        return base64.b64encode(response.SerializeToString()).decode('utf-8')

    def _execute_entity_batch(self, func: task.Entity, context) -> str:
        context_body = getattr(context, "body", None)
        if context_body is None:
            context_body = context
        orchestration_context = context_body
        request = EntityBatchRequest()
        request.ParseFromString(base64.b64decode(orchestration_context))
        stub = AzureFunctionsNullStub()
        response: Optional[EntityBatchResult] = None

        def stub_complete(stub_response: EntityBatchResult):
            nonlocal response
            response = stub_response
        stub.CompleteEntityTask = stub_complete

        self.add_entity(func)
        super()._execute_entity_batch(request, stub, None)

        if response is None:
            raise Exception("Entity execution did not produce a response.")
        # The Python worker returns the input as type "json", so double-encoding is necessary
        return base64.b64encode(response.SerializeToString()).decode('utf-8')
