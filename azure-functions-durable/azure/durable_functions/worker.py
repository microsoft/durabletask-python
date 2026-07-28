# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import base64
from threading import Lock
from typing import Any, Optional

from durabletask import task
from durabletask.internal.orchestrator_service_pb2 import (
    EntityBatchRequest,
    EntityBatchResult,
    HistoryEvent,
    OrchestratorRequest,
    OrchestratorResponse,
)
from durabletask.worker import TaskHubGrpcWorker
from .internal.azurefunctions_null_stub import AzureFunctionsNullStub
from .internal.compat.entity_context import wrap_entity
from .internal.compat.orchestration_context import wrap_orchestrator
from .internal.serialization import DEFAULT_FUNCTIONS_DATA_CONVERTER


# Host-driven Azure Functions execution engine for orchestrator and entity
# functions. Despite subclassing ``TaskHubGrpcWorker``, it never opens a gRPC
# channel or streams work items from a sidecar: the Functions host delivers work
# items directly to the ``execute_*`` methods below.
class DurableFunctionsWorker(TaskHubGrpcWorker):
    """A worker that can execute orchestrator and entity functions in the context of Azure Functions.

    Used internally by the Durable Functions Python SDK, and should not be visible to functionapps directly.
    See TaskHubGrpcWorker for base class documentation.
    """

    def __init__(self) -> None:
        # We never start the worker loop or open a gRPC channel. The base
        # constructor only initialises in-memory state (registry, logger,
        # concurrency options, payload store, etc.) that the inherited
        # ``_execute_*`` methods rely on; work items are delivered directly by
        # the methods below rather than streamed from a sidecar.
        #
        # The Functions converter routes payload serialization through the
        # azure-functions codec (df_dumps/df_loads) so user types round-trip in
        # the wire format the Durable Functions host extension expects.
        super().__init__(data_converter=DEFAULT_FUNCTIONS_DATA_CONVERTER)
        self._registration_lock = Lock()

    def add_named_orchestrator(self, name: str, func: task.Orchestrator[Any, Any]) -> None:
        self._registry.add_named_orchestrator(name, func)

    def _register_orchestrator_once(
            self,
            name: str,
            func: task.Orchestrator[Any, Any],
    ) -> None:
        with self._registration_lock:
            if self._registry.get_orchestrator(name) is None:
                self.add_named_orchestrator(name, wrap_orchestrator(func))

    def _register_entity_once(self, func: task.Entity[Any, Any]) -> None:
        name = task.get_entity_name(func).lower()
        with self._registration_lock:
            if self._registry.get_entity(name) is None:
                self.add_entity(wrap_entity(func), name=name)

    def execute_orchestration_request(self, func: task.Orchestrator[Any, Any], context: Any) -> str:
        context_body = getattr(context, "body", None)
        if context_body is None:
            context_body = context
        orchestration_context = context_body
        request = OrchestratorRequest()
        request.ParseFromString(base64.b64decode(orchestration_context))
        stub: Any = AzureFunctionsNullStub()
        response: Optional[OrchestratorResponse] = None

        def stub_complete(stub_response: OrchestratorResponse) -> None:
            nonlocal response
            response = stub_response
        stub.CompleteOrchestratorTask = stub_complete
        execution_started_events: list[HistoryEvent] = []
        for e in request.pastEvents:
            if e.HasField("executionStarted"):
                execution_started_events.append(e)
        for e in request.newEvents:
            if e.HasField("executionStarted"):
                execution_started_events.append(e)
        if len(execution_started_events) == 0:
            raise ValueError("No ExecutionStarted event found in orchestration request.")

        function_name = execution_started_events[-1].executionStarted.name
        self._register_orchestrator_once(function_name, func)
        super()._execute_orchestrator(request, stub, None)

        if response is None:
            raise RuntimeError("Orchestrator execution did not produce a response.")
        # Return the protobuf response serialized and base64-encoded, the exact
        # format the Durable Functions host expects.
        return base64.b64encode(response.SerializeToString()).decode("utf-8")

    def execute_entity_batch_request(self, func: task.Entity[Any, Any], context: Any) -> str:
        context_body = getattr(context, "body", None)
        if context_body is None:
            context_body = context
        orchestration_context = context_body
        request = EntityBatchRequest()
        request.ParseFromString(base64.b64decode(orchestration_context))
        stub: Any = AzureFunctionsNullStub()
        response: Optional[EntityBatchResult] = None

        def stub_complete(stub_response: EntityBatchResult) -> None:
            nonlocal response
            response = stub_response
        stub.CompleteEntityTask = stub_complete

        self._register_entity_once(func)
        super()._execute_entity_batch(request, stub, None)

        if response is None:
            raise RuntimeError("Entity execution did not produce a response.")
        # Return the protobuf response serialized and base64-encoded, the exact
        # format the Durable Functions host expects.
        return base64.b64encode(response.SerializeToString()).decode("utf-8")
