# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from collections.abc import Generator
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from durabletask import task
from durabletask.entities import EntityInstanceId


@dataclass
class ScheduleOperationRequest:
    """Request describing an operation to execute against a schedule entity."""

    entity_id: str
    operation_name: str
    input: Any | None = None


def execute_schedule_operation_orchestrator(
        ctx: task.OrchestrationContext, request: Any) -> Generator[task.Task[Any], Any, Any]:
    """Orchestrator that executes a single operation on a schedule entity.

    Client-side write operations route through this orchestrator so callers can await
    completion (and surface failures) of the underlying entity operation.
    """
    if isinstance(request, SimpleNamespace):
        request = vars(request)
    if isinstance(request, dict):
        request = ScheduleOperationRequest(**request)

    entity_id = EntityInstanceId.parse(request.entity_id)
    result = yield ctx.call_entity(entity_id, request.operation_name, request.input)
    return result
