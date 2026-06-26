# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from durabletask import task
from durabletask.entities import EntityInstanceId


@dataclass
class ScheduleOperationRequest:
    """Request describing an operation to execute against a schedule entity.

    A plain dataclass: the serializer round-trips it (and its ``input`` payload)
    automatically. ``input`` stays an ``Any`` here -- it is reconstructed into the
    concrete options type at the entity-method boundary from that method's
    parameter annotation.
    """

    entity_id: str
    operation_name: str
    input: Any | None = None


def execute_schedule_operation_orchestrator(
        ctx: task.OrchestrationContext,
        request: ScheduleOperationRequest) -> Generator[task.Task[Any], Any, Any]:
    """Orchestrator that executes a single operation on a schedule entity.

    Client-side write operations route through this orchestrator so callers can await
    completion (and surface failures) of the underlying entity operation.
    """
    entity_id = EntityInstanceId.parse(request.entity_id)
    result = yield ctx.call_entity(entity_id, request.operation_name, request.input)
    return result
