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

    ``input`` is typed ``Any``, so it is reconstructed as the raw deserialized
    payload; the concrete options type is rebuilt later, at the entity-method
    boundary, from that method's parameter annotation.

    The ``to_json`` / ``from_json`` hooks mirror the plain dataclass field
    mapping so the wire format is unchanged for the default JSON converter,
    while also making the type serializable by converters that require an
    explicit hook (for example the Azure Functions ``df`` codec, which cannot
    serialize a bare dataclass). This matches the sibling schedule models
    (``ScheduleState``, ``ScheduleCreationOptions``), which already define these
    hooks.
    """

    entity_id: str
    operation_name: str
    input: Any | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "entity_id": self.entity_id,
            "operation_name": self.operation_name,
            "input": self.input,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "ScheduleOperationRequest":
        return cls(
            entity_id=data["entity_id"],
            operation_name=data["operation_name"],
            input=data.get("input"),
        )


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
