# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from collections.abc import Generator
from dataclasses import dataclass
from typing import Any, cast

from durabletask import task
from durabletask.entities import EntityInstanceId
from durabletask.scheduled import transitions
from durabletask.scheduled.models import (ScheduleCreationOptions,
                                          ScheduleUpdateOptions)

# Operations whose ``input`` is a typed options object. Any other operation
# carries a native payload (a token string, or nothing), so its input is left
# as the raw deserialized value.
_OPERATION_INPUT_TYPES: dict[str, type] = {
    transitions.CREATE_SCHEDULE: ScheduleCreationOptions,
    transitions.UPDATE_SCHEDULE: ScheduleUpdateOptions,
}


@dataclass
class ScheduleOperationRequest:
    """Request describing an operation to execute against a schedule entity.

    ``input`` is typed ``Any`` but, for the operations that carry a typed
    options object (create/update), the concrete type is reconstructed in
    :meth:`from_json` from ``operation_name``. Serialization keeps the wire
    payload JSON-native: :meth:`to_json` emits the options as a plain mapping
    (via their own ``to_json`` hook) rather than as a nested custom-object
    envelope, which a strict-typing serializer would otherwise be unable to
    encode. The reconstructed options object is then handed to ``call_entity``,
    where it round-trips to the entity method as its declared type.
    """

    entity_id: str
    operation_name: str
    input: Any | None = None

    def to_json(self) -> dict[str, Any]:
        raw_input: Any = self.input
        if hasattr(raw_input, "to_json"):
            raw_input = raw_input.to_json()
        return {
            "entity_id": self.entity_id,
            "operation_name": self.operation_name,
            "input": raw_input,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "ScheduleOperationRequest":
        operation_name = data["operation_name"]
        raw_input: Any = data.get("input")
        input_type = _OPERATION_INPUT_TYPES.get(operation_name)
        if input_type is not None and isinstance(raw_input, dict):
            raw_input = cast(Any, input_type).from_json(raw_input)
        return cls(
            entity_id=data["entity_id"],
            operation_name=operation_name,
            input=raw_input,
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
