# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Entity functions for the durabletask-native-style sample app (blueprint).

Exercises both durabletask-native entity authoring styles:

- ``Counter`` uses the **class-based** style (``DurableEntity`` subclass with
  one method per operation and ``self.get_state``/``self.set_state``).
- ``probe`` uses the **function-based** two-argument style
  (``def entity(ctx, input):``) with the durabletask ``EntityContext`` API.
"""

from typing import Any

import azure.durable_functions as df
from durabletask import entities
from durabletask.entities import DurableEntity

bp = df.Blueprint()


@bp.entity_trigger(context_name="context")
class Counter(DurableEntity):
    """Class-based counter entity.

    Registered under the lowercased class name (``counter``); each operation is
    a method taking ``(self, input)``.
    """

    def add(self, input: Any = None) -> int:
        new_state = self.get_state(int, 0) + (input or 0)
        self.set_state(new_state)
        return new_state

    def reset(self, input: Any = None) -> None:
        # Returns None implicitly: verifies that a None entity operation result
        # round-trips through ``call_entity`` (rather than being lost or
        # surfacing as an error).
        self.set_state(0)

    def get(self, input: Any = None) -> int:
        return self.get_state(int, 0)


@bp.entity_trigger(context_name="context")
def probe(ctx: entities.EntityContext, input: Any = None) -> Any:
    """Entity exposing the durabletask ``EntityContext`` surface (function style)."""
    operation = ctx.operation
    if operation == "set":
        ctx.set_state(input)
        return ctx.get_state()
    if operation == "get":
        return ctx.get_state()
    if operation == "describe":
        return {
            "entity": ctx.entity_id.entity,
            "key": ctx.entity_id.key,
            "operation": ctx.operation,
        }
    if operation == "delete":
        # Setting state to None deletes the entity.
        ctx.set_state(None)
        return "deleted"
    return None


@bp.entity_trigger(context_name="context")
class Relay(DurableEntity):
    """Class-based entity exercising advanced entity patterns.

    - ``signal_counter``: signals another entity (entity-to-entity signalling).
    - ``start_orchestration``: schedules a new orchestration from the entity.
    - ``boom``: raises to exercise entity-operation failure propagation.
    """

    def signal_counter(self, input: Any = None) -> None:
        # input: {"key": <counter key>, "amount": <int>}
        target = entities.EntityInstanceId("counter", input["key"])
        self.signal_entity(target, "add", input["amount"])

    def start_orchestration(self, input: Any = None) -> None:
        # input: {"name": <orchestration name>, "input": <payload>}
        self.schedule_new_orchestration(input["name"], input=input.get("input"))

    def boom(self, input: Any = None) -> None:
        raise ValueError("entity operation failed on purpose")
