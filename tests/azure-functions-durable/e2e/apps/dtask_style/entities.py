# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Entity functions for the durabletask-native-style sample app (blueprint).

Uses the modern durabletask two-argument entity style
(``def entity(ctx, input):``) with the durabletask ``EntityContext`` API.
"""

from typing import Any

import azure.durable_functions as df
from durabletask import entities

bp = df.Blueprint()


@bp.entity_trigger(context_name="context")
def counter(ctx: entities.EntityContext, input: Any = None) -> Any:
    if ctx.operation == "add":
        new_state = ctx.get_state(int, 0) + (input or 0)
        ctx.set_state(new_state)
        return new_state
    if ctx.operation == "reset":
        ctx.set_state(0)
        return 0
    return ctx.get_state(int, 0)


@bp.entity_trigger(context_name="context")
def probe(ctx: entities.EntityContext, input: Any = None) -> Any:
    """Entity exposing the durabletask ``EntityContext`` surface."""
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
