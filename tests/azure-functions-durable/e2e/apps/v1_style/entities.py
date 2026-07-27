# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Entity functions for the V1-style sample app (blueprint).

Uses the classic v1 single-argument entity style (``def entity(context):``) and
exercises the full ``DurableEntityContext`` surface: ``operation_name``,
``get_input``/``get_state``/``set_state``/``set_result``, ``entity_name`` /
``entity_key`` / ``is_newly_constructed`` (via the ``describe`` operation), and
``destruct_on_exit`` (via the ``delete`` operation).
"""

import azure.durable_functions as df

bp = df.Blueprint()


@bp.entity_trigger(context_name="context")
def counter(context: df.DurableEntityContext) -> None:
    current = context.get_state(initializer=lambda: 0)
    operation = context.operation_name
    if operation == "add":
        current += context.get_input()
        context.set_state(current)
    elif operation == "reset":
        current = 0
        context.set_state(current)
    context.set_result(current)


@bp.entity_trigger(context_name="context")
def probe(context: df.DurableEntityContext) -> None:
    """Entity exposing the full v1 ``DurableEntityContext`` surface."""
    operation = context.operation_name
    if operation == "set":
        context.set_state(context.get_input())
        context.set_result(context.get_state())
    elif operation == "get":
        context.set_result(context.get_state(initializer=lambda: None))
    elif operation == "describe":
        context.set_result({
            "entity_name": context.entity_name,
            "entity_key": context.entity_key,
            "operation_name": context.operation_name,
            "is_newly_constructed": context.is_newly_constructed,
        })
    elif operation == "delete":
        # destruct_on_exit clears the entity state, deleting the entity.
        context.destruct_on_exit()
        context.set_result("deleted")
