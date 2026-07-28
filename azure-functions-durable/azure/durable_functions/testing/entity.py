# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""In-process execution support for unit-testing durable entities."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, TypeAlias

from durabletask.entities import DurableEntity, EntityContext, EntityInstanceId
from durabletask.internal import type_discovery
from durabletask.internal.entity_state_shim import StateShim
import durabletask.internal.orchestrator_service_pb2 as pb

from ..internal.compat.entity_context import wrap_entity
from ..internal.serialization import DEFAULT_FUNCTIONS_DATA_CONVERTER


@dataclass(frozen=True)
class EntitySignalAction:
    """A signal scheduled by an entity operation."""

    entity_id: EntityInstanceId
    operation: str
    input: Any = None
    scheduled_time: datetime | None = None


@dataclass(frozen=True)
class OrchestrationStartAction:
    """An orchestration start scheduled by an entity operation."""

    name: str
    instance_id: str
    input: Any = None


EntityAction: TypeAlias = EntitySignalAction | OrchestrationStartAction


@dataclass(frozen=True)
class EntityTestResult:
    """The observable outcome of executing one entity operation."""

    result: Any
    state: Any
    actions: tuple[EntityAction, ...]


def execute_entity(
        entity: Callable[..., Any],
        operation: str,
        input: Any = None,
        state: Any = None,
        *,
        entity_name: str | None = None,
        entity_key: str = "test",
) -> EntityTestResult:
    """Execute one entity operation without a Functions host or backend.

    The ``entity`` argument may be a v1-style one-argument function, a
    durabletask-native two-argument function, or a
    :class:`~durabletask.entities.DurableEntity` subclass. Decorated entities can
    be obtained from the function builder's exposed ``entity_function`` handle.

    Parameters
    ----------
    entity : Callable[..., Any]
        The entity function or ``DurableEntity`` subclass to execute.
    operation : str
        The entity operation name.
    input : Any, optional
        The operation input.
    state : Any, optional
        The state visible at the start of the operation.
    entity_name : str, optional
        The entity name exposed through its context. Defaults to the configured
        durable entity name or the callable's ``__name__``.
    entity_key : str, optional
        The entity key exposed through its context. Defaults to ``"test"``.

    Returns
    -------
    EntityTestResult
        The operation result, resulting state, and scheduled actions.

    Raises
    ------
    AttributeError
        If a class-based entity does not define ``operation``.
    TypeError
        If the entity or operation is not callable.
    Exception
        Any exception raised by entity code or payload serialization.
    """
    if not callable(entity):
        raise TypeError("entity must be a callable or DurableEntity subclass")

    entity_callable = wrap_entity(entity)
    resolved_name = (
        entity_name
        or getattr(entity_callable, "__durable_entity_name__", None)
        or getattr(entity_callable, "__name__", None)
    )
    if not resolved_name:
        raise ValueError(
            "entity_name is required when the entity has no __name__")

    converter = DEFAULT_FUNCTIONS_DATA_CONVERTER
    entity_id = EntityInstanceId(resolved_name, entity_key)
    state_shim = StateShim(state, converter)
    context = EntityContext(
        str(entity_id), operation, state_shim, entity_id, converter)

    encoded_input = converter.serialize(input)
    input_type = (
        type_discovery.entity_input_type(
            entity_callable, operation, converter)
        if encoded_input is not None
        else None
    )
    operation_input = converter.deserialize(encoded_input, input_type)

    try:
        result = _invoke_entity(
            entity_callable, operation, context, operation_input)
        encoded_result = converter.serialize(result)
        result = converter.deserialize(encoded_result)
        state_shim.commit()
    except Exception:
        state_shim.rollback()
        raise

    actions = tuple(
        _decode_action(action) for action in state_shim.get_operation_actions())
    return EntityTestResult(
        result=result,
        state=state_shim.get_state(),
        actions=actions,
    )


def _invoke_entity(
        entity: Callable[..., Any],
        operation: str,
        context: EntityContext,
        operation_input: Any,
) -> Any:
    if isinstance(entity, type) and issubclass(entity, DurableEntity):
        instance = entity()
        if not hasattr(instance, operation):
            raise AttributeError(
                f"Entity '{context.entity_id}' does not have operation "
                f"'{operation}'")

        method = getattr(instance, operation)
        if not callable(method):
            raise TypeError(f"Entity operation '{operation}' is not callable")

        instance._initialize_entity_context(  # pyright: ignore[reportPrivateUsage]
            context)
        signature = inspect.signature(method)
        has_required_parameter = any(
            parameter.default is inspect.Parameter.empty
            for parameter in signature.parameters.values()
            if parameter.kind not in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            )
        )
        if has_required_parameter or operation_input is not None:
            return method(operation_input)
        return method()

    return entity(context, operation_input)


def _decode_action(action: pb.OperationAction) -> EntityAction:
    converter = DEFAULT_FUNCTIONS_DATA_CONVERTER
    if action.HasField("sendSignal"):
        signal = action.sendSignal
        scheduled_time = (
            signal.scheduledTime.ToDatetime(tzinfo=timezone.utc)
            if signal.HasField("scheduledTime")
            else None
        )
        encoded_input = (
            signal.input.value if signal.HasField("input") else None)
        return EntitySignalAction(
            entity_id=EntityInstanceId.parse(signal.instanceId),
            operation=signal.name,
            input=converter.deserialize(encoded_input),
            scheduled_time=scheduled_time,
        )

    if action.HasField("startNewOrchestration"):
        start = action.startNewOrchestration
        encoded_input = start.input.value if start.HasField("input") else None
        return OrchestrationStartAction(
            name=start.name,
            instance_id=start.instanceId,
            input=converter.deserialize(encoded_input),
        )

    raise ValueError("Unsupported entity action")
