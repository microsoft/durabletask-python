# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""In-process execution support for unit-testing durable entities."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, TypeAlias, cast

from durabletask.entities import DurableEntity, EntityContext, EntityInstanceId
from durabletask.internal import type_discovery
from durabletask.internal.entity_state_shim import StateShim
import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask.serialization import DataConverter

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
    state_shim = _TestingStateShim(state, converter)
    context = _TestingEntityContext(
        str(entity_id), operation, state_shim, entity_id, converter, state)

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
        result = _restore_snapshot(encoded_result, result, converter)
        state_shim.commit()
    except Exception:
        state_shim.rollback()
        raise

    return EntityTestResult(
        result=result,
        state=context.current_state,
        actions=tuple(context.actions),
    )


class _TestingStateShim(StateShim):
    """State shim that exposes the exact action payload just serialized."""

    def __init__(
            self,
            start_state: Any,
            data_converter: DataConverter,
    ):
        super().__init__(start_state, data_converter)
        self.latest_action: pb.OperationAction | None = None

    def add_operation_action(self, action: pb.OperationAction) -> None:
        super().add_operation_action(action)
        self.latest_action = action


class _TestingEntityContext(EntityContext):
    """Entity context that captures runtime-equivalent serialized snapshots."""

    def __init__(
            self,
            orchestration_id: str,
            operation: str,
            state: StateShim,
            entity_id: EntityInstanceId,
            data_converter: DataConverter,
            initial_state: Any,
    ):
        super().__init__(
            orchestration_id, operation, state, entity_id, data_converter)
        if not isinstance(state, _TestingStateShim):
            raise TypeError("state must be a _TestingStateShim")
        self._testing_state = state
        self._testing_converter = data_converter
        self.current_state = _restore_snapshot(
            state.encode_state(), initial_state, data_converter)
        self.actions: list[EntityAction] = []

    def set_state(self, new_state: Any) -> None:
        super().set_state(new_state)
        self.current_state = _restore_snapshot(
            self._testing_state.encode_state(),
            new_state,
            self._testing_converter,
        )

    def signal_entity(
            self,
            entity_instance_id: EntityInstanceId,
            operation: str,
            input: Any | None = None,
            signal_time: datetime | None = None,
    ) -> None:
        super().signal_entity(
            entity_instance_id, operation, input, signal_time)
        action = self._require_latest_action()
        signal = action.sendSignal
        encoded_input = (
            signal.input.value if signal.HasField("input") else None)
        scheduled_time = (
            signal.scheduledTime.ToDatetime(tzinfo=timezone.utc)
            if signal.HasField("scheduledTime")
            else None
        )
        self.actions.append(EntitySignalAction(
            entity_id=EntityInstanceId.parse(signal.instanceId),
            operation=signal.name,
            input=_restore_snapshot(
                encoded_input, input, self._testing_converter),
            scheduled_time=scheduled_time,
        ))

    def schedule_new_orchestration(
            self,
            orchestration_name: str,
            input: Any | None = None,
            instance_id: str | None = None,
    ) -> str:
        resolved_instance_id = super().schedule_new_orchestration(
            orchestration_name, input, instance_id)
        action = self._require_latest_action()
        start = action.startNewOrchestration
        encoded_input = start.input.value if start.HasField("input") else None
        self.actions.append(OrchestrationStartAction(
            name=start.name,
            instance_id=start.instanceId,
            input=_restore_snapshot(
                encoded_input, input, self._testing_converter),
        ))
        return resolved_instance_id

    def _require_latest_action(self) -> pb.OperationAction:
        action = self._testing_state.latest_action
        if action is None:
            raise RuntimeError("Entity action was not recorded")
        return action


def _restore_snapshot(
        encoded_value: str | None,
        value: Any,
        converter: DataConverter,
) -> Any:
    if value is None:
        return None
    value_type = cast(type[Any], type(value))
    return converter.deserialize(encoded_value, value_type)


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
