# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from datetime import datetime
from typing import TYPE_CHECKING, Any, TypeVar, overload
import uuid
from google.protobuf import timestamp_pb2
from durabletask.entities.entity_instance_id import EntityInstanceId
from durabletask.internal import helpers
from durabletask.internal.entity_state_shim import StateShim
import durabletask.internal.orchestrator_service_pb2 as pb

if TYPE_CHECKING:
    from durabletask.serialization import DataConverter

TState = TypeVar("TState")


class EntityContext:
    def __init__(self, orchestration_id: str, operation: str, state: StateShim,
                 entity_id: EntityInstanceId, data_converter: "DataConverter | None" = None):
        self._orchestration_id = orchestration_id
        self._operation = operation
        self._state = state
        self._entity_id = entity_id
        if data_converter is None:
            from durabletask.serialization import JsonDataConverter
            data_converter = JsonDataConverter()
        self._data_converter = data_converter

    @property
    def orchestration_id(self) -> str:
        """Get the ID of the orchestration instance that scheduled this entity.

        Returns
        -------
        str
            The ID of the current orchestration instance.
        """
        return self._orchestration_id

    @property
    def operation(self) -> str:
        """Get the operation associated with this entity invocation.

        The operation is a string that identifies the specific action being
        performed on the entity. It can be used to distinguish between
        multiple operations that are part of the same entity invocation.

        Returns
        -------
        str
            The operation associated with this entity invocation.
        """
        return self._operation

    @overload
    def get_state(self, intended_type: type[TState], default: TState) -> TState:
        ...

    @overload
    def get_state(self, intended_type: type[TState]) -> TState | None:
        ...

    @overload
    def get_state(self, intended_type: None = None, default: Any = None) -> Any:
        ...

    def get_state(self, intended_type: type[TState] | None = None, default: TState | None = None) -> TState | Any | None:
        """Get the current state of the entity, optionally converting it to a specified type.

        Parameters
        ----------
        intended_type : type[TState] | None, optional
            The type to which the state should be converted. If None, the state is returned as-is.
        default : TState, optional
            The default value to return if the state is not found or cannot be converted.

        Returns
        -------
        TState | Any
            The current state of the entity, optionally converted to the specified type.
        """
        return self._state.get_state(intended_type, default)

    def set_state(self, new_state: Any) -> None:
        """Set the state of the entity to a new value.

        Parameters
        ----------
        new_state : Any
            The new state to set for the entity.
        """
        self._state.set_state(new_state)

    def signal_entity(self, entity_instance_id: EntityInstanceId, operation: str,
                      input: Any | None = None,
                      signal_time: datetime | None = None) -> None:
        """Signal another entity to perform an operation.

        Parameters
        ----------
        entity_instance_id : EntityInstanceId
            The ID of the entity instance to signal.
        operation : str
            The operation to perform on the entity.
        input : Any, optional
            The input to provide to the entity for the operation.
        signal_time : datetime, optional
            The time at which the signal should be delivered. If None, the signal is
            delivered as soon as possible. Use this to schedule a future operation,
            for example to have an entity wake itself up at a later time.
        """
        encoded_input: str | None = self._data_converter.serialize(input)
        scheduled_time: timestamp_pb2.Timestamp | None = None
        if signal_time is not None:
            scheduled_time = timestamp_pb2.Timestamp()
            scheduled_time.FromDatetime(signal_time)
        self._state.add_operation_action(
            pb.OperationAction(
                sendSignal=pb.SendSignalAction(
                    instanceId=str(entity_instance_id),
                    name=operation,
                    input=helpers.get_string_value(encoded_input),
                    scheduledTime=scheduled_time,
                    requestTime=None,
                    parentTraceContext=None,
                )
            )
        )

    def schedule_new_orchestration(self, orchestration_name: str, input: Any | None = None, instance_id: str | None = None) -> str:
        """Schedule a new orchestration instance.

        Parameters
        ----------
        orchestration_name : str
            The name of the orchestration to schedule.
        input : Any, optional
            The input to provide to the new orchestration.
        instance_id : str, optional
            The instance ID to assign to the new orchestration. If None, a new ID will be generated.

        Returns
        -------
        str
            The instance ID of the scheduled orchestration.
        """
        encoded_input: str | None = self._data_converter.serialize(input)
        if not instance_id:
            instance_id = uuid.uuid4().hex
        self._state.add_operation_action(
            pb.OperationAction(
                startNewOrchestration=pb.StartNewOrchestrationAction(
                    instanceId=instance_id,
                    name=orchestration_name,
                    input=helpers.get_string_value(encoded_input),
                    version=None,
                    scheduledTime=None,
                    requestTime=None,
                    parentTraceContext=None
                )
            )
        )
        return instance_id

    @property
    def entity_id(self) -> EntityInstanceId:
        """Get the ID of the entity instance.

        Returns
        -------
        str
            The ID of the current entity instance.
        """
        return self._entity_id
