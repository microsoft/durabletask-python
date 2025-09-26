
from typing import Any, Optional, Type, TypeVar, overload
import uuid
from durabletask.entities import EntityInstanceId
from durabletask.internal import helpers, shared
from durabletask.internal.entity_state_shim import StateShim
import durabletask.internal.orchestrator_service_pb2 as pb

TState = TypeVar("TState")


class EntityContext:
    def __init__(self, orchestration_id: str, operation: str, state: StateShim, entity_id: EntityInstanceId):
        self._orchestration_id = orchestration_id
        self._operation = operation
        self._state = state
        self._entity_id = entity_id

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
    def get_state(self, intended_type: Type[TState], default: TState) -> TState:
        ...

    @overload
    def get_state(self, intended_type: Type[TState]) -> Optional[TState]:
        ...

    @overload
    def get_state(self, intended_type: None = None, default: Any = None) -> Any:
        ...

    def get_state(self, intended_type: Optional[Type[TState]] = None, default: Optional[TState] = None) -> Optional[TState] | Any:
        return self._state.get_state(intended_type, default)

    def set_state(self, new_state: Any):
        self._state.set_state(new_state)

    def signal_entity(self, entity_instance_id: EntityInstanceId, operation: str, input: Optional[Any] = None) -> None:
        encoded_input = shared.to_json(input) if input is not None else None
        self._state.add_operation_action(
            pb.OperationAction(
                sendSignal=pb.SendSignalAction(
                    instanceId=str(entity_instance_id),
                    name=operation,
                    input=helpers.get_string_value(encoded_input),
                    scheduledTime=None,
                    requestTime=None,
                    parentTraceContext=None,
                )
            )
        )

    def schedule_new_orchestration(self, orchestration_name: str, input: Optional[Any] = None, instance_id: Optional[str] = None) -> str:
        encoded_input = shared.to_json(input) if input is not None else None
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
