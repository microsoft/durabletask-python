from typing import Any, Optional, Type, TypeVar, overload

from durabletask.entities.entity_context import EntityContext
from durabletask.entities.entity_instance_id import EntityInstanceId

TState = TypeVar("TState")


class DurableEntity:
    def _initialize_entity_context(self, context: EntityContext):
        self.entity_context = context

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
        """Get the current state of the entity, optionally converting it to a specified type.

        Parameters
        ----------
        intended_type : Type[TState] | None, optional
            The type to which the state should be converted. If None, the state is returned as-is.
        default : TState, optional
            The default value to return if the state is not found or cannot be converted.

        Returns
        -------
        TState | Any
            The current state of the entity, optionally converted to the specified type.
        """
        return self.entity_context.get_state(intended_type, default)

    def set_state(self, state: Any):
        """Set the state of the entity to a new value.

        Parameters
        ----------
        new_state : Any
            The new state to set for the entity.
        """
        self.entity_context.set_state(state)

    def signal_entity(self, entity_instance_id: EntityInstanceId, operation: str, input: Optional[Any] = None) -> None:
        """Signal another entity to perform an operation.

        Parameters
        ----------
        entity_instance_id : EntityInstanceId
            The ID of the entity instance to signal.
        operation : str
            The operation to perform on the entity.
        input : Any, optional
            The input to provide to the entity for the operation.
        """
        self.entity_context.signal_entity(entity_instance_id, operation, input)

    def schedule_new_orchestration(self, orchestration_name: str, input: Optional[Any] = None, instance_id: Optional[str] = None) -> str:
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
        return self.entity_context.schedule_new_orchestration(orchestration_name, input, instance_id=instance_id)

    def delete(self, input: Any = None) -> None:
        """Delete the entity instance.

        Parameters
        ----------
        input : Any, optional
            Unused: The input for the entity "delete" operation.
        """
        self.set_state(None)
