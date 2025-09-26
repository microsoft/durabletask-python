from typing import Any, Optional, Type, TypeVar, overload

from durabletask.entities.entity_instance_id import EntityInstanceId
from durabletask.task import EntityContext

TState = TypeVar("TState")


class DurableEntity:
    def _initialize_entity_context(self, context: EntityContext):
        self.entity_context = context

    @overload
    def get_state(self, intended_type: Type[TState], default: TState) -> TState: ...

    @overload
    def get_state(self, intended_type: Type[TState]) -> Optional[TState]: ...

    @overload
    def get_state(self, intended_type: None = None, default: Any = None) -> Any: ...

    def get_state(self, intended_type: Optional[Type[TState]] = None, default: Optional[TState] = None) -> Optional[TState] | Any:
        return self.entity_context.get_state(intended_type, default)

    def set_state(self, state: Any):
        self.entity_context.set_state(state)

    def signal_entity(self, entity_instance_id: EntityInstanceId, operation: str, input: Optional[Any] = None) -> None:
        self.entity_context.signal_entity(entity_instance_id, operation, input)

    def schedule_new_orchestration(self, orchestration_name: str, input: Optional[Any] = None, instance_id: Optional[str] = None) -> str:
        return self.entity_context.schedule_new_orchestration(orchestration_name, input, instance_id=instance_id)
