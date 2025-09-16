from typing import Any, Optional, Type, TypeVar, overload

TState = TypeVar("TState")


class DurableEntity:
    def _initialize_entity_context(self, context):
        self.entity_context = context

    @overload
    def get_state(self, intended_type: Type[TState]) -> Optional[TState]: ...
    
    @overload
    def get_state(self, intended_type: None = None) -> Any: ...
    
    def get_state(self, intended_type: Optional[Type[TState]] = None) -> Optional[TState] | Any:
        return self.entity_context.get_state(intended_type)

    def set_state(self, state: Any):
        self.entity_context.set_state(state)