from ctypes import Union
from typing import Any, TypeVar, runtime_checkable
from typing import Optional, Type, overload
from typing_extensions import Protocol


TState = TypeVar("TState")


class StateShim:
    def __init__(self, start_state):
        self._current_state: Any = start_state
        self._checkpoint_state: Any = start_state

    @overload
    def get_state(self, intended_type: Type[TState]) -> Optional[TState]: ...
    
    @overload
    def get_state(self, intended_type: None = None) -> Any: ...

    def get_state(self, intended_type: Optional[Type[TState]] = None) -> Optional[TState] | Any:
        if intended_type is None:
            return self._current_state

        if isinstance(self._current_state, intended_type) or self._current_state is None:
            return self._current_state

        try:
            return intended_type(self._current_state) # type: ignore[call-arg]
        except Exception as ex:
            raise TypeError(
                f"Could not convert state of type '{type(self._current_state).__name__}' to '{intended_type.__name__}'"
            ) from ex

    def set_state(self, state):
        self._current_state = state

    def commit(self):
        self._checkpoint_state = self._current_state

    def rollback(self):
        self._current_state = self._checkpoint_state

    def reset(self):
        self._current_state = None
