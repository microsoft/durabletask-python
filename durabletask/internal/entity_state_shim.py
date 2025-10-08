from typing import Any, TypeVar, Union
from typing import Optional, Type, overload

import durabletask.internal.orchestrator_service_pb2 as pb

TState = TypeVar("TState")


class StateShim:
    def __init__(self, start_state):
        self._current_state: Any = start_state
        self._checkpoint_state: Any = start_state
        self._operation_actions: list[pb.OperationAction] = []
        self._actions_checkpoint_state: int = 0

    @overload
    def get_state(self, intended_type: Type[TState], default: TState) -> TState:
        ...

    @overload
    def get_state(self, intended_type: Type[TState]) -> Optional[TState]:
        ...

    @overload
    def get_state(self, intended_type: None = None, default: Any = None) -> Any:
        ...

    def get_state(self, intended_type: Optional[Type[TState]] = None, default: Optional[TState] = None) -> Union[None, TState, Any]:
        if self._current_state is None and default is not None:
            return default

        if intended_type is None:
            return self._current_state

        if isinstance(self._current_state, intended_type):
            return self._current_state

        try:
            return intended_type(self._current_state)  # type: ignore[call-arg]
        except Exception as ex:
            raise TypeError(
                f"Could not convert state of type '{type(self._current_state).__name__}' to '{intended_type.__name__}'"
            ) from ex

    def set_state(self, state):
        self._current_state = state

    def add_operation_action(self, action: pb.OperationAction):
        self._operation_actions.append(action)

    def get_operation_actions(self) -> list[pb.OperationAction]:
        return self._operation_actions[:self._actions_checkpoint_state]

    def commit(self):
        self._checkpoint_state = self._current_state
        self._actions_checkpoint_state = len(self._operation_actions)

    def rollback(self):
        self._current_state = self._checkpoint_state
        self._operation_actions = self._operation_actions[:self._actions_checkpoint_state]

    def reset(self):
        self._current_state = None
        self._checkpoint_state = None
        self._operation_actions = []
        self._actions_checkpoint_state = 0
