from typing import TYPE_CHECKING, Any, TypeVar, overload

import durabletask.internal.orchestrator_service_pb2 as pb

if TYPE_CHECKING:
    from durabletask.serialization import DataConverter

TState = TypeVar("TState")


class StateShim:
    def __init__(self, start_state: Any, data_converter: "DataConverter | None" = None):
        self._current_state: Any = start_state
        self._checkpoint_state: Any = start_state
        self._operation_actions: list[pb.OperationAction] = []
        self._actions_checkpoint_state: int = 0
        if data_converter is None:
            from durabletask.serialization import JsonDataConverter
            data_converter = JsonDataConverter()
        self._data_converter = data_converter

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
        if self._current_state is None and default is not None:
            return default

        if intended_type is None:
            return self._current_state

        return self._data_converter.coerce(self._current_state, intended_type)

    def set_state(self, state: Any) -> None:
        self._current_state = state

    def add_operation_action(self, action: pb.OperationAction) -> None:
        self._operation_actions.append(action)

    def get_operation_actions(self) -> list[pb.OperationAction]:
        return self._operation_actions[:self._actions_checkpoint_state]

    def commit(self) -> None:
        self._checkpoint_state = self._current_state
        self._actions_checkpoint_state = len(self._operation_actions)

    def rollback(self) -> None:
        self._current_state = self._checkpoint_state
        self._operation_actions = self._operation_actions[:self._actions_checkpoint_state]

    def reset(self) -> None:
        self._current_state = None
        self._checkpoint_state = None
        self._operation_actions = []
        self._actions_checkpoint_state = 0
