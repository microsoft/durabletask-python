# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

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

        coerced = self._data_converter.coerce(self._current_state, intended_type)

        # An explicit ``intended_type`` is a request to receive that type. The
        # default converter is best-effort and would silently return the raw
        # value on a failed coercion; restore the stricter contract here by
        # raising when a non-None state could not be coerced to a concrete type.
        # ``intended_type`` may be a typing generic (e.g. ``list[int]``) at
        # runtime, which is not a ``type`` instance, so the guard is required.
        if (self._current_state is not None
                and isinstance(intended_type, type)  # pyright: ignore[reportUnnecessaryIsInstance]
                and not isinstance(coerced, intended_type)):
            raise TypeError(
                f"Could not convert state of type '{type(self._current_state).__name__}' to '{intended_type.__name__}'"
            )

        return coerced

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
