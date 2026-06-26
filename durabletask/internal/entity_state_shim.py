# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import TYPE_CHECKING, Any, TypeVar, overload

import durabletask.internal.orchestrator_service_pb2 as pb

if TYPE_CHECKING:
    from durabletask.serialization import DataConverter

TState = TypeVar("TState")


class StateShim:
    """In-memory view of an entity's state during a batch.

    The state arriving from the wire is held as its raw serialized JSON string
    and is **not** deserialized in the constructor: deserialization is deferred
    until :meth:`get_state` is called, so the caller's requested type reaches the
    data converter together with the original payload (a custom converter can
    then deserialize the string directly into the target type). Once the state
    has been read into a Python value or replaced via :meth:`set_state`, it is
    held as that live object instead.

    Tracking whether the current value is still the raw serialized string also
    lets :meth:`encode_state` pass an unmodified payload straight back to the
    wire instead of re-serializing it, which would double-encode the JSON.
    """

    def __init__(self, start_state: Any, data_converter: "DataConverter | None" = None,
                 *, is_serialized: bool = False):
        # ``is_serialized`` marks ``start_state`` as a raw serialized payload
        # (the value off the wire) whose deserialization should be deferred. A
        # ``None`` state is never treated as serialized.
        serialized = is_serialized and start_state is not None
        self._current_state: Any = start_state
        self._current_is_serialized: bool = serialized
        self._checkpoint_state: Any = start_state
        self._checkpoint_is_serialized: bool = serialized
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
        if self._current_state is None:
            return default

        if self._current_is_serialized:
            # Deferred deserialization: the converter receives the raw payload
            # together with the requested type.
            if intended_type is None:
                return self._data_converter.deserialize(self._current_state)
            result = self._data_converter.deserialize(self._current_state, intended_type)
        else:
            if intended_type is None:
                return self._current_state
            result = self._data_converter.coerce(self._current_state, intended_type)

        # An explicit ``intended_type`` is a request to receive that type. The
        # default converter is best-effort and would silently return the raw
        # value on a failed coercion; restore the stricter contract here by
        # raising when a non-None state could not be coerced to a concrete type.
        # ``intended_type`` may be a typing generic (e.g. ``list[int]``) at
        # runtime, which is not a ``type`` instance, so the guard is required.
        if (isinstance(intended_type, type)  # pyright: ignore[reportUnnecessaryIsInstance]
                and not isinstance(result, intended_type)):
            raise TypeError(
                f"Could not convert state of type '{type(self._current_state).__name__}' to '{intended_type.__name__}'"
            )

        return result

    def set_state(self, state: Any) -> None:
        # A value set in-process is a live Python object, not a serialized payload.
        self._current_state = state
        self._current_is_serialized = False

    def encode_state(self) -> str | None:
        """Serialize the current state for persistence back to the wire.

        Returns ``None`` only when the state is actually ``None`` (which clears
        the persisted entity state). When the current value is still the raw
        serialized payload (the state was never modified), it is returned
        unchanged to avoid double-encoding; otherwise the live value is
        serialized.
        """
        if self._current_state is None:
            return None
        if self._current_is_serialized:
            return self._current_state
        return self._data_converter.serialize(self._current_state)

    def add_operation_action(self, action: pb.OperationAction) -> None:
        self._operation_actions.append(action)

    def get_operation_actions(self) -> list[pb.OperationAction]:
        return self._operation_actions[:self._actions_checkpoint_state]

    def commit(self) -> None:
        self._checkpoint_state = self._current_state
        self._checkpoint_is_serialized = self._current_is_serialized
        self._actions_checkpoint_state = len(self._operation_actions)

    def rollback(self) -> None:
        self._current_state = self._checkpoint_state
        self._current_is_serialized = self._checkpoint_is_serialized
        self._operation_actions = self._operation_actions[:self._actions_checkpoint_state]

    def reset(self) -> None:
        self._current_state = None
        self._current_is_serialized = False
        self._checkpoint_state = None
        self._checkpoint_is_serialized = False
        self._operation_actions = []
        self._actions_checkpoint_state = 0
