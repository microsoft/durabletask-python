# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import TYPE_CHECKING, Any, TypeVar, overload

import durabletask.internal.orchestrator_service_pb2 as pb

if TYPE_CHECKING:
    from durabletask.serialization import DataConverter

TState = TypeVar("TState")


class StateShim:
    """In-memory view of an entity's state during a batch.

    The state is held internally as its serialized JSON string at all times.
    The raw payload off the wire is stored verbatim; a live value supplied via
    :meth:`set_state` (or as a non-serialized constructor argument) is
    serialized immediately. Keeping a single, always-serialized representation
    has two consequences worth noting:

    * Deserialization is deferred to :meth:`get_state`, so the caller's
      requested type reaches the data converter together with the original
      payload (a custom converter can deserialize the string directly into the
      target type), and the unmodified wire payload is handed back by
      :meth:`encode_state` without being re-encoded.
    * Serialization errors surface inside the failing operation (at
      :meth:`set_state`) rather than after the batch has run, so a bad write
      rolls back just that operation.

    Because the held value is always the serialized form, :meth:`get_state`
    returns a freshly reconstructed object on every call; it does **not** return
    a reference to a stored live object. Mutating a value read from
    :meth:`get_state` therefore has no effect on the persisted state unless it
    is written back with :meth:`set_state`.
    """

    def __init__(self, start_state: Any, data_converter: "DataConverter",
                 *, is_serialized: bool = False):
        self._data_converter = data_converter
        # The state is normalized to its serialized string form. ``is_serialized``
        # marks ``start_state`` as a raw payload already off the wire (stored
        # verbatim); otherwise a live value is serialized now. ``None`` stays
        # ``None`` (no persisted state).
        serialized_start = self._serialize(start_state, is_serialized)
        self._current_state: str | None = serialized_start
        self._checkpoint_state: str | None = serialized_start
        self._operation_actions: list[pb.OperationAction] = []
        self._actions_checkpoint_state: int = 0

    def _serialize(self, state: Any, is_serialized: bool = False) -> str | None:
        if state is None:
            return None
        if is_serialized:
            return state
        return self._data_converter.serialize(state)

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

        # Deferred deserialization: the converter receives the raw payload
        # together with the requested type.
        if intended_type is None:
            return self._data_converter.deserialize(self._current_state)
        result = self._data_converter.deserialize(self._current_state, intended_type)

        # An explicit ``intended_type`` is a request to receive that type. The
        # default converter is best-effort and would silently return the raw
        # value on a failed coercion; restore the stricter contract here by
        # raising when a non-None state could not be coerced to a concrete type.
        # ``intended_type`` may be a typing generic (e.g. ``list[int]``) at
        # runtime, which is not a ``type`` instance, so the guard is required.
        if (isinstance(intended_type, type)  # pyright: ignore[reportUnnecessaryIsInstance]
                and not isinstance(result, intended_type)):
            raise TypeError(
                f"Could not convert state of type '{type(result).__name__}' to '{intended_type.__name__}'"
            )

        return result

    def set_state(self, state: Any) -> None:
        # Serialize eagerly so the held value is always the wire form and any
        # serialization error surfaces here, inside the failing operation.
        self._current_state = self._serialize(state)

    def encode_state(self) -> str | None:
        """Return the serialized current state for persistence back to the wire.

        The state is already held in serialized form, so this is the stored
        value verbatim: ``None`` when there is no state (which clears the
        persisted entity state), otherwise the JSON string. No re-encoding
        occurs, so a payload that was never modified round-trips unchanged.
        """
        return self._current_state

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
