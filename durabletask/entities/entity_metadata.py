# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, TypeVar, overload
from durabletask.entities.entity_instance_id import EntityInstanceId

import durabletask.internal.orchestrator_service_pb2 as pb

if TYPE_CHECKING:
    from durabletask.serialization import DataConverter

TState = TypeVar("TState")


class EntityMetadata:
    """Class representing the metadata of a durable entity.

    This class encapsulates the metadata information of a durable entity, allowing for
    easy access and manipulation of the entity's metadata within the Durable Task
    Framework.

    Attributes:
        id (EntityInstanceId): The unique identifier of the entity instance.
        last_modified (datetime): The timestamp of the last modification to the entity.
        backlog_queue_size (int): The size of the backlog queue for the entity.
        locked_by (str): The identifier of the worker that currently holds the lock on the entity.
        includes_state (bool): Indicates whether the metadata includes the state of the entity.
        state (Any | None): The current state of the entity, if included.
    """

    def __init__(self,
                 id: EntityInstanceId,
                 last_modified: datetime,
                 backlog_queue_size: int,
                 locked_by: str,
                 includes_state: bool,
                 state: Any | None,
                 data_converter: "DataConverter | None" = None):
        """Initializes a new instance of the EntityMetadata class.

        Args:
            value: The initial state value of the entity.
        """
        self.id = id
        self.last_modified = last_modified
        self.backlog_queue_size = backlog_queue_size
        self._locked_by = locked_by
        self.includes_state = includes_state
        self._state = state
        if data_converter is None:
            from durabletask.serialization import JsonDataConverter
            data_converter = JsonDataConverter()
        self._data_converter = data_converter

    @staticmethod
    def from_entity_response(entity_response: pb.GetEntityResponse, includes_state: bool,
                             data_converter: "DataConverter | None" = None):
        return EntityMetadata.from_entity_metadata(
            entity_response.entity, includes_state, data_converter)

    @staticmethod
    def from_entity_metadata(entity: pb.EntityMetadata, includes_state: bool,
                             data_converter: "DataConverter | None" = None):
        try:
            entity_id = EntityInstanceId.parse(entity.instanceId)
        except ValueError:
            raise ValueError("Invalid entity instance ID in entity response.")
        entity_state = None
        if includes_state:
            entity_state = entity.serializedState.value
        return EntityMetadata(
            id=entity_id,
            last_modified=entity.lastModifiedTime.ToDatetime(timezone.utc),
            backlog_queue_size=entity.backlogQueueSize,
            locked_by=entity.lockedBy.value,
            includes_state=includes_state,
            state=entity_state,
            data_converter=data_converter,
        )

    @overload
    def get_state(self, intended_type: type[TState]) -> TState | None:
        ...

    @overload
    def get_state(self, intended_type: None = None) -> Any:
        ...

    def get_state(self, intended_type: type[TState] | None = None) -> TState | Any | None:
        """Get the entity's raw persisted state, optionally constructor-coerced.

        The state is held as the raw serialized JSON payload (a ``str``). With no
        argument the raw payload is returned unchanged; passing ``intended_type``
        applies the legacy constructor-based coercion (``intended_type(raw)``)
        and raises ``TypeError`` if that fails.

        This preserves the pre-existing contract. To deserialize the payload and
        reconstruct dataclasses or ``from_json()``-capable types, use
        :meth:`get_typed_state` instead.
        """
        if intended_type is None or self._state is None:
            return self._state

        if isinstance(self._state, intended_type):
            return self._state

        try:
            return intended_type(self._state)  # type: ignore[call-arg]
        except Exception as ex:
            raise TypeError(
                f"Could not convert state of type '{type(self._state).__name__}' to '{intended_type.__name__}'"
            ) from ex

    @overload
    def get_typed_state(self, intended_type: type[TState]) -> TState | None:
        ...

    @overload
    def get_typed_state(self, intended_type: None = None) -> Any:
        ...

    def get_typed_state(self, intended_type: type[TState] | None = None) -> TState | Any | None:
        """Deserialize the entity's persisted state, optionally reconstructing a type.

        The state is stored as its raw serialized JSON payload and deserialized
        here. When ``intended_type`` is provided the payload is reconstructed as
        that type (dataclasses, ``from_json()``-capable types, etc.); otherwise
        the plain deserialized JSON value is returned.
        """
        if self._state is None:
            return None

        return self._data_converter.deserialize(self._state, intended_type)

    def get_locked_by(self) -> EntityInstanceId | None:
        """Get the identifier of the worker that currently holds the lock on the entity.

        Returns
        -------
        str
            The identifier of the worker that currently holds the lock on the entity.
        """
        if not self._locked_by:
            return None

        # Will throw ValueError if the format is invalid
        return EntityInstanceId.parse(self._locked_by)
