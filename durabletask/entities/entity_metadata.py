from datetime import datetime
from typing import Any, Optional, Type, TypeVar, Union, overload
from durabletask.entities.entity_instance_id import EntityInstanceId

import durabletask.internal.orchestrator_service_pb2 as pb

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
        state (Optional[Any]): The current state of the entity, if included.
    """

    def __init__(self,
                 id: EntityInstanceId,
                 last_modified: datetime,
                 backlog_queue_size: int,
                 locked_by: str,
                 includes_state: bool,
                 state: Optional[Any]):
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

    @staticmethod
    def from_entity_response(entity_response: pb.GetEntityResponse, includes_state: bool):
        entity_id = EntityInstanceId.parse(entity_response.entity.instanceId)
        if not entity_id:
            raise ValueError("Invalid entity instance ID in entity response.")
        entity_state = None
        if includes_state:
            entity_state = entity_response.entity.serializedState.value
        return EntityMetadata(
            id=entity_id,
            last_modified=entity_response.entity.lastModifiedTime.ToDatetime(),
            backlog_queue_size=entity_response.entity.backlogQueueSize,
            locked_by=entity_response.entity.lockedBy.value,
            includes_state=includes_state,
            state=entity_state
        )

    @overload
    def get_state(self, intended_type: Type[TState]) -> Optional[TState]:
        ...

    @overload
    def get_state(self, intended_type: None = None) -> Any:
        ...

    def get_state(self, intended_type: Optional[Type[TState]] = None) -> Union[None, TState, Any]:
        """Get the current state of the entity, optionally converting it to a specified type."""
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

    def get_locked_by(self) -> Optional[EntityInstanceId]:
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
