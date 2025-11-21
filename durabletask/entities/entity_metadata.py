from datetime import datetime
from typing import Any, Optional
from durabletask.entities.entity_instance_id import EntityInstanceId

import durabletask.internal.orchestrator_service_pb2 as pb


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
        """Initializes a new instance of the EntityState class.

        Args:
            value: The initial state value of the entity.
        """
        self.id = id
        self.last_modified = last_modified
        self.backlog_queue_size = backlog_queue_size
        self.locked_by = locked_by
        self.includes_state = includes_state
        self.state = state

    @staticmethod
    def from_entity_response(entity_response: pb.GetEntityResponse, includes_state: bool):
        entity_id = EntityInstanceId.parse(entity_response.entity.instanceId)
        if not entity_id:
            raise ValueError("Invalid entity instance ID in entity response.")
        entity_state = None
        if includes_state:
            entity_state = str(entity_response.entity.serializedState)
        return EntityMetadata(
            id=entity_id,
            last_modified=entity_response.entity.lastModifiedTime.ToDatetime(),
            backlog_queue_size=entity_response.entity.backlogQueueSize,
            locked_by=str(entity_response.entity.lockedBy),
            includes_state=includes_state,
            state=entity_state
        )
