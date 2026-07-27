# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Any, Optional

from durabletask.entities.entity_metadata import EntityMetadata


class EntityStateResponse:
    """Entity state response object for ``read_entity_state``.

    Backwards-compatible wrapper around the durabletask
    :class:`~durabletask.entities.entity_metadata.EntityMetadata`. New code
    should use ``get_entity`` and the returned ``EntityMetadata`` directly.
    """

    def __init__(self, entity_exists: bool, entity_state: Any = None):
        self._entity_exists = entity_exists
        self._entity_state = entity_state

    @classmethod
    def from_entity_metadata(
            cls, metadata: Optional[EntityMetadata]) -> "EntityStateResponse":
        """Build a response from a durabletask ``EntityMetadata`` (or ``None``)."""
        if metadata is None:
            return cls(False)
        state = metadata.get_typed_state() if metadata.includes_state else None
        return cls(True, state)

    @property
    def entity_exists(self) -> bool:
        """Get the bool representing whether the entity exists."""
        return self._entity_exists

    @property
    def entity_state(self) -> Any:
        """Get the state of the entity.

        When ``entity_exists`` is ``False``, this value is ``None``.
        """
        return self._entity_state
