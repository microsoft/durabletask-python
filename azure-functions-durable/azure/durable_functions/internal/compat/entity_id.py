# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from warnings import deprecated

from durabletask.entities import EntityInstanceId


@deprecated(
    "EntityId is deprecated; use durabletask.entities.EntityInstanceId instead.")
class EntityId(EntityInstanceId):
    """Backwards-compatible shim for the v1 ``EntityId`` class.

    Identifies an entity by its name and key. New code should use
    :class:`durabletask.entities.EntityInstanceId`.
    """

    def __init__(self, name: str, key: str):
        """Instantiate an EntityId object.

        Args:
            name (str): The entity name.
            key (str): The entity key.
        """
        super().__init__(entity=name, key=key)

    @property
    def name(self) -> str:
        """Get the entity name (v1 alias for ``entity``)."""
        return self.entity

    @staticmethod
    def get_scheduler_id(entity_id: EntityInstanceId) -> str:
        """Produce a scheduler ID string (``@name@key``) from an entity ID."""
        return str(entity_id)

    @staticmethod
    def get_entity_id(scheduler_id: str) -> "EntityId":  # pyright: ignore[reportDeprecated]
        """Return an entity ID from a scheduler ID string (``@name@key``).

        Returns a v1-compatible :class:`EntityId` (exposing ``.name``) rather
        than a bare :class:`~durabletask.entities.EntityInstanceId`, so v1 code
        such as ``EntityId.get_entity_id("@Counter@one").name`` keeps working.

        The return annotation and the construction below reference the
        deprecated public :class:`EntityId` on purpose (that is this factory's
        contract), so the internal ``reportDeprecated`` diagnostics are
        suppressed while the public deprecation stays intact for callers.
        """
        parsed = EntityInstanceId.parse(scheduler_id)
        return EntityId(name=parsed.entity, key=parsed.key)  # pyright: ignore[reportDeprecated]

    @staticmethod
    def get_entity_id_url_path(entity_id: EntityInstanceId) -> str:
        """Return the entity URL path (``entities/{name}/{key}``) for an entity ID."""
        return f"entities/{entity_id.entity}/{entity_id.key}"
