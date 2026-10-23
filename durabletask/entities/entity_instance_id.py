# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.


class EntityInstanceId:
    __slots__ = ("_entity", "_key")

    def __init__(self, entity: str, key: str):
        EntityInstanceId.validate_entity_name(entity)
        EntityInstanceId.validate_key(key)
        object.__setattr__(self, "_entity", entity.lower())
        object.__setattr__(self, "_key", key)

    @property
    def entity(self) -> str:
        return self._entity

    @property
    def key(self) -> str:
        return self._key

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(
            f"'{type(self).__name__}' object does not support attribute assignment"
        )

    def __delattr__(self, name: str) -> None:
        raise AttributeError(
            f"'{type(self).__name__}' object does not support attribute deletion"
        )

    def __copy__(self) -> "EntityInstanceId":
        return self

    def __deepcopy__(self, memo: object) -> "EntityInstanceId":
        return self

    def __reduce__(self) -> tuple[type["EntityInstanceId"], tuple[str, str]]:
        return (EntityInstanceId, (self._entity, self._key))

    def __str__(self) -> str:
        return f"@{self.entity}@{self.key}"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, EntityInstanceId):
            return False
        return self.entity == other.entity and self.key == other.key

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, EntityInstanceId):
            return NotImplemented
        return str(self) < str(other)

    def __hash__(self) -> int:
        return hash((self.entity, self.key))

    def __repr__(self) -> str:
        return f"EntityInstanceId(entity={self.entity!r}, key={self.key!r})"

    @staticmethod
    def parse(entity_id: str) -> "EntityInstanceId":
        """Parse a string representation of an entity ID into an EntityInstanceId object.

        Parameters
        ----------
        entity_id : str
            The string representation of the entity ID, in the format '@entity@key'.

        Returns
        -------
        EntityInstanceId
            The parsed EntityInstanceId object.

        Raises
        ------
        ValueError
            If the input string is not in the correct format.
        """
        if not entity_id.startswith("@"):
            raise ValueError("Entity ID must start with '@'.")
        try:
            _, entity, key = entity_id.split("@", 2)
        except ValueError as ex:
            raise ValueError(f"Invalid entity ID format: {entity_id}") from ex
        return EntityInstanceId(entity=entity, key=key)

    @staticmethod
    def validate_entity_name(name: str) -> None:
        """Validate that the entity name does not contain invalid characters.

        Parameters
        ----------
        name : str
            The entity name to validate.

        Raises
        ------
        ValueError
            If the name is not a valid entity name.
        """
        if not name:
            raise ValueError("Entity name cannot be empty.")
        if "@" in name:
            raise ValueError("Entity name cannot contain '@' symbol.")

    @staticmethod
    def validate_key(key: str) -> None:
        """Validate that the entity key does not contain invalid characters.

        Parameters
        ----------
        key : str
            The entity key to validate.

        Raises
        ------
        ValueError
            If the key is not a valid entity key.
        """
        if not key:
            raise ValueError("Entity key cannot be empty.")
        if "@" in key:
            raise ValueError("Entity key cannot contain '@' symbol.")
