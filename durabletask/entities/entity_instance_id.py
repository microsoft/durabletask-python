from typing import Optional


class EntityInstanceId:
    def __init__(self, entity: str, key: str):
        self.entity = entity
        self.key = key

    def __str__(self) -> str:
        return f"@{self.entity}@{self.key}"

    def __eq__(self, other):
        if not isinstance(other, EntityInstanceId):
            return False
        return self.entity == other.entity and self.key == other.key

    def __lt__(self, other):
        if not isinstance(other, EntityInstanceId):
            return self < other
        return str(self) < str(other)

    @staticmethod
    def parse(entity_id: str) -> Optional["EntityInstanceId"]:
        """Parse a string representation of an entity ID into an EntityInstanceId object.

        Parameters
        ----------
        entity_id : str
            The string representation of the entity ID, in the format '@entity@key'.

        Returns
        -------
        Optional[EntityInstanceId]
            The parsed EntityInstanceId object, or None if the input is None.
        """
        try:
            _, entity, key = entity_id.split("@", 2)
            return EntityInstanceId(entity=entity, key=key)
        except ValueError as ex:
            raise ValueError("Invalid entity ID format", ex)
