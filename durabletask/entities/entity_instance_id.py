class EntityInstanceId:
    def __init__(self, entity: str, key: str):
        if not entity or not key:
            raise ValueError("Entity name and key cannot be empty.")
        if "@" in key:
            raise ValueError("Entity key cannot contain '@' symbol.")
        self.entity = entity.lower()
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
