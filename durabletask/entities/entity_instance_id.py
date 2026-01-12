from typing import Any, Callable, TypeVar, Union, overload, TYPE_CHECKING

if TYPE_CHECKING:
    from durabletask import task
    from durabletask.entities.durable_entity import DurableEntity
    from durabletask.entities.entity_context import EntityContext


TInput = TypeVar('TInput')
TOutput = TypeVar('TOutput')


class EntityInstanceId[TInput, TOutput]:
    @overload
    def __new__(
        cls,
        entity: Callable[[EntityContext, TInput], TOutput],
        key: str
    ) -> "EntityInstanceId[TInput, TOutput]": ...

    @overload
    def __new__(
        cls,
        entity: type[DurableEntity],
        key: str
    ) -> "EntityInstanceId[Any, Any]": ...

    @overload
    def __new__(
        cls,
        entity: str,
        key: str
    ) -> "EntityInstanceId[Any, Any]": ...

    def __new__(
        cls,
        entity: Union[task.Entity[TInput, TOutput], str],
        key: str
    ) -> "EntityInstanceId[Any, Any]":
        return super().__new__(cls)

    def __init__(
        self,
        entity: Union[task.Entity[TInput, TOutput], str],
        key: str
    ):
        if not isinstance(entity, str):
            from durabletask import task
            entity = task.get_entity_name(entity)
        self.entity: str = entity
        self.key: str = key

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
    def parse(entity_id: str) -> "EntityInstanceId[Any, Any]":
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
        try:
            _, entity, key = entity_id.split("@", 2)
            return EntityInstanceId(entity=entity, key=key)
        except ValueError as ex:
            raise ValueError(f"Invalid entity ID format: {entity_id}", ex)
