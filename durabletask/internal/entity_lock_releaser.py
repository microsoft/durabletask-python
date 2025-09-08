from durabletask.entities.entity_instance_id import EntityInstanceId


class EntityLockReleaser:
    def __init__(self, entities: list[EntityInstanceId]):
        self.entities = entities
