from durabletask.entities.entity_instance_id import EntityInstanceId
from durabletask.internal.orchestration_entity_context import OrchestrationEntityContext


class EntityLock:
    def __init__(self, entity_context: OrchestrationEntityContext, entities: list[EntityInstanceId]):
        self.entity_context = entity_context
        self.entities = entities

    def __enter__(self):
        print(f"Locking entities: {self.entities}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f"Unlocking entities: {self.entities}")
