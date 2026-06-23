from durabletask.internal.orchestrator_service_pb2 import TaskFailureDetails
from durabletask.entities.entity_instance_id import EntityInstanceId


class EntityOperationFailedException(Exception):
    """Exception raised when an operation on an Entity Function fails."""

    def __init__(self, entity_instance_id: EntityInstanceId, operation_name: str, failure_details: TaskFailureDetails) -> None:
        super().__init__()
        self.entity_instance_id = entity_instance_id
        self.operation_name = operation_name
        self.failure_details = failure_details

    def __str__(self) -> str:
        return f"Operation '{self.operation_name}' on entity '{self.entity_instance_id}' failed with error: {self.failure_details.errorMessage}"
