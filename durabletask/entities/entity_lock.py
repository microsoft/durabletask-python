import durabletask.internal.helpers as ph

from durabletask.entities.entity_instance_id import EntityInstanceId
import durabletask.internal.orchestrator_service_pb2 as pb


class EntityLock:
    def __init__(self, context):
        self._context = context

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb): # TODO: Handle exceptions?
        print(f"Unlocking entities: {self._context._entity_context.critical_section_locks}")
        for entity_unlock_message in self._context._entity_context.emit_lock_release_messages():
            task_id = self._context.next_sequence_number()
            action = pb.OrchestratorAction(task_id, sendEntityMessage=entity_unlock_message)
            self._context._pending_actions[task_id] = action
