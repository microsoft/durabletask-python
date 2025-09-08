from datetime import datetime
from typing import Generator, List, Optional, Tuple

from durabletask.entities.entity_instance_id import EntityInstanceId


class OrchestrationEntityContext:
    def __init__(self, instance_id: str):
        self.instance_id = instance_id

        self.lock_acquisition_pending = False

        self.critical_section_id = None
        self.critical_section_locks = []
        self.available_locks = []

    @property
    def is_inside_critical_section(self) -> bool:
        return self.critical_section_id is not None

    def get_available_entities(self) -> Generator[str, None, None]:
        if self.is_inside_critical_section:
            for available_lock in self.available_locks:
                yield available_lock

    def validate_suborchestration_transition(self) -> Tuple[bool, str]:
        if self.is_inside_critical_section:
            return False, "While holding locks, cannot call suborchestrators."
        return True, ""

    def validate_operation_transition(self, target_instance_id: EntityInstanceId, one_way: bool) -> Tuple[bool, str]:
        if self.is_inside_critical_section:
            lock_to_use = target_instance_id
            if one_way:
                if target_instance_id in self.critical_section_locks:
                    return False, "Must not signal a locked entity from a critical section."
            else:
                try:
                    self.available_locks.remove(lock_to_use)
                except ValueError:
                    if self.lock_acquisition_pending:
                        return False, "Must await the completion of the lock request prior to calling any entity."
                    if lock_to_use in self.critical_section_locks:
                        return False, "Must not call an entity from a critical section while a prior call to the same entity is still pending."
                    else:
                        return False, "Must not call an entity from a critical section if it is not one of the locked entities."
        return True, ""

    def validate_acquire_transition(self) -> Tuple[bool, str]:
        if self.is_inside_critical_section:
            return False, "Must not enter another critical section from within a critical section."
        return True, ""

    def recover_lock_after_call(self, target_instance_id: EntityInstanceId):
        if self.is_inside_critical_section:
            self.available_locks.append(target_instance_id)

    def emit_lock_release_messages(self):
        raise NotImplementedError()

    def emit_request_message(self, target, operation_name: str, one_way: bool, operation_id: str,
                             scheduled_time_utc: datetime, input: Optional[str],
                             request_time: Optional[datetime] = None, create_trace: bool = False):
        raise NotImplementedError()

    def emit_acquire_message(self, lock_request_id: str, entities: List[str]):
        raise NotImplementedError()

    def complete_acquire(self, result, critical_section_id):
        # TODO: HashSet or equivalent
        self.available_locks = self.critical_section_locks
        self.lock_acquisition_pending = False

    def adjust_outgoing_message(self, instance_id: str, request_message, capped_time: datetime) -> str:
        raise NotImplementedError()

    def deserialize_entity_response_event(self, event_content: str):
        raise NotImplementedError()
