from datetime import datetime
from typing import Generator, List, Optional, Tuple, Union

from durabletask.internal.helpers import get_string_value
import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask.entities import EntityInstanceId


class OrchestrationEntityContext:
    def __init__(self, instance_id: str):
        self.instance_id = instance_id

        self.lock_acquisition_pending = False

        self.critical_section_id = None
        self.critical_section_locks: list[EntityInstanceId] = []
        self.available_locks: list[EntityInstanceId] = []

    @property
    def is_inside_critical_section(self) -> bool:
        return self.critical_section_id is not None

    def get_available_entities(self) -> Generator[EntityInstanceId, None, None]:
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
        if self.is_inside_critical_section:
            for entity_id in self.critical_section_locks:
                unlock_event = pb.SendEntityMessageAction(entityUnlockSent=pb.EntityUnlockSentEvent(
                    criticalSectionId=self.critical_section_id,
                    targetInstanceId=get_string_value(str(entity_id)),
                    parentInstanceId=get_string_value(self.instance_id)
                ))
                yield unlock_event

            # TODO: Emit the actual release messages (?)
            self.critical_section_locks = []
            self.available_locks = []
            self.critical_section_id = None

    def emit_request_message(self, target, operation_name: str, one_way: bool, operation_id: str,
                             scheduled_time_utc: datetime, input: Optional[str],
                             request_time: Optional[datetime] = None, create_trace: bool = False):
        raise NotImplementedError()

    def emit_acquire_message(self, critical_section_id: str, entities: List[EntityInstanceId]) -> Union[Tuple[None, None], Tuple[pb.SendEntityMessageAction, pb.OrchestrationInstance]]:
        if not entities:
            return None, None

        # Acquire the locks in a globally fixed order to avoid deadlocks
        # Also remove duplicates - this can be optimized for perf if necessary
        entity_ids = sorted(entities)
        entity_ids_dedup = []
        for i, entity_id in enumerate(entity_ids):
            if entity_id != entity_ids[i - 1] if i > 0 else True:
                entity_ids_dedup.append(entity_id)

        target = pb.OrchestrationInstance(instanceId=str(entity_ids_dedup[0]))
        request = pb.SendEntityMessageAction(entityLockRequested=pb.EntityLockRequestedEvent(
            criticalSectionId=critical_section_id,
            parentInstanceId=get_string_value(self.instance_id),
            lockSet=[str(eid) for eid in entity_ids_dedup],
            position=0,
        ))

        self.critical_section_id = critical_section_id
        self.critical_section_locks = entity_ids_dedup
        self.lock_acquisition_pending = True

        return request, target

    def complete_acquire(self, critical_section_id):
        # TODO: HashSet or equivalent
        if self.critical_section_id != critical_section_id:
            raise RuntimeError(f"Unexpected lock acquire for critical section ID '{critical_section_id}' (expected '{self.critical_section_id}')")
        self.available_locks = self.critical_section_locks
        self.lock_acquisition_pending = False

    def adjust_outgoing_message(self, instance_id: str, request_message, capped_time: datetime) -> str:
        raise NotImplementedError()

    def deserialize_entity_response_event(self, event_content: str):
        raise NotImplementedError()
