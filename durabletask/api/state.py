from dataclasses import dataclass
from datetime import datetime

import durabletask.protos.orchestrator_service_pb2 as pb
import durabletask.protos.helpers as helpers
from durabletask.protos.orchestrator_service_pb2 import TaskFailureDetails


@dataclass
class OrchestrationState:
    instance_id: str
    name: str
    runtime_status: pb.OrchestrationStatus
    created_at: datetime
    last_updated_at: datetime
    serialized_input: str | None
    serialized_output: str | None
    serialized_custom_status: str | None
    failure_details: pb.TaskFailureDetails | None


def new_orchestration_state(instance_id: str, res: pb.GetInstanceResponse) -> OrchestrationState | None:
    if not res.exists:
        return None

    state = res.orchestrationState
    return OrchestrationState(
        instance_id,
        state.name,
        state.orchestrationStatus,
        state.createdTimestamp.ToDatetime(),
        state.lastUpdatedTimestamp.ToDatetime(),
        state.input.value if not helpers.is_empty(state.input) else None,
        state.output.value if not helpers.is_empty(state.output) else None,
        state.customStatus.value if not helpers.is_empty(state.customStatus) else None,
        state.failureDetails if state.failureDetails.errorMessage != '' or state.failureDetails.errorType != '' else None)
