# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from durabletask.worker import TaskHubGrpcWorker
from durabletask.scheduled.orchestrator import \
    execute_schedule_operation_orchestrator
from durabletask.scheduled.schedule_entity import ENTITY_NAME, Schedule


def configure_scheduled_tasks(worker: TaskHubGrpcWorker) -> None:
    """Register the scheduled tasks entity and orchestrator with a worker.

    Call this before starting the worker to enable scheduled tasks support.

    Parameters
    ----------
    worker : TaskHubGrpcWorker
        The worker to register the schedule entity and operation orchestrator with.
    """
    worker.add_entity(Schedule, ENTITY_NAME)
    worker.add_orchestrator(execute_schedule_operation_orchestrator)
