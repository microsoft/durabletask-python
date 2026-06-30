# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Scheduled tasks support for the Durable Task SDK.

This package provides a recurring schedule feature built on top of durable
entities and a helper orchestrator. Enable it on a worker via
:meth:`durabletask.worker.TaskHubGrpcWorker.configure_scheduled_tasks`, then
manage schedules from the client via :class:`ScheduledTaskClient`.
"""

from durabletask.scheduled.client import ScheduleClient, ScheduledTaskClient
from durabletask.scheduled.exceptions import (ScheduleClientValidationError,
                                              ScheduleError,
                                              ScheduleInvalidTransitionError,
                                              ScheduleNotFoundError)
from durabletask.scheduled.models import (ScheduleCreationOptions,
                                          ScheduleDescription, ScheduleQuery,
                                          ScheduleUpdateOptions)
from durabletask.scheduled.schedule_status import ScheduleStatus

__all__ = [
    "ScheduledTaskClient",
    "ScheduleClient",
    "ScheduleCreationOptions",
    "ScheduleUpdateOptions",
    "ScheduleDescription",
    "ScheduleQuery",
    "ScheduleStatus",
    "ScheduleError",
    "ScheduleNotFoundError",
    "ScheduleClientValidationError",
    "ScheduleInvalidTransitionError",
]

PACKAGE_NAME = "durabletask.scheduled"
