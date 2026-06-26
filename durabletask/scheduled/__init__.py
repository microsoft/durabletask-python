# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Scheduled tasks support for the Durable Task SDK.

This package provides a recurring schedule feature built on top of durable
entities and a helper orchestrator. Register the entity and orchestrator with a
worker via :func:`configure_scheduled_tasks`, then manage schedules from the
client via :class:`ScheduledTaskClient`.
"""

from durabletask.scheduled.client import ScheduleClient, ScheduledTaskClient
from durabletask.scheduled.exceptions import (ScheduleClientValidationError,
                                              ScheduleError,
                                              ScheduleInvalidTransitionError,
                                              ScheduleNotFoundError)
from durabletask.scheduled.models import (ScheduleCreationOptions,
                                          ScheduleDescription, ScheduleQuery,
                                          ScheduleUpdateOptions)
from durabletask.scheduled.registration import configure_scheduled_tasks
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
    "configure_scheduled_tasks",
]

PACKAGE_NAME = "durabletask.scheduled"
