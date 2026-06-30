# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from enum import Enum


class ScheduleStatus(str, Enum):
    """Represents the current status of a schedule."""

    UNINITIALIZED = "Uninitialized"
    """Schedule has not been created."""

    ACTIVE = "Active"
    """Schedule is active and running."""

    PAUSED = "Paused"
    """Schedule is paused."""
