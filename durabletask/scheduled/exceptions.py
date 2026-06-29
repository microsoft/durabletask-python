# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.


class ScheduleError(Exception):
    """Base class for schedule-related errors."""


class ScheduleNotFoundError(ScheduleError):
    """Raised when a requested schedule does not exist."""

    def __init__(self, schedule_id: str):
        self.schedule_id = schedule_id
        super().__init__(f"Schedule with ID '{schedule_id}' was not found.")


class ScheduleClientValidationError(ScheduleError):
    """Raised when a schedule operation fails client-side validation."""

    def __init__(self, schedule_id: str, message: str):
        self.schedule_id = schedule_id
        super().__init__(f"Validation failed for schedule '{schedule_id}': {message}")


class ScheduleInvalidTransitionError(ScheduleError):
    """Raised when an operation is not valid for the schedule's current status."""

    def __init__(self, schedule_id: str, from_status: object, to_status: object, operation_name: str):
        self.schedule_id = schedule_id
        self.from_status = from_status
        self.to_status = to_status
        self.operation_name = operation_name
        super().__init__(
            f"Invalid state transition for schedule '{schedule_id}': operation "
            f"'{operation_name}' cannot transition from '{from_status}' to '{to_status}'."
        )
