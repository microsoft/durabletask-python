# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from durabletask.scheduled.schedule_status import ScheduleStatus

# Operation names used by the Schedule entity. These must match the entity
# method names so that the transition table can be keyed by operation.
CREATE_SCHEDULE = "create_schedule"
UPDATE_SCHEDULE = "update_schedule"
PAUSE_SCHEDULE = "pause_schedule"
RESUME_SCHEDULE = "resume_schedule"


def is_valid_transition(operation_name: str, from_status: ScheduleStatus,
                        target_status: ScheduleStatus) -> bool:
    """Check whether a transition to the target status is valid for the given operation.

    Parameters
    ----------
    operation_name : str
        The name of the operation being performed.
    from_status : ScheduleStatus
        The current schedule status.
    target_status : ScheduleStatus
        The status the schedule would transition to.

    Returns
    -------
    bool
        True if the transition is valid; otherwise False.
    """
    if operation_name == CREATE_SCHEDULE:
        return (
            (from_status == ScheduleStatus.UNINITIALIZED and target_status == ScheduleStatus.ACTIVE)
            or (from_status == ScheduleStatus.ACTIVE and target_status == ScheduleStatus.ACTIVE)
            or (from_status == ScheduleStatus.PAUSED and target_status == ScheduleStatus.ACTIVE)
        )
    if operation_name == UPDATE_SCHEDULE:
        return (
            (from_status == ScheduleStatus.ACTIVE and target_status == ScheduleStatus.ACTIVE)
            or (from_status == ScheduleStatus.PAUSED and target_status == ScheduleStatus.PAUSED)
        )
    if operation_name == PAUSE_SCHEDULE:
        return from_status == ScheduleStatus.ACTIVE and target_status == ScheduleStatus.PAUSED
    if operation_name == RESUME_SCHEDULE:
        return from_status == ScheduleStatus.PAUSED and target_status == ScheduleStatus.ACTIVE
    return False
