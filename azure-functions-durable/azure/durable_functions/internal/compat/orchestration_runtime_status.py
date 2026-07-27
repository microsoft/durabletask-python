# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from enum import Enum

from durabletask.client import OrchestrationStatus


class OrchestrationRuntimeStatus(Enum):
    """The status of an orchestration instance.

    Backwards-compatible enum matching the v1 ``OrchestrationRuntimeStatus``
    values. New code should use :class:`durabletask.client.OrchestrationStatus`.
    """

    Running = 'Running'
    """The orchestration instance has started running."""

    Completed = 'Completed'
    """The orchestration instance has completed normally."""

    ContinuedAsNew = 'ContinuedAsNew'
    """The orchestration instance has restarted itself with a new history.

    This is a transient state.
    """

    Failed = 'Failed'
    """The orchestration instance failed with an error."""

    Canceled = 'Canceled'
    """The orchestration was canceled gracefully."""

    Terminated = 'Terminated'
    """The orchestration instance was stopped abruptly."""

    Pending = 'Pending'
    """The orchestration instance has been scheduled but has not yet started running."""

    Suspended = 'Suspended'
    """The orchestration instance has been suspended and may go back to running at a later time."""


# Maps the v1 OrchestrationRuntimeStatus members onto the durabletask
# OrchestrationStatus enum. ``Canceled`` has no durabletask equivalent.
_TO_DURABLETASK_STATUS: dict[OrchestrationRuntimeStatus, OrchestrationStatus] = {
    OrchestrationRuntimeStatus.Running: OrchestrationStatus.RUNNING,
    OrchestrationRuntimeStatus.Completed: OrchestrationStatus.COMPLETED,
    OrchestrationRuntimeStatus.ContinuedAsNew: OrchestrationStatus.CONTINUED_AS_NEW,
    OrchestrationRuntimeStatus.Failed: OrchestrationStatus.FAILED,
    OrchestrationRuntimeStatus.Terminated: OrchestrationStatus.TERMINATED,
    OrchestrationRuntimeStatus.Pending: OrchestrationStatus.PENDING,
    OrchestrationRuntimeStatus.Suspended: OrchestrationStatus.SUSPENDED,
}


def to_durabletask_status(status: "OrchestrationRuntimeStatus") -> OrchestrationStatus:
    """Convert a v1 ``OrchestrationRuntimeStatus`` to a durabletask ``OrchestrationStatus``.

    Raises
    ------
    ValueError
        If the status has no durabletask equivalent (e.g. ``Canceled``).
    """
    try:
        return _TO_DURABLETASK_STATUS[status]
    except KeyError:
        raise ValueError(
            f"OrchestrationRuntimeStatus.{status.name} has no durabletask "
            "OrchestrationStatus equivalent.")


def to_durabletask_statuses(
        statuses: "list[OrchestrationRuntimeStatus] | None") -> "list[OrchestrationStatus] | None":
    """Convert a list of v1 statuses to durabletask statuses, preserving ``None``."""
    if statuses is None:
        return None
    return [to_durabletask_status(status) for status in statuses]


# Reverse mapping: durabletask OrchestrationStatus -> v1 OrchestrationRuntimeStatus.
# Every durabletask status has a v1 equivalent (``Canceled`` is v1-only).
_FROM_DURABLETASK_STATUS: dict[OrchestrationStatus, OrchestrationRuntimeStatus] = {
    durabletask_status: v1_status
    for v1_status, durabletask_status in _TO_DURABLETASK_STATUS.items()
}


def from_durabletask_status(status: OrchestrationStatus) -> "OrchestrationRuntimeStatus":
    """Convert a durabletask ``OrchestrationStatus`` to a v1 ``OrchestrationRuntimeStatus``.

    Raises
    ------
    ValueError
        If the status has no v1 equivalent.
    """
    try:
        return _FROM_DURABLETASK_STATUS[status]
    except KeyError:
        raise ValueError(
            f"OrchestrationStatus {status} has no v1 OrchestrationRuntimeStatus equivalent.")
