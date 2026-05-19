# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""State-transition matrix for the export-job entity.

The matrix is a single, declarative source of truth for which entity
operation may transition the job from which current status to which
new status.  Mirrors the .NET ``ExportJobTransitions`` design.

Conventions
-----------
``None`` is used as the "from" key to represent the absence of any
persisted state (a brand-new entity).  The transitions table is
consulted by :class:`~durabletask.extensions.history_export.entity.ExportJobEntity`
before any status-changing operation.
"""

from __future__ import annotations

from typing import Mapping, Optional

from durabletask.extensions.history_export.exceptions import (
    ExportJobInvalidTransitionError,
)
from durabletask.extensions.history_export.models import ExportJobStatus


# Maps (operation_name, current_status_or_None) -> {valid target statuses}.
TRANSITIONS: Mapping[tuple[str, Optional[ExportJobStatus]], frozenset[ExportJobStatus]] = {
    # ``create`` initialises a fresh job and revives terminal jobs.
    ("create", None): frozenset({ExportJobStatus.PENDING}),
    ("create", ExportJobStatus.FAILED): frozenset({ExportJobStatus.PENDING}),
    ("create", ExportJobStatus.COMPLETED): frozenset({ExportJobStatus.PENDING}),

    # ``run`` flips the job from PENDING to ACTIVE.  Idempotent so the
    # client may signal it more than once without crashing the entity.
    ("run", ExportJobStatus.PENDING): frozenset({ExportJobStatus.ACTIVE}),
    ("run", ExportJobStatus.ACTIVE): frozenset({ExportJobStatus.ACTIVE}),

    # ``commit_checkpoint`` is a no-op transition during normal runs.
    # When the orchestrator signals ``mark_failed_on_batch`` the entity
    # transitions ACTIVE -> FAILED; that is also allowed here.
    ("commit_checkpoint", ExportJobStatus.ACTIVE): frozenset({
        ExportJobStatus.ACTIVE,
        ExportJobStatus.FAILED,
    }),

    ("mark_completed", ExportJobStatus.ACTIVE): frozenset({ExportJobStatus.COMPLETED}),

    # ``mark_failed`` from PENDING covers the rare case of a failure
    # happening between create and run.
    ("mark_failed", ExportJobStatus.PENDING): frozenset({ExportJobStatus.FAILED}),
    ("mark_failed", ExportJobStatus.ACTIVE): frozenset({ExportJobStatus.FAILED}),
}


def is_valid_transition(
    operation: str,
    from_status: Optional[ExportJobStatus],
    to_status: ExportJobStatus,
) -> bool:
    """Return whether *to_status* is reachable from *from_status* via *operation*."""
    targets = TRANSITIONS.get((operation, from_status))
    return targets is not None and to_status in targets


def assert_valid_transition(
    operation: str,
    from_status: Optional[ExportJobStatus],
    to_status: ExportJobStatus,
    *,
    job_id: Optional[str] = None,
) -> None:
    """Raise :class:`ExportJobInvalidTransitionError` for invalid transitions."""
    if not is_valid_transition(operation, from_status, to_status):
        raise ExportJobInvalidTransitionError(
            operation=operation,
            from_status=from_status.value if from_status is not None else None,
            to_status=to_status.value,
            job_id=job_id,
        )


__all__ = ["TRANSITIONS", "is_valid_transition", "assert_valid_transition"]
