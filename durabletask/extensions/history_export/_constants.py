# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Stable, cross-cutting constants for the history-export extension.

These constants are imported by both the entity and the orchestrator
modules.  Keeping them in their own module avoids the circular import
that would arise if either of those modules imported the other.
"""

from __future__ import annotations

ENTITY_NAME = "ExportJobEntity"
"""Logical name of the export-job durable entity."""

ORCHESTRATOR_NAME = "export_job_orchestrator"
"""Function-derived name of the export-job orchestrator."""

ORCHESTRATOR_INSTANCE_ID_PREFIX = "export-job-"
"""Prefix applied to deterministic orchestrator instance IDs."""


def orchestrator_instance_id_for(job_id: str) -> str:
    """Return the deterministic orchestrator instance ID for *job_id*.

    All export-job orchestrators share a stable instance-ID pattern so
    that public clients can reliably correlate a job ID with the
    orchestrator driving it (for logs, monitoring, restart, etc.).
    Matches the .NET ``ExportHistoryConstants.GetOrchestratorInstanceId``
    pattern.
    """
    if not job_id:
        raise ValueError("job_id must be a non-empty string")
    return f"{ORCHESTRATOR_INSTANCE_ID_PREFIX}{job_id}"
