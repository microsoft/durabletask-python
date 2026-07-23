# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Azure Functions history-export client surface.

Re-exports the ``durabletask`` history-export API, replacing
:class:`ExportHistoryClient` with a Functions-aware subclass that rejects
``ExportMode.CONTINUOUS``.

Continuous export tails terminal instances indefinitely, which requires the
host's ``ListInstanceIds`` call (a server-side completed-time cursor). The
Durable Functions host extension does not implement it, so the Functions
enumeration path queries instances and pages a *fixed* completed-time window
client-side -- correct for a bounded ``BATCH`` export, but unable to safely tail
a growing set (new completions can be missed). Continuous mode is therefore
rejected here until host support lands, at which point the core client tails
correctly and this override can be removed.
"""

from __future__ import annotations

from durabletask.extensions.history_export import (
    ExportDestination,
    ExportFormat,
    ExportFormatKind,
    ExportJobCreationOptions,
    ExportJobDescription,
    ExportMode,
)
from durabletask.extensions.history_export import (
    ExportHistoryClient as _CoreExportHistoryClient,
)

__all__ = [
    "ExportDestination",
    "ExportFormat",
    "ExportFormatKind",
    "ExportHistoryClient",
    "ExportJobCreationOptions",
    "ExportJobDescription",
    "ExportMode",
]


class ExportHistoryClient(_CoreExportHistoryClient):
    """History-export client for Azure Functions.

    Behaves like
    :class:`durabletask.extensions.history_export.ExportHistoryClient` but
    rejects ``ExportMode.CONTINUOUS`` jobs, which the Functions host cannot tail
    safely (see the module docstring).
    """

    def create_job(
            self,
            options: ExportJobCreationOptions,
            *,
            job_id: str | None = None) -> ExportJobDescription:
        if options.mode is ExportMode.CONTINUOUS:
            raise ValueError(
                "Continuous history export is not supported on Azure Functions. "
                "Use ExportMode.BATCH with a bounded completed-time window. "
                "Continuous export requires host ListInstanceIds support, which "
                "the Durable Functions host extension does not yet implement.")
        return super().create_job(options, job_id=job_id)
