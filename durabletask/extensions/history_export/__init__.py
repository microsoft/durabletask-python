# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Orchestration history export for Durable Task.

This optional extension package provides a workflow for exporting
orchestration history from terminal instances to a configured
destination, modeled after the durabletask-dotnet ``ExportHistory``
package.

The core building blocks (models, durable entity, activities,
orchestrator, and the public client) live in this package and have
no required runtime dependencies beyond the core SDK.  Specific
destinations (for example Azure Blob Storage) may require optional
dependencies; see the destination module documentation for details.
"""

from durabletask.extensions.history_export._constants import (
    ENTITY_NAME,
    ORCHESTRATOR_NAME,
    orchestrator_instance_id_for,
)
from durabletask.extensions.history_export.activities import (
    HistoryExportContext,
)
from durabletask.extensions.history_export.client import (
    ExportHistoryClient,
    ExportHistoryJobClient,
)
from durabletask.extensions.history_export.exceptions import (
    ExportJobError,
    ExportJobInvalidTransitionError,
    ExportJobNotFoundError,
)
from durabletask.extensions.history_export.models import (
    STATE_SCHEMA_VERSION,
    ExportCheckpoint,
    ExportDestination,
    ExportFailure,
    ExportFilter,
    ExportFormat,
    ExportFormatKind,
    ExportJobConfiguration,
    ExportJobCreationOptions,
    ExportJobDescription,
    ExportJobQuery,
    ExportJobState,
    ExportJobStatus,
    ExportMode,
)
from durabletask.extensions.history_export.writer import HistoryWriter

__all__ = [
    "ENTITY_NAME",
    "ORCHESTRATOR_NAME",
    "STATE_SCHEMA_VERSION",
    "ExportCheckpoint",
    "ExportDestination",
    "ExportFailure",
    "ExportFilter",
    "ExportFormat",
    "ExportFormatKind",
    "ExportHistoryClient",
    "ExportHistoryJobClient",
    "ExportJobConfiguration",
    "ExportJobCreationOptions",
    "ExportJobDescription",
    "ExportJobError",
    "ExportJobInvalidTransitionError",
    "ExportJobNotFoundError",
    "ExportJobQuery",
    "ExportJobState",
    "ExportJobStatus",
    "ExportMode",
    "HistoryExportContext",
    "HistoryWriter",
    "orchestrator_instance_id_for",
]

PACKAGE_NAME = "durabletask.extensions.history_export"
