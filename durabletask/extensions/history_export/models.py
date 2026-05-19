# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Public data models for the history export extension.

These dataclasses describe export jobs at the public API surface.  All
JSON-primitive conversions (for entity state, orchestrator inputs, and
activity inputs) are implemented as ``_to_dict`` / ``_from_dict`` pairs
in this module so the rest of the extension can stay free of ad-hoc
serialization logic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, List, Mapping, Optional

from durabletask.client import OrchestrationStatus


class ExportMode(Enum):
    """How the export job processes instances."""

    BATCH = "Batch"
    """Export a fixed time window of terminal instances, then complete."""

    CONTINUOUS = "Continuous"
    """Tail terminal instances continuously until stopped externally.

    The orchestrator processes one page, signals a checkpoint, sleeps
    when the page is empty, and never calls ``mark_completed``.  The
    job is stopped by deleting the entity (via
    :meth:`ExportHistoryClient.delete_job`) or by signalling
    ``mark_failed`` externally.
    """


class ExportFormatKind(Enum):
    """Serialization format for exported history."""

    JSON = "Json"
    """Single JSON document per instance (uncompressed)."""

    JSONL_GZIP = "JsonlGzip"
    """One JSON event per line, gzip compressed."""


class ExportJobStatus(Enum):
    """Lifecycle status of an export job.

    The enum values double as the persisted ``status`` field in the
    export-job entity state.  Adding a new value here is a public-API
    change because consumers may switch on the result of
    :meth:`ExportHistoryClient.get_job`.

    Status meanings
    ---------------
    ``PENDING``
        The job has been created but the entity has not yet processed
        the ``create`` signal, *or* the entity has accepted the
        configuration but has not yet kicked off its driving
        orchestrator.  The value is reserved for the forthcoming
        ``run`` operation (see the .NET ``ExportJob.Run`` pattern);
        the current implementation transitions directly from creation
        to :attr:`ACTIVE`, so jobs are not persisted in ``Pending``
        today.
    ``ACTIVE``
        The job is running and the driving orchestrator is making
        progress through pages of terminal instances.
    ``COMPLETED``
        The orchestrator finished a batch successfully.
    ``FAILED``
        The orchestrator threw, or a page of exports exhausted its
        retries.
    """

    PENDING = "Pending"
    ACTIVE = "Active"
    COMPLETED = "Completed"
    FAILED = "Failed"


# Default set of runtime statuses considered "terminal" for export.
_DEFAULT_TERMINAL_STATUSES: List[OrchestrationStatus] = [
    OrchestrationStatus.COMPLETED,
    OrchestrationStatus.FAILED,
    OrchestrationStatus.TERMINATED,
]


# ----------------------------------------------------------------------
# Datetime helpers
# ----------------------------------------------------------------------

def _dt_to_iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat()


def _dt_from_iso(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


# ----------------------------------------------------------------------
# Configuration dataclasses
# ----------------------------------------------------------------------

@dataclass
class ExportFormat:
    """Output format for serialized history."""

    kind: ExportFormatKind = ExportFormatKind.JSONL_GZIP
    schema_version: str = "1.0"

    def _to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind.value, "schema_version": self.schema_version}

    @classmethod
    def _from_dict(cls, data: Mapping[str, Any]) -> "ExportFormat":
        return cls(
            kind=ExportFormatKind(data["kind"]),
            schema_version=data.get("schema_version", "1.0"),
        )


@dataclass
class ExportDestination:
    """Identifies where exported history should be written.

    The destination is destination-agnostic at this layer; the writer
    implementation (for example Azure Blob Storage) is selected by the
    extension package configuration, not by this dataclass.

    Attributes:
        container: Logical container name (for example, an Azure Blob
            container).  Required.
        prefix: Optional prefix prepended to each exported blob/object
            name.  Useful for grouping exports of the same job.
    """

    container: str
    prefix: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.container:
            raise ValueError("destination.container must be a non-empty string")

    def _to_dict(self) -> dict[str, Any]:
        return {"container": self.container, "prefix": self.prefix}

    @classmethod
    def _from_dict(cls, data: Mapping[str, Any]) -> "ExportDestination":
        return cls(container=data["container"], prefix=data.get("prefix"))


@dataclass
class ExportFilter:
    """Filter applied when selecting instances to export.

    Attributes:
        completed_time_from: Inclusive lower bound on instance
            completion time.  Required.
        completed_time_to: Exclusive upper bound on instance
            completion time.  Required for batch mode.
        runtime_status: Restrict to a specific set of terminal
            statuses.  Defaults to ``COMPLETED``, ``FAILED``, and
            ``TERMINATED``.
    """

    completed_time_from: datetime
    completed_time_to: Optional[datetime] = None
    runtime_status: Optional[List[OrchestrationStatus]] = None

    def effective_runtime_status(self) -> List[OrchestrationStatus]:
        """Return the runtime statuses to use, applying the default."""
        if self.runtime_status is None:
            return list(_DEFAULT_TERMINAL_STATUSES)
        return list(self.runtime_status)

    def _to_dict(self) -> dict[str, Any]:
        return {
            "completed_time_from": _dt_to_iso(self.completed_time_from),
            "completed_time_to": _dt_to_iso(self.completed_time_to),
            "runtime_status": (
                [s.name for s in self.runtime_status]
                if self.runtime_status is not None
                else None
            ),
        }

    @classmethod
    def _from_dict(cls, data: Mapping[str, Any]) -> "ExportFilter":
        statuses = data.get("runtime_status")
        completed_from = _dt_from_iso(data.get("completed_time_from"))
        if completed_from is None:
            raise ValueError("completed_time_from is required")
        return cls(
            completed_time_from=completed_from,
            completed_time_to=_dt_from_iso(data.get("completed_time_to")),
            runtime_status=(
                [OrchestrationStatus[name] for name in statuses]
                if statuses is not None
                else None
            ),
        )


@dataclass
class ExportCheckpoint:
    """Continuation state for resumable exports.

    Attributes:
        last_instance_key: Opaque continuation token returned by the
            backend's ``ListInstanceIds`` API.  ``None`` indicates the
            export has not started or has completed.
    """

    last_instance_key: Optional[str] = None

    def _to_dict(self) -> dict[str, Any]:
        return {"last_instance_key": self.last_instance_key}

    @classmethod
    def _from_dict(cls, data: Mapping[str, Any]) -> "ExportCheckpoint":
        return cls(last_instance_key=data.get("last_instance_key"))


@dataclass
class ExportFailure:
    """Records a single instance that failed to export."""

    instance_id: str
    reason: str
    attempt_count: int
    last_attempt: datetime

    def _to_dict(self) -> dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "reason": self.reason,
            "attempt_count": self.attempt_count,
            "last_attempt": _dt_to_iso(self.last_attempt),
        }

    @classmethod
    def _from_dict(cls, data: Mapping[str, Any]) -> "ExportFailure":
        last_attempt = _dt_from_iso(data["last_attempt"])
        assert last_attempt is not None
        return cls(
            instance_id=data["instance_id"],
            reason=data["reason"],
            attempt_count=int(data["attempt_count"]),
            last_attempt=last_attempt,
        )


@dataclass
class ExportJobConfiguration:
    """Resolved configuration for a running export job."""

    mode: ExportMode
    filter: ExportFilter
    destination: ExportDestination
    format: ExportFormat = field(default_factory=ExportFormat)
    max_instances_per_batch: int = 100
    max_parallel_exports: int = 32

    def __post_init__(self) -> None:
        if self.max_instances_per_batch <= 0:
            raise ValueError("max_instances_per_batch must be positive")
        if self.max_parallel_exports <= 0:
            raise ValueError("max_parallel_exports must be positive")
        if self.mode == ExportMode.BATCH and self.filter.completed_time_to is None:
            raise ValueError(
                "completed_time_to is required for batch mode exports"
            )

    def _to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode.value,
            "filter": self.filter._to_dict(),
            "destination": self.destination._to_dict(),
            "format": self.format._to_dict(),
            "max_instances_per_batch": self.max_instances_per_batch,
            "max_parallel_exports": self.max_parallel_exports,
        }

    @classmethod
    def _from_dict(cls, data: Mapping[str, Any]) -> "ExportJobConfiguration":
        return cls(
            mode=ExportMode(data["mode"]),
            filter=ExportFilter._from_dict(data["filter"]),
            destination=ExportDestination._from_dict(data["destination"]),
            format=ExportFormat._from_dict(data.get("format") or {"kind": ExportFormatKind.JSONL_GZIP.value}),
            max_instances_per_batch=int(data.get("max_instances_per_batch", 100)),
            max_parallel_exports=int(data.get("max_parallel_exports", 32)),
        )


@dataclass
class ExportJobQuery:
    """Filter for :meth:`ExportHistoryClient.list_jobs`.

    Attributes:
        status: When set, only jobs whose persisted status is one of
            these values are returned.
        last_modified_from: When set, only jobs modified at or after
            this timestamp are returned.
        last_modified_to: When set, only jobs modified at or before
            this timestamp are returned.
        page_size: Backend page size used to enumerate the underlying
            entities.
        include_state: Whether to fetch full job state (set ``False``
            to retrieve job IDs and metadata only).
    """

    status: Optional[List["ExportJobStatus"]] = None
    last_modified_from: Optional[datetime] = None
    last_modified_to: Optional[datetime] = None
    page_size: Optional[int] = None
    include_state: bool = True


@dataclass
class ExportJobCreationOptions:
    """User-supplied options for creating a new export job."""

    mode: ExportMode
    completed_time_from: datetime
    destination: ExportDestination
    completed_time_to: Optional[datetime] = None
    runtime_status: Optional[List[OrchestrationStatus]] = None
    format: ExportFormat = field(default_factory=ExportFormat)
    job_id: Optional[str] = None
    max_instances_per_batch: int = 100
    max_parallel_exports: int = 32

    def to_configuration(self) -> ExportJobConfiguration:
        """Resolve into a fully-populated :class:`ExportJobConfiguration`."""
        return ExportJobConfiguration(
            mode=self.mode,
            filter=ExportFilter(
                completed_time_from=self.completed_time_from,
                completed_time_to=self.completed_time_to,
                runtime_status=self.runtime_status,
            ),
            destination=self.destination,
            format=self.format,
            max_instances_per_batch=self.max_instances_per_batch,
            max_parallel_exports=self.max_parallel_exports,
        )


# ----------------------------------------------------------------------
# Persisted entity state
# ----------------------------------------------------------------------
#
# The export-job entity persists its state through the SDK's default JSON
# encoder, which only handles JSON-primitive types.  Rather than encoding
# Python class metadata into the persisted payload (which is a known
# deserialization-attack vector), we serialize through an explicit, named,
# schema-versioned shape defined by :class:`ExportJobState`.
#
# Every persisted dict carries a ``schema_version`` string.  Loading code
# dispatches on that version, never on a class name or module path.  When the
# SDK eventually grows type-aware deserialization, the dispatch can be
# replaced with a registry keyed by ``(entity_name, schema_version)`` without
# changing the on-disk shape.

STATE_SCHEMA_VERSION = "1.0"
"""The schema version emitted by :meth:`ExportJobState._to_dict`.

Increment this when the persisted shape changes in a non-backward-compatible
way and add a new branch in :meth:`ExportJobState._from_dict`.
"""


@dataclass
class ExportJobState:
    """Typed, schema-versioned mirror of the entity's persisted state.

    This dataclass is the single source of truth for the on-disk schema.
    All persistence flows through :meth:`_to_dict` (write) and
    :meth:`_from_dict` (read); the dict contains only JSON primitives plus
    nested dicts produced by the model ``_to_dict`` methods.  No Python
    class names, module paths, or other type metadata appear in the
    serialized form.
    """

    status: ExportJobStatus
    config: ExportJobConfiguration
    created_at: datetime
    last_modified_at: datetime
    orchestrator_instance_id: Optional[str] = None
    checkpoint: ExportCheckpoint = field(default_factory=ExportCheckpoint)
    last_checkpoint_time: Optional[datetime] = None
    last_error: Optional[str] = None
    scanned_instances: int = 0
    exported_instances: int = 0
    failed_instances: int = 0
    failures: List[ExportFailure] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def _to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": STATE_SCHEMA_VERSION,
            "status": self.status.value,
            "config": self.config._to_dict(),
            "checkpoint": self.checkpoint._to_dict(),
            "created_at": _dt_to_iso(self.created_at),
            "last_modified_at": _dt_to_iso(self.last_modified_at),
            "last_checkpoint_time": _dt_to_iso(self.last_checkpoint_time),
            "last_error": self.last_error,
            "scanned_instances": self.scanned_instances,
            "exported_instances": self.exported_instances,
            "failed_instances": self.failed_instances,
            "orchestrator_instance_id": self.orchestrator_instance_id,
            "failures": [f._to_dict() for f in self.failures],
        }

    @classmethod
    def _from_dict(cls, data: Mapping[str, Any]) -> "ExportJobState":
        version = data.get("schema_version", "1.0")
        if version != STATE_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported export job state schema_version={version!r}; "
                f"expected {STATE_SCHEMA_VERSION!r}"
            )

        config_data = data.get("config")
        if not config_data:
            raise ValueError("persisted state is missing 'config'")
        created_at = _dt_from_iso(data.get("created_at"))
        last_modified_at = _dt_from_iso(data.get("last_modified_at"))
        if created_at is None or last_modified_at is None:
            raise ValueError(
                "persisted state must include 'created_at' and 'last_modified_at'"
            )
        checkpoint_data = data.get("checkpoint")
        failures_data = data.get("failures") or []

        return cls(
            status=ExportJobStatus(data["status"]),
            config=ExportJobConfiguration._from_dict(config_data),
            created_at=created_at,
            last_modified_at=last_modified_at,
            orchestrator_instance_id=data.get("orchestrator_instance_id"),
            checkpoint=(
                ExportCheckpoint._from_dict(checkpoint_data)
                if checkpoint_data is not None
                else ExportCheckpoint()
            ),
            last_checkpoint_time=_dt_from_iso(data.get("last_checkpoint_time")),
            last_error=data.get("last_error"),
            scanned_instances=int(data.get("scanned_instances", 0)),
            exported_instances=int(data.get("exported_instances", 0)),
            failed_instances=int(data.get("failed_instances", 0)),
            failures=[ExportFailure._from_dict(f) for f in failures_data],
        )

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def new(
        cls,
        config: ExportJobConfiguration,
        *,
        created_at: datetime,
        orchestrator_instance_id: Optional[str] = None,
    ) -> "ExportJobState":
        """Construct a fresh state for a newly-created job."""
        return cls(
            status=ExportJobStatus.ACTIVE,
            config=config,
            created_at=created_at,
            last_modified_at=created_at,
            orchestrator_instance_id=orchestrator_instance_id,
        )


# ----------------------------------------------------------------------
# Public job description (read view)
# ----------------------------------------------------------------------

@dataclass
class ExportJobDescription:
    """Public view of an export job."""

    job_id: str
    status: ExportJobStatus
    created_at: Optional[datetime]
    last_modified_at: Optional[datetime]
    config: Optional[ExportJobConfiguration]
    orchestrator_instance_id: Optional[str]
    scanned_instances: int
    exported_instances: int
    failed_instances: int
    last_error: Optional[str]
    checkpoint: Optional[ExportCheckpoint]
    last_checkpoint_time: Optional[datetime]
    failures: List[ExportFailure] = field(default_factory=list)

    @classmethod
    def _from_state(cls, job_id: str, state: "ExportJobState") -> "ExportJobDescription":
        return cls(
            job_id=job_id,
            status=state.status,
            created_at=state.created_at,
            last_modified_at=state.last_modified_at,
            config=state.config,
            orchestrator_instance_id=state.orchestrator_instance_id,
            scanned_instances=state.scanned_instances,
            exported_instances=state.exported_instances,
            failed_instances=state.failed_instances,
            last_error=state.last_error,
            checkpoint=state.checkpoint,
            last_checkpoint_time=state.last_checkpoint_time,
            failures=list(state.failures),
        )

    @classmethod
    def _from_state_dict(
        cls, job_id: str, state: Mapping[str, Any]
    ) -> "ExportJobDescription":
        """Build a description from a persisted entity-state dict."""
        return cls._from_state(job_id, ExportJobState._from_dict(state))
