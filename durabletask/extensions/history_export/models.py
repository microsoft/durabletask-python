# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Public data models for the history export extension.

These dataclasses describe export jobs at the public API surface.  All
JSON-primitive conversions (for entity state, orchestrator inputs, and
activity inputs) are implemented as ``to_dict`` / ``from_dict`` pairs
in this module so the rest of the extension can stay free of ad-hoc
serialization logic.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from durabletask.client import OrchestrationStatus

from durabletask.extensions.history_export._internal import (
    dt_from_iso,
    dt_to_iso,
)


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
        The job has been created and persisted but the entity has not
        yet kicked off its driving orchestrator.  Jobs sit in this
        state briefly between the ``create`` and ``run`` signals
        (the public client sends both in immediate succession), or
        for longer if ``run`` is never invoked or if a caller revives
        a previously terminal job via ``create``.
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
_DEFAULT_TERMINAL_STATUSES: list[OrchestrationStatus] = [
    OrchestrationStatus.COMPLETED,
    OrchestrationStatus.FAILED,
    OrchestrationStatus.TERMINATED,
]


def _parse_runtime_status(value: Any) -> OrchestrationStatus:
    """Parse a runtime status from its persisted representation.

    Accepts both the current wire format (``.value`` — the protobuf
    integer) and the legacy schema-1.0 format (``.name`` — the enum
    constant name).  Renaming an enum constant in the core SDK is
    therefore non-breaking for persisted state.
    """
    if isinstance(value, OrchestrationStatus):
        return value
    if isinstance(value, int):
        return OrchestrationStatus(value)
    if isinstance(value, str):
        # Try integer-as-string first, then fall back to enum name.
        try:
            return OrchestrationStatus(int(value))
        except (TypeError, ValueError):
            return OrchestrationStatus[value]
    raise TypeError(
        f"Cannot parse runtime status from value of type {type(value).__name__!r}"
    )


# ----------------------------------------------------------------------
# Configuration dataclasses
# ----------------------------------------------------------------------

@dataclass
class ExportFormat:
    """Output format for serialized history."""

    kind: ExportFormatKind = ExportFormatKind.JSONL_GZIP
    schema_version: str = "1.0"

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind.value, "schema_version": self.schema_version}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ExportFormat":
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
    prefix: str | None = None

    def __post_init__(self) -> None:
        if not self.container:
            raise ValueError("destination.container must be a non-empty string")

    def to_dict(self) -> dict[str, Any]:
        return {"container": self.container, "prefix": self.prefix}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ExportDestination":
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
    completed_time_to: datetime | None = None
    runtime_status: list[OrchestrationStatus] | None = None

    def effective_runtime_status(self) -> list[OrchestrationStatus]:
        """Return the runtime statuses to use, applying the default."""
        if self.runtime_status is None:
            return list(_DEFAULT_TERMINAL_STATUSES)
        return list(self.runtime_status)

    def to_dict(self) -> dict[str, Any]:
        return {
            "completed_time_from": dt_to_iso(self.completed_time_from),
            "completed_time_to": dt_to_iso(self.completed_time_to),
            # Persist by ``.value`` (the protobuf integer) rather than
            # ``.name`` so renaming an enum constant in the core SDK
            # does not break previously-persisted job state.
            "runtime_status": (
                [s.value for s in self.runtime_status]
                if self.runtime_status is not None
                else None
            ),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ExportFilter":
        statuses = data.get("runtime_status")
        completed_from = dt_from_iso(data.get("completed_time_from"))
        if completed_from is None:
            raise ValueError("completed_time_from is required")
        return cls(
            completed_time_from=completed_from,
            completed_time_to=dt_from_iso(data.get("completed_time_to")),
            runtime_status=(
                [_parse_runtime_status(s) for s in statuses]
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

    last_instance_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"last_instance_key": self.last_instance_key}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ExportCheckpoint":
        return cls(last_instance_key=data.get("last_instance_key"))


@dataclass
class ExportFailure:
    """Records a single instance that failed to export."""

    instance_id: str
    reason: str
    attempt_count: int
    last_attempt: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "reason": self.reason,
            "attempt_count": self.attempt_count,
            "last_attempt": dt_to_iso(self.last_attempt),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ExportFailure":
        last_attempt = dt_from_iso(data["last_attempt"])
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
        # Bounds on batch sizing.  Upper bound matches the .NET
        # ``ExportJobCreationOptions`` cap to avoid runaway page sizes.
        if not 1 <= self.max_instances_per_batch <= 1000:
            raise ValueError(
                "max_instances_per_batch must be in [1, 1000]; got "
                f"{self.max_instances_per_batch}"
            )
        if self.max_parallel_exports <= 0:
            raise ValueError("max_parallel_exports must be positive")

        # Mode-specific filter validation.
        if self.mode is ExportMode.BATCH and self.filter.completed_time_to is None:
            raise ValueError(
                "completed_time_to is required for batch mode exports"
            )
        if self.mode is ExportMode.CONTINUOUS and self.filter.completed_time_to is not None:
            raise ValueError(
                "completed_time_to is not allowed for continuous mode "
                "exports; the tail has no upper bound"
            )

        # Window must be a strictly-increasing range when both ends
        # are set.  Catches upside-down windows early.
        if (
            self.filter.completed_time_to is not None
            and self.filter.completed_time_to <= self.filter.completed_time_from
        ):
            raise ValueError(
                "completed_time_to must be strictly greater than "
                "completed_time_from"
            )

        # Only terminal statuses make sense for export.  Match the .NET
        # validation set.
        if self.filter.runtime_status is not None:
            disallowed = [
                s for s in self.filter.runtime_status
                if s not in _DEFAULT_TERMINAL_STATUSES
            ]
            if disallowed:
                names = ", ".join(sorted(s.name for s in disallowed))
                raise ValueError(
                    f"runtime_status may only contain terminal statuses "
                    f"({{COMPLETED, FAILED, TERMINATED}}); got {names}"
                )

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode.value,
            "filter": self.filter.to_dict(),
            "destination": self.destination.to_dict(),
            "format": self.format.to_dict(),
            "max_instances_per_batch": self.max_instances_per_batch,
            "max_parallel_exports": self.max_parallel_exports,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ExportJobConfiguration":
        # Use an explicit ``None`` check (rather than ``or``) so that an
        # empty ``format`` dict still goes through ``from_dict`` and
        # raises a clear KeyError, instead of silently being replaced
        # by the default.
        format_data = data.get("format")
        if format_data is None:
            format_data = {"kind": ExportFormatKind.JSONL_GZIP.value}
        return cls(
            mode=ExportMode(data["mode"]),
            filter=ExportFilter.from_dict(data["filter"]),
            destination=ExportDestination.from_dict(data["destination"]),
            format=ExportFormat.from_dict(format_data),
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
    """

    status: list[ExportJobStatus] | None = None
    last_modified_from: datetime | None = None
    last_modified_to: datetime | None = None
    page_size: int | None = None


@dataclass
class ExportJobCreationOptions:
    """User-supplied options for creating a new export job.

    The job ID is **not** an attribute here; pass it explicitly to
    :meth:`ExportHistoryClient.create_job` via the ``job_id`` kwarg,
    or let the client auto-generate one.  Keeping the ID separate from
    the configuration avoids the .NET API's awkward duplication where
    both ``options.JobId`` and a constructor argument could specify
    the same field.
    """

    mode: ExportMode
    completed_time_from: datetime
    destination: ExportDestination
    completed_time_to: datetime | None = None
    runtime_status: list[OrchestrationStatus] | None = None
    format: ExportFormat = field(default_factory=ExportFormat)
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

STATE_SCHEMA_VERSION = "1.1"
"""The schema version emitted by :meth:`ExportJobState.to_dict`.

Increment this when the persisted shape changes in a non-backward-compatible
way and add a new branch in :meth:`ExportJobState.from_dict`.

Version history:

``"1.0"``
    Initial shape.  ``runtime_status`` filter values were persisted as
    enum *names* (e.g. ``"COMPLETED"``), which broke if the core SDK
    renamed an enum constant.  Read support retained.
``"1.1"``
    ``runtime_status`` filter values are persisted as the protobuf
    enum *integer* (e.g. ``2`` for ``COMPLETED``).  Reads still accept
    the legacy 1.0 string form for backward compatibility.
"""


@dataclass
class ExportJobState:
    """Typed, schema-versioned mirror of the entity's persisted state.

    This dataclass is the single source of truth for the on-disk schema.
    All persistence flows through :meth:`to_dict` (write) and
    :meth:`from_dict` (read); the dict contains only JSON primitives plus
    nested dicts produced by the model ``to_dict`` methods.  No Python
    class names, module paths, or other type metadata appear in the
    serialized form.
    """

    status: ExportJobStatus
    config: ExportJobConfiguration
    created_at: datetime
    last_modified_at: datetime
    orchestrator_instance_id: str | None = None
    checkpoint: ExportCheckpoint = field(default_factory=ExportCheckpoint)
    last_checkpoint_time: datetime | None = None
    last_error: str | None = None
    scanned_instances: int = 0
    exported_instances: int = 0
    failed_instances: int = 0
    failures: list[ExportFailure] = field(default_factory=list[ExportFailure])

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": STATE_SCHEMA_VERSION,
            "status": self.status.value,
            "config": self.config.to_dict(),
            "checkpoint": self.checkpoint.to_dict(),
            "created_at": dt_to_iso(self.created_at),
            "last_modified_at": dt_to_iso(self.last_modified_at),
            "last_checkpoint_time": dt_to_iso(self.last_checkpoint_time),
            "last_error": self.last_error,
            "scanned_instances": self.scanned_instances,
            "exported_instances": self.exported_instances,
            "failed_instances": self.failed_instances,
            "orchestrator_instance_id": self.orchestrator_instance_id,
            "failures": [f.to_dict() for f in self.failures],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ExportJobState":
        version = data.get("schema_version", "1.0")
        if version not in {"1.0", "1.1"}:
            raise ValueError(
                f"Unsupported export job state schema_version={version!r}; "
                f"expected one of: '1.0', '1.1' (current: {STATE_SCHEMA_VERSION!r})"
            )

        config_data = data.get("config")
        if not config_data:
            raise ValueError("persisted state is missing 'config'")
        created_at = dt_from_iso(data.get("created_at"))
        last_modified_at = dt_from_iso(data.get("last_modified_at"))
        if created_at is None or last_modified_at is None:
            raise ValueError(
                "persisted state must include 'created_at' and 'last_modified_at'"
            )
        checkpoint_data = data.get("checkpoint")
        failures_data: list[Mapping[str, Any]] = list(data.get("failures") or [])

        return cls(
            status=ExportJobStatus(data["status"]),
            config=ExportJobConfiguration.from_dict(config_data),
            created_at=created_at,
            last_modified_at=last_modified_at,
            orchestrator_instance_id=data.get("orchestrator_instance_id"),
            checkpoint=(
                ExportCheckpoint.from_dict(checkpoint_data)
                if checkpoint_data is not None
                else ExportCheckpoint()
            ),
            last_checkpoint_time=dt_from_iso(data.get("last_checkpoint_time")),
            last_error=data.get("last_error"),
            scanned_instances=int(data.get("scanned_instances", 0)),
            exported_instances=int(data.get("exported_instances", 0)),
            failed_instances=int(data.get("failed_instances", 0)),
            failures=[ExportFailure.from_dict(f) for f in failures_data],
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
        orchestrator_instance_id: str | None = None,
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
    created_at: datetime | None
    last_modified_at: datetime | None
    config: ExportJobConfiguration | None
    orchestrator_instance_id: str | None
    scanned_instances: int
    exported_instances: int
    failed_instances: int
    last_error: str | None
    checkpoint: ExportCheckpoint | None
    last_checkpoint_time: datetime | None
    failures: list[ExportFailure] = field(default_factory=list[ExportFailure])

    @classmethod
    def from_state(cls, job_id: str, state: "ExportJobState") -> "ExportJobDescription":
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
    def from_state_dict(
        cls, job_id: str, state: Mapping[str, Any]
    ) -> "ExportJobDescription":
        """Build a description from a persisted entity-state dict."""
        return cls.from_state(job_id, ExportJobState.from_dict(state))
