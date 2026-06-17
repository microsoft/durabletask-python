# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Activities for the history export workflow.

Two activities cooperate to drive an export job:

* ``list_terminal_instances`` — wraps
  :meth:`TaskHubGrpcClient.list_instance_ids` to fetch one page of
  terminal instance IDs that match the job's filter.

* ``export_instance_history`` — fetches the full history for a single
  instance via :meth:`TaskHubGrpcClient.get_orchestration_history`,
  serializes it with the configured format, and writes the resulting
  blob through a :class:`HistoryWriter`.

The client and writer are not serializable, so they cannot be passed
through orchestrator inputs.  Instead, the public client registers a
module-level :class:`HistoryExportContext` once at worker startup.
The activities resolve their dependencies from that context at
execution time.  This is acceptable because activities run in-process
within the worker that registered them.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, cast

from durabletask import client as client_module
from durabletask import task
from durabletask import worker as worker_module

from durabletask.extensions.history_export._internal import dt_from_iso
from durabletask.extensions.history_export.models import (
    ExportFormat,
    ExportFormatKind,
)
from durabletask.extensions.history_export.serialization import (
    content_encoding_for,
    content_type_for,
    file_extension_for,
    orchestration_state_to_dict,
    serialize_history,
)
from durabletask.extensions.history_export.writer import HistoryWriter


# The set of runtime statuses considered "terminal" by the export
# activity's safety guard.  Matches the .NET ``IsCompleted`` helper.
_TERMINAL_RUNTIME_STATUSES: frozenset[client_module.OrchestrationStatus] = frozenset({
    client_module.OrchestrationStatus.COMPLETED,
    client_module.OrchestrationStatus.FAILED,
    client_module.OrchestrationStatus.TERMINATED,
})


# The activity name registered with the worker is simply ``fn.__name__``
# (see :func:`durabletask.task.get_name`).  These constants exist so
# downstream code (the orchestrator, tests) can refer to the names
# symbolically without re-deriving them from the function objects.
LIST_TERMINAL_INSTANCES_ACTIVITY = "list_terminal_instances"
EXPORT_INSTANCE_HISTORY_ACTIVITY = "export_instance_history"


@dataclass
class HistoryExportContext:
    """Runtime dependencies shared by all history-export activities."""

    client: client_module.TaskHubGrpcClient
    writer: HistoryWriter


_context: HistoryExportContext | None = None


def bind_context(context: HistoryExportContext) -> None:
    """Install the runtime dependencies for the history-export activities.

    The bound context is process-wide.  Calling this more than once in
    the same process — for example by constructing two
    :class:`ExportHistoryClient` instances with different writers —
    silently replaces the previously-bound writer for *all* in-flight
    activities.  Such a rebind emits a logger warning so the
    misconfiguration is visible at runtime.
    """
    global _context
    if _context is not None and _context is not context:
        from durabletask.extensions.history_export._logging import logger
        logger.warning(
            "history_export.bind_context() replacing an existing bound "
            "context (writer=%r); only one writer can be active per process. "
            "Run a separate worker process per writer if you need multiple "
            "destinations.",
            type(context.writer).__name__,
        )
    _context = context


def clear_context() -> None:
    """Remove the bound context.  Useful for tests."""
    global _context
    _context = None


def _require_context() -> HistoryExportContext:
    if _context is None:
        raise RuntimeError(
            "history-export activities invoked without a bound context; "
            "call bind_context(HistoryExportContext(...)) before starting the worker"
        )
    return _context


# ----------------------------------------------------------------------
# Activity bodies
# ----------------------------------------------------------------------

def list_terminal_instances(
    _: task.ActivityContext, input: Mapping[str, Any],
) -> dict[str, Any]:
    """Activity: fetch one page of terminal instance IDs."""
    ctx = _require_context()

    raw_statuses = input.get("runtime_status")
    runtime_status_names: list[str] | None = (
        list(raw_statuses) if raw_statuses is not None else None
    )
    completed_time_from = dt_from_iso(input.get("completed_time_from"))
    completed_time_to = dt_from_iso(input.get("completed_time_to"))
    page_size_raw = input.get("page_size")
    page_size: int | None = int(page_size_raw) if page_size_raw is not None else None
    continuation_token_raw = input.get("continuation_token")
    continuation_token: str | None = (
        str(continuation_token_raw) if continuation_token_raw is not None else None
    )

    if completed_time_from is None:
        raise ValueError("list_terminal_instances requires 'completed_time_from'")

    runtime_status: list[client_module.OrchestrationStatus] | None = None
    if runtime_status_names is not None:
        runtime_status = [
            client_module.OrchestrationStatus[name] for name in runtime_status_names
        ]

    page = ctx.client.list_instance_ids(
        runtime_status=runtime_status,
        completed_time_from=completed_time_from,
        completed_time_to=completed_time_to,
        page_size=page_size,
        continuation_token=continuation_token,
    )

    return {
        "instance_ids": list(page.items),
        "continuation_token": page.continuation_token,
    }


def export_instance_history(
    _: task.ActivityContext, input: Mapping[str, Any],
) -> dict[str, Any]:
    """Activity: serialize and write one instance's history."""
    ctx = _require_context()

    instance_id = str(input["instance_id"])
    fmt_input = input.get("format") or {
        "kind": ExportFormatKind.JSONL_GZIP.value,
        "schema_version": "1.0",
    }
    if not isinstance(fmt_input, Mapping):
        raise TypeError("format must be a mapping")
    fmt = ExportFormat.from_dict(cast("Mapping[str, Any]", fmt_input))
    destination_raw: Mapping[str, Any] = input.get("destination") or {}
    container_raw: Any = destination_raw.get("container")
    if not container_raw:
        raise ValueError("destination.container is required")
    container: str = str(container_raw)
    prefix_raw: Any = destination_raw.get("prefix")
    prefix: str | None = str(prefix_raw) if prefix_raw is not None else None

    try:
        # Resolve the instance's terminal metadata first.  If the
        # instance was purged, deleted, or has somehow re-entered a
        # non-terminal state between ``list_terminal_instances`` and
        # now, we refuse to write a partial/empty blob and surface a
        # specific failure to the orchestrator.  Matches the .NET
        # ``ExportInstanceHistoryActivity`` guard.
        state = ctx.client.get_orchestration_state(
            instance_id, fetch_payloads=True,
        )
        if state is None:
            return {
                "instance_id": instance_id,
                "success": False,
                "error": (
                    f"instance {instance_id!r} no longer exists or has been "
                    "purged"
                ),
            }
        if state.runtime_status not in _TERMINAL_RUNTIME_STATUSES:
            return {
                "instance_id": instance_id,
                "success": False,
                "error": (
                    f"instance {instance_id!r} is no longer terminal "
                    f"(runtime_status={state.runtime_status.name})"
                ),
            }

        events = ctx.client.get_orchestration_history(instance_id)
        # The exported blob is self-describing: it carries the
        # serialized ``OrchestrationState`` metadata alongside the
        # event list.  Matches the .NET behavior.
        metadata = orchestration_state_to_dict(state)
        payload = serialize_history(
            events,
            instance_id=instance_id,
            fmt=fmt,
            metadata=metadata,
        )
        # Blob name is a SHA-256 hash of the instance's terminal
        # timestamp + instance ID (matches the .NET
        # ``ExportInstanceHistoryActivity`` scheme).  This means:
        # • Two exports of the *same* completion produce the same
        #   blob name (idempotent under retry when ``overwrite=True``).
        # • An instance re-exported after a later completion lands
        #   at a new path rather than overwriting the previous one.
        # • Instance IDs that differ only by ``/`` no longer collide
        #   under the old ``.replace("/", "_")`` transform.
        blob_name = _blob_name_for(
            instance_id=instance_id,
            last_updated_at=state.last_updated_at,
            prefix=prefix,
            fmt=fmt,
        )
        ctx.writer.write(
            instance_id=instance_id,
            container=container,
            blob_name=blob_name,
            payload=payload,
            content_type=content_type_for(fmt),
            content_encoding=content_encoding_for(fmt),
            # Standard hook downstream consumers use to scan a
            # container without parsing each blob body.  Matches the
            # .NET writer's ``Metadata["instanceId"]`` convention.
            metadata={"instance_id": instance_id},
        )
    except Exception as ex:  # noqa: BLE001 - reported back via return value
        return {
            "instance_id": instance_id,
            "success": False,
            "error": f"{type(ex).__name__}: {ex}",
        }

    return {"instance_id": instance_id, "success": True, "error": None}


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _blob_name_for(
    *,
    instance_id: str,
    last_updated_at: datetime,
    prefix: str | None,
    fmt: ExportFormat,
) -> str:
    """Return the destination blob name for one exported instance.

    Matches the .NET ``ExportInstanceHistoryActivity.GenerateBlobFileName``
    scheme: lowercase-hex SHA-256 of
    ``f"{last_updated_at:O}|{instance_id}"`` with the format-appropriate
    extension appended, optionally namespaced under the configured
    destination prefix.  Hash byte-equivalence with .NET output
    requires matching the .NET ``DateTimeOffset.ToString("O")`` format
    exactly (see :func:`_dotnet_o_format`).
    """
    timestamp_str = _dotnet_o_format(last_updated_at)
    hash_input = f"{timestamp_str}|{instance_id}"
    digest = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()
    ext = file_extension_for(fmt)
    blob_name = f"{digest}{ext}"
    if prefix:
        return f"{prefix.rstrip('/')}/{blob_name}"
    return blob_name


def _dotnet_o_format(dt: datetime) -> str:
    """Format *dt* to match .NET ``DateTimeOffset.ToString("O")``.

    .NET's round-trip format is ``yyyy-MM-ddTHH:mm:ss.fffffffK`` for
    ``DateTimeOffset``, where ``K`` resolves to ``+HH:MM`` / ``-HH:MM``
    and fractional seconds always render with seven digits (100-ns
    ticks resolution).  Python :class:`datetime.datetime` only carries
    microsecond precision (six digits), so the seventh digit is always
    a trailing zero.  Naive datetimes are assumed UTC.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    base = dt.strftime("%Y-%m-%dT%H:%M:%S")
    fractional = f"{dt.microsecond:06d}0"
    offset = dt.utcoffset()
    if offset is None:
        offset_str = "+00:00"
    else:
        total_minutes = int(offset.total_seconds() // 60)
        sign = "+" if total_minutes >= 0 else "-"
        total_minutes = abs(total_minutes)
        offset_str = f"{sign}{total_minutes // 60:02d}:{total_minutes % 60:02d}"
    return f"{base}.{fractional}{offset_str}"


def register(worker_instance: worker_module.TaskHubGrpcWorker) -> None:
    """Convenience helper to register both activities on *worker*."""
    worker_instance.add_activity(list_terminal_instances)
    worker_instance.add_activity(export_instance_history)


# Used by the orchestrator to build a fresh activity input from the
# resolved job configuration without leaking model objects.
def build_list_activity_input(
    *,
    runtime_status_names: list[str] | None,
    completed_time_from: datetime,
    completed_time_to: datetime | None,
    page_size: int,
    continuation_token: str | None,
) -> dict[str, Any]:
    return {
        "runtime_status": runtime_status_names,
        "completed_time_from": completed_time_from.isoformat(),
        "completed_time_to": completed_time_to.isoformat() if completed_time_to else None,
        "page_size": page_size,
        "continuation_token": continuation_token,
    }
