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

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
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
    """Install the runtime dependencies for the history-export activities."""
    global _context
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
    prefix_raw: Any = destination_raw.get("prefix")
    prefix: str | None = str(prefix_raw) if prefix_raw is not None else None

    try:
        events = ctx.client.get_orchestration_history(instance_id)
        # Fetch the orchestration's terminal metadata too so the
        # exported blob is self-describing (matches the .NET behavior).
        state = ctx.client.get_orchestration_state(
            instance_id, fetch_payloads=True,
        )
        metadata = orchestration_state_to_dict(state) if state is not None else None
        payload = serialize_history(
            events,
            instance_id=instance_id,
            fmt=fmt,
            metadata=metadata,
        )
        blob_name = _blob_name_for(instance_id=instance_id, prefix=prefix, fmt=fmt)
        ctx.writer.write(
            instance_id=instance_id,
            blob_name=blob_name,
            payload=payload,
            content_type=content_type_for(fmt),
            content_encoding=content_encoding_for(fmt),
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

def _blob_name_for(*, instance_id: str, prefix: str | None, fmt: ExportFormat) -> str:
    ext = file_extension_for(fmt)
    safe_id = instance_id.replace("/", "_")
    if prefix:
        return f"{prefix.rstrip('/')}/{safe_id}{ext}"
    return f"{safe_id}{ext}"


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
