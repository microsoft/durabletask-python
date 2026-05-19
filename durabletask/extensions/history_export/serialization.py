# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Serialization helpers for exported orchestration history.

Two formats are supported:

* ``JSON`` — a single self-describing JSON document per instance,
  containing the schema version, instance ID, and the full ordered
  list of events.

* ``JSONL_GZIP`` — gzip-compressed newline-delimited JSON.  The first
  line is an envelope object with metadata (``schema_version`` and
  ``instance_id``) and the remaining lines are individual events.
  This format is appropriate for very long histories where streaming
  ingestion or seeking by event is desirable.

Output is deterministic: given the same list of input events, the
serialized bytes are byte-for-byte identical across calls.  This is
achieved by sorting JSON object keys and using a fixed (non-localized)
representation of timestamps and other primitives.
"""

from __future__ import annotations

import gzip
import json
from typing import Any, Iterable, Mapping, Optional, Sequence

from durabletask import client as client_module
from durabletask import history
from durabletask import task

from durabletask.extensions.history_export.models import (
    ExportFormat,
    ExportFormatKind,
    _dt_to_iso,
)


def event_to_dict(event: history.HistoryEvent) -> dict:
    """Convert a :class:`history.HistoryEvent` into a JSON-safe dict.

    A discriminator field ``event_type`` is added so downstream
    consumers can distinguish event subclasses without inspecting
    their fields.
    """
    payload = event.to_dict()
    # Insert the discriminator first so the resulting dict orders it
    # near the front of the JSON object even before any sorting.
    return {"event_type": type(event).__name__, **payload}


def orchestration_state_to_dict(
    state: client_module.OrchestrationState,
) -> dict[str, Any]:
    """Convert an :class:`OrchestrationState` into a JSON-safe dict.

    All fields are mapped to literal-named primitives.  No Python
    class names or module paths appear in the resulting dict.
    """
    failure = state.failure_details
    failure_dict: Optional[dict[str, Any]] = None
    if failure is not None:
        failure_dict = {
            "message": failure.message,
            "error_type": failure.error_type,
            "stack_trace": failure.stack_trace,
        }
        inner = getattr(failure, "inner_failure", None)
        if isinstance(inner, task.FailureDetails):
            failure_dict["inner_failure"] = {
                "message": inner.message,
                "error_type": inner.error_type,
                "stack_trace": inner.stack_trace,
            }
    return {
        "instance_id": state.instance_id,
        "name": state.name,
        "runtime_status": state.runtime_status.name,
        "created_at": _dt_to_iso(state.created_at),
        "last_updated_at": _dt_to_iso(state.last_updated_at),
        "serialized_input": state.serialized_input,
        "serialized_output": state.serialized_output,
        "serialized_custom_status": state.serialized_custom_status,
        "failure_details": failure_dict,
    }


def _dump_json(value) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def serialize_history(
    events: Sequence[history.HistoryEvent],
    *,
    instance_id: str,
    fmt: ExportFormat,
    metadata: Optional[Mapping[str, Any]] = None,
) -> bytes:
    """Serialize a list of history events for a single instance.

    Args:
        events: The ordered list of events to serialize.
        instance_id: The orchestration instance the events belong to.
        fmt: The output format (kind + schema version).
        metadata: Optional dict produced by
            :func:`orchestration_state_to_dict` that will be embedded
            in the serialized output (top-level ``metadata`` field for
            JSON; embedded in the first JSONL line for JSONL).

    Returns:
        The serialized bytes, ready to be written to the destination.
    """
    if fmt.kind is ExportFormatKind.JSON:
        document: dict[str, Any] = {
            "schema_version": fmt.schema_version,
            "instance_id": instance_id,
            "events": [event_to_dict(e) for e in events],
        }
        if metadata is not None:
            document["metadata"] = dict(metadata)
        return _dump_json(document).encode("utf-8")

    if fmt.kind is ExportFormatKind.JSONL_GZIP:
        return _gzip_jsonl(events, instance_id=instance_id, fmt=fmt, metadata=metadata)

    raise ValueError(f"Unsupported export format kind: {fmt.kind!r}")


def _gzip_jsonl(
    events: Iterable[history.HistoryEvent],
    *,
    instance_id: str,
    fmt: ExportFormat,
    metadata: Optional[Mapping[str, Any]] = None,
) -> bytes:
    # Build the uncompressed JSONL document first so the test surface
    # can decode the bytes deterministically.
    header: dict[str, Any] = {
        "schema_version": fmt.schema_version,
        "instance_id": instance_id,
        "kind": "metadata",
    }
    if metadata is not None:
        header["metadata"] = dict(metadata)
    lines = [_dump_json(header)]
    lines.extend(_dump_json(event_to_dict(e)) for e in events)
    raw = ("\n".join(lines) + "\n").encode("utf-8")
    # mtime=0 keeps the gzip header deterministic across runs.
    return gzip.compress(raw, mtime=0)


def content_type_for(fmt: ExportFormat) -> str:
    """Return the appropriate HTTP-style content type for *fmt*."""
    if fmt.kind is ExportFormatKind.JSON:
        return "application/json"
    if fmt.kind is ExportFormatKind.JSONL_GZIP:
        return "application/x-ndjson"
    raise ValueError(f"Unsupported export format kind: {fmt.kind!r}")


def content_encoding_for(fmt: ExportFormat) -> str | None:
    """Return the appropriate ``Content-Encoding`` for *fmt*, if any."""
    if fmt.kind is ExportFormatKind.JSONL_GZIP:
        return "gzip"
    return None


def file_extension_for(fmt: ExportFormat) -> str:
    """Return the file extension to append to exported blobs."""
    if fmt.kind is ExportFormatKind.JSON:
        return ".json"
    if fmt.kind is ExportFormatKind.JSONL_GZIP:
        return ".jsonl.gz"
    raise ValueError(f"Unsupported export format kind: {fmt.kind!r}")
