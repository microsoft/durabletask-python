# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Compatibility override for the history-export enumeration activity.

The core durabletask ``list_terminal_instances`` activity enumerates terminal
instances via the ``ListInstanceIds`` gRPC call. The Azure Functions Durable
extension's gRPC endpoint does not implement that method (it returns
``UNIMPLEMENTED``), so this module provides a drop-in replacement that
enumerates via ``QueryInstances`` (:meth:`get_all_orchestration_states`)
instead -- a method the extension does implement.

> [!NOTE]
> This shim exists only because the Durable Functions host extension does not
> yet implement ``ListInstanceIds``. Once it does, delete this module and have
> :meth:`DFApp.configure_history_export` register the core
> ``durabletask.extensions.history_export.activities.list_terminal_instances``
> activity directly.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Optional, cast

from durabletask import task
from durabletask.client import (
    OrchestrationQuery,
    OrchestrationStatus,
    TaskHubGrpcClient,
)
from durabletask.internal.helpers import ensure_aware

from durabletask.extensions.history_export._internal import dt_from_iso
from durabletask.extensions.history_export.activities import (
    EXPORT_INSTANCE_HISTORY_ACTIVITY,
    HistoryExportContext,
    _require_context,  # pyright: ignore[reportPrivateUsage]
    bind_context,
    export_instance_history,
)
from durabletask.extensions.history_export.entity import ExportJobEntity
from durabletask.extensions.history_export.models import (
    ExportJobConfiguration,
    ExportJobState,
    ExportJobStatus,
    ExportMode,
)
from durabletask.extensions.history_export.writer import HistoryWriter

from .azurefunctions_grpc_interceptor import (
    AzureFunctionsDefaultClientInterceptorImpl,
)
from .serialization import DEFAULT_FUNCTIONS_DATA_CONVERTER

# The activity registers under the same name the export orchestrator calls, so
# it transparently replaces the core activity.
LIST_TERMINAL_INSTANCES_ACTIVITY = "list_terminal_instances"


def list_terminal_instances(
        _: task.ActivityContext, input: Mapping[str, Any]) -> dict[str, Any]:
    """Enumerate terminal instances via ``QueryInstances``, one page at a time.

    Drop-in replacement for the core ``list_terminal_instances`` activity that
    avoids the unimplemented ``ListInstanceIds`` call. ``QueryInstances`` filters
    by *created* time whereas the export filters by *completed* time, so the
    completed-time window is applied client-side against each instance's last
    update time.

    ``QueryInstances`` has no server-side completed-time cursor, so each call
    re-enumerates the matching instances and pages them client-side:
    the matches are sorted by instance ID and the ``continuation_token`` carries
    the last instance ID returned (a keyset cursor). A call returns only the
    instances whose ID sorts strictly after that cursor, sliced to ``page_size``.
    This keeps the export orchestrator's fan-out bounded to one ``page_size``
    batch at a time (matching ``max_instances_per_batch``) instead of scheduling
    an export activity for every matching instance at once.
    """
    ctx = _require_context()

    raw_statuses = input.get("runtime_status")
    runtime_status_names: Optional[list[str]] = (
        list(raw_statuses) if raw_statuses is not None else None
    )
    completed_time_from = dt_from_iso(input.get("completed_time_from"))
    completed_time_to = dt_from_iso(input.get("completed_time_to"))
    if completed_time_from is None:
        raise ValueError("list_terminal_instances requires 'completed_time_from'")

    page_size_raw = input.get("page_size")
    page_size: Optional[int] = int(page_size_raw) if page_size_raw is not None else None
    cursor_raw = input.get("continuation_token")
    cursor: Optional[str] = str(cursor_raw) if cursor_raw is not None else None

    runtime_status: Optional[list[OrchestrationStatus]] = None
    if runtime_status_names is not None:
        runtime_status = [OrchestrationStatus[name] for name in runtime_status_names]

    states = ctx.client.get_all_orchestration_states(
        OrchestrationQuery(runtime_status=runtime_status))

    instance_ids: list[str] = []
    for state in states:
        completed_at = ensure_aware(state.last_updated_at)
        if completed_at is not None:
            if completed_at < completed_time_from:
                continue
            if completed_time_to is not None and completed_at > completed_time_to:
                continue
        instance_ids.append(state.instance_id)

    # Deterministic keyset paging: sort by instance ID and return only the
    # slice strictly after the cursor, capped at ``page_size``.
    instance_ids.sort()
    if cursor is not None:
        instance_ids = [i for i in instance_ids if i > cursor]

    if page_size is not None and len(instance_ids) > page_size:
        page_ids = instance_ids[:page_size]
        next_cursor: Optional[str] = page_ids[-1]
    else:
        page_ids = instance_ids
        next_cursor = None

    return {"instance_ids": page_ids, "continuation_token": next_cursor}


# ---------------------------------------------------------------------------
# Per-invocation dependency resolution for the export activities.
#
# The export activities need a durabletask client and a ``HistoryWriter``. In
# the host-driven Functions model the client is not ambient in a worker
# process, so it is injected per-invocation via a ``durable_client_input``
# binding: the host supplies it wherever the activity runs, which is safe across
# a scaled-out, multi-worker deployment. The writer is user-supplied and not
# host-injectable, so it is registered once at app configuration time (which
# runs in every worker process on import) and reused.
# ---------------------------------------------------------------------------

_export_writer: Optional[HistoryWriter] = None
_context_bound = False


def set_export_writer(writer: HistoryWriter) -> None:
    """Register the ``HistoryWriter`` the export activities write through.

    Called from :meth:`DFApp.configure_history_export`, which runs at app import
    in every worker process, so the writer is available wherever an export
    activity is scheduled.
    """
    global _export_writer
    _export_writer = writer


def _build_sync_client(client: Any) -> TaskHubGrpcClient:
    """Build a synchronous ``TaskHubGrpcClient`` from an injected durable client.

    The ``durable_client_input`` binding yields an async ``DurableFunctionsClient``
    carrying the host's RPC endpoint and auth; the export activities use the
    synchronous client, so this bridges to one aimed at the same endpoint.
    """
    interceptors = [AzureFunctionsDefaultClientInterceptorImpl(
        client.taskHubName, client.requiredQueryStringParameters)]
    return TaskHubGrpcClient(
        host_address=client.rpcBaseUrl,
        secure_channel=False,
        interceptors=interceptors,
        data_converter=DEFAULT_FUNCTIONS_DATA_CONVERTER)


def _ensure_context_bound(client: Any) -> None:
    """Bind the export activity context once per worker process (lazily).

    Uses the per-invocation injected client to build the synchronous client and
    pairs it with the configured writer. Binding is idempotent per process: the
    endpoint and writer are stable for the app's lifetime, so the first
    invocation in each process establishes the context for the rest.
    """
    global _context_bound
    if _context_bound:
        return
    if _export_writer is None:
        raise RuntimeError(
            "history export writer is not configured; pass a writer to "
            "DFApp.configure_history_export(writer=...) at app startup")
    bind_context(HistoryExportContext(
        client=_build_sync_client(client), writer=_export_writer))
    _context_bound = True


def list_terminal_instances_client_bound(
        input: Mapping[str, Any], client: Any) -> dict[str, Any]:
    """``list_terminal_instances`` with the client injected per-invocation."""
    _ensure_context_bound(client)
    return list_terminal_instances(task.ActivityContext("", 0), input)


def export_instance_history_client_bound(
        input: Mapping[str, Any], client: Any) -> dict[str, Any]:
    """``export_instance_history`` with the client injected per-invocation."""
    _ensure_context_bound(client)
    return export_instance_history(task.ActivityContext("", 0), input)


# The host indexer rejects parameterized generics on trigger parameters and
# requires the registered function name to match the durable activity name the
# export orchestrator calls. Set both explicitly (the ``durable_client_input``
# decorator adds the ``client`` annotation when it is applied).
list_terminal_instances_client_bound.__name__ = LIST_TERMINAL_INSTANCES_ACTIVITY
list_terminal_instances_client_bound.__annotations__ = {"input": dict, "return": dict}
export_instance_history_client_bound.__name__ = EXPORT_INSTANCE_HISTORY_ACTIVITY
export_instance_history_client_bound.__annotations__ = {"input": dict, "return": dict}


# ---------------------------------------------------------------------------
# Continuous-mode guard (enforced at the entity boundary).
#
# The QueryInstances-based enumeration pages a fixed completed-time window and
# cannot safely tail a growing set, so continuous export is unsupported on
# Functions. Rejecting it only in a client wrapper is bypassable (the core
# ``ExportHistoryClient`` signals the same entity), so the guard lives on the
# entity that every job-creation path must signal.
# ---------------------------------------------------------------------------

_CONTINUOUS_UNSUPPORTED_MESSAGE = (
    "Continuous history export (ExportMode.CONTINUOUS) is not supported on "
    "Azure Functions: the enumeration path pages a fixed completed-time window "
    "via QueryInstances and cannot safely tail a growing set, so continuous "
    "jobs would miss terminal histories. Use ExportMode.BATCH with a bounded "
    "completed-time window."
)


def _created_at_from(payload: Mapping[str, Any]) -> datetime:
    raw = payload.get("created_at")
    parsed = dt_from_iso(raw) if raw else None
    return parsed or datetime.now(timezone.utc)


class FunctionsExportJobEntity(ExportJobEntity):
    """Export-job entity for Azure Functions that rejects ``CONTINUOUS`` mode.

    ``configure_history_export`` registers this in place of the core
    ``ExportJobEntity`` so continuous export is refused at the entity boundary --
    the single choke point every job-creation path signals -- rather than only
    in an optional client wrapper. A continuous ``create`` is marked ``FAILED``
    with a clear reason (and the driving orchestrator is not scheduled) instead
    of starting a job whose enumeration would silently miss histories.
    """

    def create(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        config_dict = payload.get("config")
        if isinstance(config_dict, Mapping) and config_dict:
            config = ExportJobConfiguration.from_dict(
                cast("Mapping[str, Any]", config_dict))
            if config.mode is ExportMode.CONTINUOUS:
                created_at = _created_at_from(payload)
                state = ExportJobState(
                    status=ExportJobStatus.FAILED,
                    config=config,
                    created_at=created_at,
                    last_modified_at=created_at,
                    last_error=_CONTINUOUS_UNSUPPORTED_MESSAGE,
                )
                return self._save(state)
        return super().create(payload)
