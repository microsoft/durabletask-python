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
from typing import Any, Optional

from durabletask import task
from durabletask.client import OrchestrationQuery, OrchestrationStatus
from durabletask.internal.helpers import ensure_aware

from durabletask.extensions.history_export._internal import dt_from_iso
from durabletask.extensions.history_export.activities import _require_context  # pyright: ignore[reportPrivateUsage]

# The activity registers under the same name the export orchestrator calls, so
# it transparently replaces the core activity.
LIST_TERMINAL_INSTANCES_ACTIVITY = "list_terminal_instances"


def list_terminal_instances(
        _: task.ActivityContext, input: Mapping[str, Any]) -> dict[str, Any]:
    """Enumerate terminal instances via ``QueryInstances``.

    Drop-in replacement for the core ``list_terminal_instances`` activity that
    avoids the unimplemented ``ListInstanceIds`` call. ``QueryInstances`` filters
    by *created* time whereas the export filters by *completed* time, so the
    completed-time window is applied client-side against each instance's last
    update time. Every match is returned in a single page
    (``continuation_token`` is always ``None``) because
    ``get_all_orchestration_states`` paginates internally.
    """
    ctx = _require_context()

    # A continuation token means the first call already returned everything;
    # there is no second page under this enumeration strategy.
    if input.get("continuation_token"):
        return {"instance_ids": [], "continuation_token": None}

    raw_statuses = input.get("runtime_status")
    runtime_status_names: Optional[list[str]] = (
        list(raw_statuses) if raw_statuses is not None else None
    )
    completed_time_from = dt_from_iso(input.get("completed_time_from"))
    completed_time_to = dt_from_iso(input.get("completed_time_to"))
    if completed_time_from is None:
        raise ValueError("list_terminal_instances requires 'completed_time_from'")

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

    return {"instance_ids": instance_ids, "continuation_token": None}
