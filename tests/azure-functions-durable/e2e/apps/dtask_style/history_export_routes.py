# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""HTTP routes for the history-export extension (opt-in).

Exercises ``durabletask.extensions.history_export`` end-to-end through the
Functions host: the export-job entity, driving orchestrator, and the two
activities (registered via ``app.configure_history_export()``), plus the
public ``ExportHistoryClient`` surface (create / get job).

The export activities need a durabletask client and a ``HistoryWriter`` bound
into the worker process via ``bind_context``. The client is only available at
request time (from the durable-client binding), so the context is bound here on
each request -- the route handler shares the worker process with the activities,
so the binding is visible to them. A local-filesystem writer sends exported
history to ``<app>/_export_output`` where the test can read it back.
"""

import json
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import azure.functions as func

import azure.durable_functions as df
from durabletask.client import TaskHubGrpcClient
from azure.durable_functions.extensions.history_export import (
    ExportDestination,
    ExportFormat,
    ExportFormatKind,
    ExportHistoryClient,
    ExportJobCreationOptions,
    ExportMode,
)
from azure.durable_functions.internal.azurefunctions_grpc_interceptor import (
    AzureFunctionsDefaultClientInterceptorImpl,
)
from azure.durable_functions.internal.serialization import (
    DEFAULT_FUNCTIONS_DATA_CONVERTER,
)

bp = df.Blueprint()

# Exported history is written under the app directory so the E2E test (running
# on the same machine) can read it back and assert on the contents.
EXPORT_ROOT = Path(__file__).parent / "_export_output"


class _FileSystemHistoryWriter:
    """Minimal ``HistoryWriter`` that writes each blob to the local filesystem."""

    def __init__(self, root: Path) -> None:
        self._root = Path(root)

    def write(
            self,
            *,
            instance_id: str,
            container: str,
            blob_name: str,
            payload: bytes,
            content_type: str,
            content_encoding: Optional[str] = None,
            metadata: Optional[Mapping[str, str]] = None) -> None:
        path = self._root / container / blob_name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)


# Shared writer instance: registered with the export activities via
# ``configure_history_export`` (in ``function_app.py``) and reused by the route's
# ``ExportHistoryClient``. Constructing it at import makes it available in every
# worker process.
EXPORT_WRITER = _FileSystemHistoryWriter(EXPORT_ROOT)


def _sync_client(client: df.DurableFunctionsClient) -> TaskHubGrpcClient:
    """Build a synchronous durabletask client aimed at the same sidecar.

    ``ExportHistoryClient`` (and the export activities) use the synchronous
    ``TaskHubGrpcClient``, whereas ``DurableFunctionsClient`` is async.
    """
    interceptors = [AzureFunctionsDefaultClientInterceptorImpl(
        client.taskHubName, client.requiredQueryStringParameters)]
    return TaskHubGrpcClient(
        host_address=client.rpcBaseUrl,
        secure_channel=False,
        interceptors=interceptors,
        data_converter=DEFAULT_FUNCTIONS_DATA_CONVERTER)


def _export_client(client: df.DurableFunctionsClient) -> ExportHistoryClient:
    """Build an ExportHistoryClient for job management.

    The export activities resolve their own client per-invocation from a durable
    client binding, so the route no longer binds a process-wide context; it only
    needs a client for the job-management surface (create / get job).
    """
    return ExportHistoryClient(_sync_client(client), EXPORT_WRITER)


@bp.route(route="export/start", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def start_export(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    body = req.get_json() or {}
    export = _export_client(client)
    now = datetime.now(timezone.utc)
    # Callers can narrow the completed-time window so an export only covers the
    # instances they just produced (rather than every terminal instance in the
    # last hour, which would make the fan-out unbounded).
    completed_from_raw = body.get("completed_from")
    completed_from = (
        datetime.fromisoformat(completed_from_raw)
        if completed_from_raw else now - timedelta(hours=1))
    options = ExportJobCreationOptions(
        mode=ExportMode.BATCH,
        completed_time_from=completed_from,
        completed_time_to=now + timedelta(hours=1),
        destination=ExportDestination(container=body.get("container", "exports")),
        # Uncompressed single-JSON-document-per-instance so the test can read it.
        format=ExportFormat(kind=ExportFormatKind.JSON),
    )
    desc = export.create_job(options, job_id=body.get("job_id"))
    return func.HttpResponse(
        json.dumps({
            "jobId": desc.job_id,
            "status": desc.status.value,
            "exportRoot": str(EXPORT_ROOT),
        }),
        mimetype="application/json")


@bp.route(route="export/status/{job_id}", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def export_status(
        req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
    export = _export_client(client)
    desc = export.get_job(req.route_params["job_id"])
    if desc is None:
        return func.HttpResponse(
            json.dumps({"status": None}), mimetype="application/json")
    payload: dict[str, Any] = {
        "jobId": desc.job_id,
        "status": desc.status.value,
        "scanned": desc.scanned_instances,
        "exported": desc.exported_instances,
        "failed": desc.failed_instances,
        "lastError": desc.last_error,
    }
    return func.HttpResponse(json.dumps(payload), mimetype="application/json")
