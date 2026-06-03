# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Public client API for the history export extension.

The :class:`ExportHistoryClient` wraps a :class:`TaskHubGrpcClient`
and a
:class:`~durabletask.extensions.history_export.writer.HistoryWriter`
to expose a small, typed surface for creating, inspecting, listing,
and deleting export jobs.  Most callers will pair it with the
per-job :class:`ExportHistoryJobClient` returned by
:meth:`ExportHistoryClient.get_job_client`.

Typical usage::

    from durabletask import client, worker
    from durabletask.extensions.history_export import (
        ExportDestination,
        ExportFormat,
        ExportFormatKind,
        ExportHistoryClient,
        ExportJobCreationOptions,
        ExportMode,
    )
    from durabletask.extensions.history_export.azure_blob import (
        AzureBlobHistoryExportWriter,
        AzureBlobHistoryExportWriterOptions,
    )

    writer = AzureBlobHistoryExportWriter(
        AzureBlobHistoryExportWriterOptions(
            container_name="exports",
            connection_string="UseDevelopmentStorage=true",
        )
    )
    dt_client = client.TaskHubGrpcClient(host_address="localhost:4001")
    export_client = ExportHistoryClient(dt_client, writer)

    with worker.TaskHubGrpcWorker(host_address="localhost:4001") as w:
        export_client.register_worker(w)
        w.start()

        desc = export_client.create_job(ExportJobCreationOptions(
            mode=ExportMode.BATCH,
            completed_time_from=datetime(2026, 1, 1, tzinfo=timezone.utc),
            completed_time_to=datetime(2026, 2, 1, tzinfo=timezone.utc),
            destination=ExportDestination(container="exports", prefix="january"),
        ))
        job_client = export_client.get_job_client(desc.job_id)
        final = job_client.wait(timeout=300)
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, cast

from durabletask import client as client_module
from durabletask import entities
from durabletask import worker as worker_module

from durabletask.extensions.history_export._constants import (
    ENTITY_NAME,
    ORCHESTRATOR_NAME,
    orchestrator_instance_id_for,
)
from durabletask.extensions.history_export._logging import logger
from durabletask.extensions.history_export.activities import (
    HistoryExportContext,
    bind_context,
    register as _register_activities,
)
from durabletask.extensions.history_export.writer import HistoryWriter
from durabletask.extensions.history_export.entity import ExportJobEntity
from durabletask.extensions.history_export.exceptions import (
    ExportJobNotFoundError,
)
from durabletask.extensions.history_export.models import (
    ExportJobCreationOptions,
    ExportJobDescription,
    ExportJobQuery,
    ExportJobStatus,
)
from durabletask.extensions.history_export.orchestrator import (
    export_job_orchestrator,
)


_TERMINAL_STATUSES = frozenset({ExportJobStatus.COMPLETED, ExportJobStatus.FAILED})
_ENTITY_ID_PREFIX = f"@{ENTITY_NAME.lower()}@"


__all__ = ["ExportHistoryClient", "ExportHistoryJobClient"]


class ExportHistoryClient:
    """Public façade for creating and inspecting export jobs."""

    def __init__(
        self,
        durable_task_client: client_module.TaskHubGrpcClient,
        writer: HistoryWriter,
    ) -> None:
        self._client = durable_task_client
        self._writer = writer

    # ------------------------------------------------------------------
    # Worker wiring
    # ------------------------------------------------------------------

    def register_worker(self, worker_instance: worker_module.TaskHubGrpcWorker) -> None:
        """Register the entity, activities, and orchestrator on *worker*.

        Also binds the activity execution context so the activities
        can find the underlying client and writer at runtime.  Call
        this once per worker before :meth:`start`.
        """
        worker_instance.add_entity(ExportJobEntity, name=ENTITY_NAME)
        _register_activities(worker_instance)
        worker_instance.add_orchestrator(export_job_orchestrator)
        bind_context(HistoryExportContext(client=self._client, writer=self._writer))

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    def create_job(
        self,
        options: ExportJobCreationOptions,
        *,
        job_id: str | None = None,
    ) -> ExportJobDescription:
        """Create a new export job and start its driving orchestrator.

        The entity is created in :attr:`ExportJobStatus.PENDING` and
        immediately signalled with ``run``, which schedules the
        driving orchestrator from inside the entity using a
        deterministic instance ID (``export-job-{job_id}``).  This
        matches the .NET ``ExportJob.Run`` pattern: callers can
        correlate a job with its orchestrator by ID alone and may
        safely re-create a previously-terminated job.
        """
        config = options.to_configuration()
        resolved_job_id = job_id or uuid.uuid4().hex
        entity_id = entities.EntityInstanceId(ENTITY_NAME, resolved_job_id)
        created_at = datetime.now(timezone.utc)
        config_dict = config.to_dict()

        # Signal create first; the entity will validate the transition
        # and persist PENDING.  Then signal run; the entity will
        # schedule the orchestrator and transition to ACTIVE.  Both
        # signals are processed in FIFO order by the entity dispatcher.
        self._client.signal_entity(
            entity_id,
            ExportJobEntity.OP_CREATE,
            input={
                "config": config_dict,
                "created_at": created_at.isoformat(),
            },
        )
        self._client.signal_entity(entity_id, ExportJobEntity.OP_RUN)
        logger.info(
            "Submitted export job %r; orchestrator instance ID will be %s",
            resolved_job_id, orchestrator_instance_id_for(resolved_job_id),
        )
        return ExportJobDescription(
            job_id=resolved_job_id,
            status=ExportJobStatus.PENDING,
            created_at=created_at,
            last_modified_at=created_at,
            config=config,
            orchestrator_instance_id=orchestrator_instance_id_for(resolved_job_id),
            scanned_instances=0,
            exported_instances=0,
            failed_instances=0,
            last_error=None,
            checkpoint=None,
            last_checkpoint_time=None,
        )

    def get_job(self, job_id: str) -> ExportJobDescription | None:
        """Look up an export job by ID.  Returns ``None`` if not found.

        Note that the lookup-miss contract differs from
        :meth:`wait_for_job`: ``get_job`` is a passive read that
        returns ``None`` when the entity does not exist, while
        ``wait_for_job`` raises :class:`ExportJobNotFoundError` after
        its timeout if the entity never appears.
        """
        entity_id = entities.EntityInstanceId(ENTITY_NAME, job_id)
        meta = self._client.get_entity(entity_id, include_state=True)
        if meta is None:
            return None
        raw = meta.get_state(str)
        if not raw:
            return None
        try:
            state = json.loads(raw)
        except (TypeError, ValueError):
            return None
        if not isinstance(state, dict):
            return None
        return ExportJobDescription.from_state_dict(
            job_id, cast("dict[str, Any]", state),
        )

    def list_jobs(
        self,
        query: ExportJobQuery | None = None,
    ) -> Iterator[ExportJobDescription]:
        """Enumerate export jobs.

        Filters from *query* (status, last-modified window) are
        applied client-side after fetching pages from the backend.
        Yields one :class:`ExportJobDescription` per matching job.
        """
        if query is None:
            query = ExportJobQuery()

        entity_query = client_module.EntityQuery(
            instance_id_starts_with=_ENTITY_ID_PREFIX,
            last_modified_from=query.last_modified_from,
            last_modified_to=query.last_modified_to,
            # list_jobs always needs the persisted state to populate
            # ExportJobDescription; an entity-only view doesn't carry
            # status or progress and would always be filtered out.
            include_state=True,
            page_size=query.page_size,
        )
        status_filter = set(query.status) if query.status else None

        for meta in self._client.get_all_entities(entity_query):
            # The query may catch unrelated entities if some other
            # extension picks the same prefix; guard with an
            # explicit entity-name check.
            if meta.id.entity != ENTITY_NAME.lower():
                continue
            raw = meta.get_state(str)
            if not raw:
                logger.warning(
                    "list_jobs: skipping export-job entity %r with no "
                    "persisted state", meta.id.key,
                )
                continue
            try:
                state = json.loads(raw)
            except (TypeError, ValueError) as ex:
                logger.warning(
                    "list_jobs: skipping export-job entity %r; failed to "
                    "parse state JSON (%s)", meta.id.key, ex,
                )
                continue
            if not isinstance(state, dict):
                logger.warning(
                    "list_jobs: skipping export-job entity %r; persisted "
                    "state is not a JSON object (got %s)",
                    meta.id.key, type(state).__name__,
                )
                continue
            try:
                desc = ExportJobDescription.from_state_dict(
                    meta.id.key, cast("dict[str, Any]", state),
                )
            except (KeyError, ValueError) as ex:
                logger.warning(
                    "list_jobs: skipping export-job entity %r; state did "
                    "not match the current schema (%s)", meta.id.key, ex,
                )
                continue
            if status_filter is not None and desc.status not in status_filter:
                continue
            yield desc

    def wait_for_job(
        self,
        job_id: str,
        *,
        timeout: float = 300.0,
        poll_interval: float = 1.0,
    ) -> ExportJobDescription:
        """Poll until the job reaches a terminal status or *timeout* elapses.

        Raises:
            TimeoutError: If the job is still pending/active after
                *timeout* seconds.
            ExportJobNotFoundError: If the job cannot be found at all.
        """
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")

        deadline = time.monotonic() + timeout
        last: ExportJobDescription | None = None
        while True:
            desc = self.get_job(job_id)
            if desc is not None:
                last = desc
                if desc.status in _TERMINAL_STATUSES:
                    return desc
            if time.monotonic() >= deadline:
                if last is None:
                    raise ExportJobNotFoundError(job_id)
                raise TimeoutError(
                    f"Export job '{job_id}' did not reach a terminal status "
                    f"within {timeout}s (last status: {last.status.value})"
                )
            time.sleep(poll_interval)

    def delete_job(self, job_id: str) -> None:
        """Request deletion of the export-job entity, clearing its state.

        This call is **best-effort and fire-and-forget**: it enqueues a
        ``delete`` signal on the entity but does not wait for the
        entity dispatcher to process it.  Callers that need
        confirmation should poll :meth:`get_job` and wait for it to
        return ``None``.

        The driving orchestrator will detect the deletion at its next
        loop iteration (via :meth:`OrchestrationContext.call_entity`)
        and exit cleanly without issuing further signals.

        This does NOT delete blobs already written to the destination.
        """
        entity_id = entities.EntityInstanceId(ENTITY_NAME, job_id)
        self._client.signal_entity(entity_id, ExportJobEntity.OP_DELETE)

    def cancel_job(self, job_id: str) -> None:
        """Alias for :meth:`delete_job`.

        ``CONTINUOUS`` mode has no natural completion, so users
        looking to stop a tailing export are likely to look for
        ``cancel_job`` rather than ``delete_job``.  Provided as a thin
        alias to make either name discoverable.
        """
        self.delete_job(job_id)

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def get_job_client(self, job_id: str) -> "ExportHistoryJobClient":
        """Return a per-job façade for *job_id*."""
        return ExportHistoryJobClient(self, job_id)

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    @property
    def entity_name(self) -> str:
        return ENTITY_NAME

    @property
    def orchestrator_name(self) -> str:
        return ORCHESTRATOR_NAME

    @property
    def writer(self) -> HistoryWriter:
        return self._writer

    @property
    def underlying_client(self) -> client_module.TaskHubGrpcClient:
        return self._client


class ExportHistoryJobClient:
    """Per-job convenience façade returned by :meth:`ExportHistoryClient.get_job_client`.

    All methods are thin pass-throughs to the parent client; the
    class exists so callers can pass around a single object that
    encapsulates a job ID rather than re-typing it at every call
    site.
    """

    def __init__(self, parent: ExportHistoryClient, job_id: str) -> None:
        if not job_id:
            raise ValueError("job_id must be a non-empty string")
        self._parent = parent
        self._job_id = job_id

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def orchestrator_instance_id(self) -> str:
        return orchestrator_instance_id_for(self._job_id)

    def describe(self) -> ExportJobDescription | None:
        """Fetch the latest description, or ``None`` if the job is missing."""
        return self._parent.get_job(self._job_id)

    def wait(
        self,
        *,
        timeout: float = 300.0,
        poll_interval: float = 1.0,
    ) -> ExportJobDescription:
        """Poll until terminal; see :meth:`ExportHistoryClient.wait_for_job`."""
        return self._parent.wait_for_job(
            self._job_id, timeout=timeout, poll_interval=poll_interval,
        )

    def delete(self) -> None:
        """Delete the export job; see :meth:`ExportHistoryClient.delete_job`."""
        self._parent.delete_job(self._job_id)
