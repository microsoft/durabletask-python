# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import random
import threading

from typing import Iterator, Optional

from azure.identity import ManagedIdentityCredential

from durabletask.azuremanaged.preview.on_demand_sandbox.helpers import _resolve_activity_names
from durabletask.azuremanaged.preview.on_demand_sandbox.declarations import (
    DEFAULT_MAX_CONCURRENT_ACTIVITIES,
    DEFAULT_WORKER_PROFILE_ID,
    _build_on_demand_sandbox_worker_heartbeat,
    _build_on_demand_sandbox_worker_start,
)
from durabletask.azuremanaged.preview.on_demand_sandbox.transport import (
    OnDemandSandboxActivitiesGrpcTransport,
)
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
import durabletask.internal.shared as shared
from durabletask.worker import (
    ActivityWorkItemFilter,
    ConcurrencyOptions,
    WorkItemFilters,
)


class OnDemandSandboxWorker(DurableTaskSchedulerWorker):
    """Durable Task Scheduler worker mode for activity containers started by on-demand sandbox activities.

    This worker registers a live worker session with Durable Task Scheduler and
    restricts dispatch to the activities registered on this worker.
    """

    def __init__(self):
        resolved_host_address = _resolve_host_address()
        resolved_taskhub = _resolve_taskhub()
        resolved_secure_channel = _resolve_secure_channel(resolved_host_address)
        resolved_token_credential = _resolve_token_credential()
        resolved_max_concurrent_activities = _resolve_max_concurrent_activities()
        concurrency_options = ConcurrencyOptions(
            maximum_concurrent_activity_work_items=resolved_max_concurrent_activities)

        self._on_demand_sandbox_host_address = resolved_host_address
        self._on_demand_sandbox_secure_channel = resolved_secure_channel
        self._on_demand_sandbox_token_credential = resolved_token_credential
        self._on_demand_sandbox_logger = shared.get_logger("worker")

        super().__init__(
            host_address=resolved_host_address,
            taskhub=resolved_taskhub,
            token_credential=resolved_token_credential,
            secure_channel=resolved_secure_channel,
            concurrency_options=concurrency_options)

        self._on_demand_sandbox_taskhub = resolved_taskhub
        self._on_demand_sandbox_worker_profile_id = _resolve_worker_profile_id()
        self._on_demand_sandbox_activity_names: list[str] = []
        self._on_demand_sandbox_max_activities = resolved_max_concurrent_activities
        self._on_demand_sandbox_substrate = os.getenv("DTS_SUBSTRATE")
        self._on_demand_sandbox_dts_sandbox_identifier = os.getenv("DTS_SANDBOX_ID")
        self._on_demand_sandbox_heartbeat_interval_seconds = 2.0
        self._on_demand_sandbox_registration_stop = threading.Event()
        self._on_demand_sandbox_registration_thread: Optional[threading.Thread] = None
        self._on_demand_sandbox_active_activities = 0
        self._on_demand_sandbox_active_activities_lock = threading.Lock()

    def add_activity(self, fn) -> str:
        activity_name = super().add_activity(fn)
        self._on_demand_sandbox_activity_names.append(activity_name)
        return activity_name

    def start(self) -> None:
        self._configure_on_demand_sandbox_activity_filters()
        super().start()
        self._start_on_demand_sandbox_registration()

    def stop(self) -> None:
        self._stop_on_demand_sandbox_registration()
        super().stop()

    def _durabletask_on_activity_execution_started(self, req) -> None:
        with self._on_demand_sandbox_active_activities_lock:
            self._on_demand_sandbox_active_activities += 1

    def _durabletask_on_activity_execution_completed(self, req) -> None:
        with self._on_demand_sandbox_active_activities_lock:
            self._on_demand_sandbox_active_activities = max(0, self._on_demand_sandbox_active_activities - 1)

    def _configure_on_demand_sandbox_activity_filters(self) -> None:
        activity_names = _resolve_activity_names(self._on_demand_sandbox_activity_names)
        if not activity_names:
            raise RuntimeError(
                "On-demand sandbox worker requires at least one registered activity before it can register.")

        self._on_demand_sandbox_activity_names = activity_names
        self.use_work_item_filters(WorkItemFilters(
            orchestrations=[],
            activities=[ActivityWorkItemFilter(name=name) for name in activity_names],
            entities=[]))

    def _start_on_demand_sandbox_registration(self) -> None:
        self._on_demand_sandbox_registration_stop.clear()
        self._on_demand_sandbox_registration_thread = threading.Thread(
            target=self._run_on_demand_sandbox_registration_loop,
            name="dts-on-demand-sandbox-worker-registration",
            daemon=True)
        self._on_demand_sandbox_registration_thread.start()

    def _stop_on_demand_sandbox_registration(self) -> None:
        self._on_demand_sandbox_registration_stop.set()
        thread = self._on_demand_sandbox_registration_thread
        if thread is not None:
            thread.join(timeout=10)
            if thread.is_alive():
                self._on_demand_sandbox_logger.warning(
                    "On-demand sandbox activity worker registration thread did not stop within 10 seconds.")
                return
            self._on_demand_sandbox_registration_thread = None

    def _run_on_demand_sandbox_registration_loop(self) -> None:
        retry_delay = 1.0
        while not self._on_demand_sandbox_registration_stop.is_set():
            try:
                client = OnDemandSandboxActivitiesGrpcTransport(
                    host_address=self._on_demand_sandbox_host_address,
                    taskhub=self._on_demand_sandbox_taskhub,
                    token_credential=self._on_demand_sandbox_token_credential,
                    secure_channel=self._on_demand_sandbox_secure_channel)
                try:
                    client.connect_on_demand_sandbox_activity_worker(self._registration_messages())
                    retry_delay = 1.0
                finally:
                    client.close()
            except Exception as ex:
                if self._on_demand_sandbox_registration_stop.is_set():
                    break
                self._on_demand_sandbox_logger.warning("On-demand sandbox activity worker registration failed: %s", ex)
                delay = random.uniform(0, retry_delay)
                self._on_demand_sandbox_registration_stop.wait(delay)
                retry_delay = min(retry_delay * 2, 30.0)

    def _registration_messages(self) -> Iterator:
        yield _build_on_demand_sandbox_worker_start(
            taskhub=self._on_demand_sandbox_taskhub,
            worker_profile_id=self._on_demand_sandbox_worker_profile_id,
            max_activities_count=self._on_demand_sandbox_max_activities,
            activity_names=self._on_demand_sandbox_activity_names,
            substrate=self._on_demand_sandbox_substrate,
            dts_sandbox_identifier=self._on_demand_sandbox_dts_sandbox_identifier)

        while not self._on_demand_sandbox_registration_stop.wait(
                self._on_demand_sandbox_heartbeat_interval_seconds):
            with self._on_demand_sandbox_active_activities_lock:
                active_count = self._on_demand_sandbox_active_activities
            yield _build_on_demand_sandbox_worker_heartbeat(active_count)


def _resolve_taskhub() -> str:
    resolved_taskhub = os.getenv("DTS_TASK_HUB")
    if not resolved_taskhub:
        raise ValueError(
            "On-demand sandbox worker requires DTS_TASK_HUB to be injected in the "
            "sandbox environment.")
    return resolved_taskhub.strip()


def _resolve_host_address() -> str:
    resolved_host_address = os.getenv("DTS_ENDPOINT")
    if not resolved_host_address:
        raise ValueError(
            "On-demand sandbox worker requires DTS_ENDPOINT to be injected in the "
            "sandbox environment.")
    return resolved_host_address.strip()


def _resolve_secure_channel(host_address: str) -> bool:
    lower_host_address = host_address.lower()
    if lower_host_address.startswith(("https://", "grpcs://")):
        return True
    if lower_host_address.startswith(("http://", "grpc://")):
        return False
    return True


def _resolve_worker_profile_id() -> str:
    resolved_worker_profile_id = (
        os.getenv("DTS_WORKER_PROFILE_ID")
        or DEFAULT_WORKER_PROFILE_ID)
    return resolved_worker_profile_id.strip()


def _resolve_token_credential():
    authentication = os.getenv("DTS_AUTHENTICATION", "")
    if authentication.lower() != "managedidentity":
        return None

    client_id = os.getenv("DTS_UMI_CLIENT_ID", "")
    if not client_id.strip():
        raise ValueError(
            "On-demand sandbox worker requires DTS_UMI_CLIENT_ID to be injected when "
            "DTS_AUTHENTICATION is ManagedIdentity.")

    return ManagedIdentityCredential(client_id=client_id.strip())


def _resolve_max_concurrent_activities() -> int:
    value = os.getenv("DTS_ON_DEMAND_SANDBOX_MAX_ACTIVITIES")
    max_concurrent_activities = (
        int(value)
        if value
        else DEFAULT_MAX_CONCURRENT_ACTIVITIES)

    if max_concurrent_activities <= 0:
        raise ValueError(
            "On-demand sandbox activity worker max concurrent activities must be greater than zero.")
    return max_concurrent_activities
