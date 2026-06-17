# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import random
import threading

from typing import Any, Iterator, Optional

from azure.core.credentials import TokenCredential
from azure.identity import ManagedIdentityCredential

from durabletask import task
from durabletask.azuremanaged.internal import sandbox_service_pb2 as pb
from durabletask.azuremanaged.preview.sandboxes.helpers import SandboxActivity
from durabletask.azuremanaged.preview.sandboxes.helpers import resolve_activities
from durabletask.azuremanaged.preview.sandboxes.worker_profiles import (
    DEFAULT_MAX_CONCURRENT_ACTIVITIES,
    build_sandbox_worker_heartbeat,
    build_sandbox_worker_start,
)
from durabletask.azuremanaged.preview.sandboxes.transport import (
    SandboxActivitiesGrpcTransport,
)
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
import durabletask.internal.orchestrator_service_pb2 as worker_pb
import durabletask.internal.shared as shared
from durabletask.worker import (
    ActivityWorkItemFilter,
    ConcurrencyOptions,
    WorkItemFilters,
)


class SandboxWorker(DurableTaskSchedulerWorker):
    """Durable Task Scheduler worker mode for activity containers started by sandbox activities.

    This worker registers a live worker session with Durable Task Scheduler and
    restricts dispatch to the activities registered on this worker.
    """

    def __init__(self) -> None:
        resolved_host_address = _resolve_host_address()
        resolved_taskhub = _resolve_taskhub()
        resolved_secure_channel = _resolve_secure_channel(resolved_host_address)
        resolved_token_credential = _resolve_token_credential()
        resolved_max_concurrent_activities = _resolve_max_concurrent_activities()
        resolved_sandbox_provider = _resolve_sandbox_provider()
        concurrency_options = ConcurrencyOptions(
            maximum_concurrent_activity_work_items=resolved_max_concurrent_activities)

        self._sandbox_host_address = resolved_host_address
        self._sandbox_secure_channel = resolved_secure_channel
        self._sandbox_token_credential = resolved_token_credential
        self._sandbox_logger = shared.get_logger("worker")

        super().__init__(
            host_address=resolved_host_address,
            taskhub=resolved_taskhub,
            token_credential=resolved_token_credential,
            secure_channel=resolved_secure_channel,
            concurrency_options=concurrency_options)

        self._sandbox_taskhub = resolved_taskhub
        self._sandbox_worker_profile_id = _resolve_worker_profile_id()
        self._sandbox_activities: list[SandboxActivity] = []
        self._sandbox_max_activities = resolved_max_concurrent_activities
        self._sandbox_provider = resolved_sandbox_provider
        self._sandbox_dts_sandbox_identifier = os.getenv("DTS_SANDBOX_ID")
        self._sandbox_heartbeat_interval_seconds = 2.0
        self._sandbox_registration_stop = threading.Event()
        self._sandbox_registration_thread: Optional[threading.Thread] = None
        self._sandbox_active_activities = 0
        self._sandbox_active_activities_lock = threading.Lock()

    def add_activity(
            self,
            fn: task.Activity[Any, Any],
            **kwargs: Any) -> str:
        version = kwargs.pop("version", None)
        if kwargs:
            unexpected = next(iter(kwargs))
            raise TypeError(f"Unexpected keyword argument: {unexpected}")
        activity_name = super().add_activity(fn)
        self._sandbox_activities.append(SandboxActivity(activity_name, version))
        return activity_name

    def start(self) -> None:
        self._configure_sandbox_activity_filters()
        super().start()
        self._start_sandbox_registration()

    def stop(self) -> None:
        self._stop_sandbox_registration()
        super().stop()

    def _on_activity_execution_started(self, req: worker_pb.ActivityRequest) -> None:
        with self._sandbox_active_activities_lock:
            self._sandbox_active_activities += 1

    def _on_activity_execution_completed(self, req: worker_pb.ActivityRequest) -> None:
        with self._sandbox_active_activities_lock:
            self._sandbox_active_activities = max(0, self._sandbox_active_activities - 1)

    def _configure_sandbox_activity_filters(self) -> None:
        activities = resolve_activities(self._sandbox_activities)
        if not activities:
            raise RuntimeError(
                "Sandbox worker requires at least one registered activity before it can register.")

        self._sandbox_activities = activities
        self.use_work_item_filters(WorkItemFilters(
            orchestrations=[],
            activities=[ActivityWorkItemFilter(
                name=activity.name,
                versions=[] if activity.version is None else [activity.version]) for activity in activities],
            entities=[]))

    def _start_sandbox_registration(self) -> None:
        self._sandbox_registration_stop.clear()
        self._sandbox_registration_thread = threading.Thread(
            target=self._run_sandbox_registration_loop,
            name="dts-sandboxes-worker-registration",
            daemon=True)
        self._sandbox_registration_thread.start()

    def _stop_sandbox_registration(self) -> None:
        self._sandbox_registration_stop.set()
        thread = self._sandbox_registration_thread
        if thread is not None:
            thread.join(timeout=10)
            if thread.is_alive():
                self._sandbox_logger.warning(
                    "Sandbox activity worker registration thread did not stop within 10 seconds.")
                return
            self._sandbox_registration_thread = None

    def _run_sandbox_registration_loop(self) -> None:
        retry_delay = 1.0
        while not self._sandbox_registration_stop.is_set():
            try:
                client = SandboxActivitiesGrpcTransport(
                    host_address=self._sandbox_host_address,
                    taskhub=self._sandbox_taskhub,
                    token_credential=self._sandbox_token_credential,
                    secure_channel=self._sandbox_secure_channel)
                try:
                    client.connect_sandbox_activity_worker(self._registration_messages())
                    retry_delay = 1.0
                finally:
                    client.close()
            except Exception as ex:
                if self._sandbox_registration_stop.is_set():
                    break
                self._sandbox_logger.warning("Sandbox activity worker registration failed: %s", ex)
                delay = random.uniform(0, retry_delay)
                self._sandbox_registration_stop.wait(delay)
                retry_delay = min(retry_delay * 2, 30.0)

    def _registration_messages(self) -> Iterator[pb.SandboxActivityWorkerMessage]:
        yield build_sandbox_worker_start(
            taskhub=self._sandbox_taskhub,
            worker_profile_id=self._sandbox_worker_profile_id,
            max_activities_count=self._sandbox_max_activities,
            activities=self._sandbox_activities,
            sandbox_provider=self._sandbox_provider,
            dts_sandbox_identifier=self._sandbox_dts_sandbox_identifier)

        while not self._sandbox_registration_stop.wait(
                self._sandbox_heartbeat_interval_seconds):
            with self._sandbox_active_activities_lock:
                active_count = self._sandbox_active_activities
            yield build_sandbox_worker_heartbeat(active_count)


def _resolve_taskhub() -> str:
    resolved_taskhub = os.getenv("DTS_TASK_HUB")
    if not resolved_taskhub:
        raise ValueError(
            "Sandbox worker requires DTS_TASK_HUB to be injected in the "
            "sandbox environment.")
    return resolved_taskhub.strip()


def _resolve_host_address() -> str:
    resolved_host_address = os.getenv("DTS_ENDPOINT")
    if not resolved_host_address:
        raise ValueError(
            "Sandbox worker requires DTS_ENDPOINT to be injected in the "
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
    resolved_worker_profile_id = os.getenv("DTS_WORKER_PROFILE_ID")
    if not resolved_worker_profile_id or not resolved_worker_profile_id.strip():
        raise ValueError(
            "Sandbox worker requires DTS_WORKER_PROFILE_ID to be injected in the "
            "sandbox environment.")

    return resolved_worker_profile_id.strip()


def _resolve_token_credential() -> TokenCredential | None:
    authentication = os.getenv("DTS_AUTHENTICATION", "")
    if authentication.strip().lower() != "managedidentity":
        raise ValueError(
            "Sandbox worker requires DTS_AUTHENTICATION to be ManagedIdentity.")

    client_id = os.getenv("DTS_UMI_CLIENT_ID", "")
    if not client_id.strip():
        raise ValueError(
            "Sandbox worker requires DTS_UMI_CLIENT_ID to be injected when "
            "DTS_AUTHENTICATION is ManagedIdentity.")

    return ManagedIdentityCredential(client_id=client_id.strip())


def _resolve_sandbox_provider() -> str:
    sandbox_provider = os.getenv("DTS_SANDBOX_PROVIDER")
    if not sandbox_provider:
        raise ValueError(
            "Sandbox worker requires DTS_SANDBOX_PROVIDER to be injected in the "
            "sandbox environment.")

    normalized = sandbox_provider.strip()
    if normalized.lower() not in ("sandbox", "acasessionpool"):
        raise ValueError(
            "Sandbox worker requires DTS_SANDBOX_PROVIDER to be Sandbox or AcaSessionPool.")

    return normalized


def _resolve_max_concurrent_activities() -> int:
    value = os.getenv("DTS_SANDBOX_MAX_ACTIVITIES")
    if value is None:
        return DEFAULT_MAX_CONCURRENT_ACTIVITIES

    try:
        max_concurrent_activities = int(value.strip())
    except ValueError as ex:
        raise ValueError(
            "DTS_SANDBOX_MAX_ACTIVITIES must be a positive integer when injected by DTS.") from ex

    if max_concurrent_activities <= 0:
        raise ValueError(
            "DTS_SANDBOX_MAX_ACTIVITIES must be a positive integer when injected by DTS.")
    return max_concurrent_activities
