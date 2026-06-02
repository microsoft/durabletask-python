# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import random
import threading

from typing import Iterator, Optional

from durabletask.azuremanaged.extensions.serverless.client import (
    DEFAULT_MAX_CONCURRENT_ACTIVITIES,
    DEFAULT_WORKER_PROFILE_ID,
    ServerlessActivitiesClient,
    build_serverless_worker_heartbeat,
    build_serverless_worker_start,
    resolve_activity_names,
)
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from durabletask.worker import (
    ActivityWorkItemFilter,
    ConcurrencyOptions,
    WorkItemFilters,
)


class ServerlessWorker(DurableTaskSchedulerWorker):
    """DTS worker mode for activity containers started by serverless activities.

    This worker registers a live worker session with DTS and restricts dispatch
    to the activities registered on this worker.
    """

    def __init__(self):
        resolved_host_address = _resolve_host_address()
        resolved_taskhub = _resolve_taskhub()
        resolved_secure_channel = _resolve_secure_channel(resolved_host_address)
        resolved_token_credential = None
        resolved_max_concurrent_activities = _resolve_max_concurrent_activities()
        concurrency_options = ConcurrencyOptions(
            maximum_concurrent_activity_work_items=resolved_max_concurrent_activities)

        self._serverless_token_credential = resolved_token_credential

        super().__init__(
            host_address=resolved_host_address,
            taskhub=resolved_taskhub,
            token_credential=resolved_token_credential,
            secure_channel=resolved_secure_channel,
            concurrency_options=concurrency_options)

        self._serverless_taskhub = resolved_taskhub
        self._serverless_worker_profile_id = _resolve_worker_profile_id()
        self._serverless_activity_names: list[str] = []
        self._serverless_max_activities = resolved_max_concurrent_activities
        self._serverless_substrate = os.getenv("DTS_SUBSTRATE")
        self._serverless_dts_sandbox_identifier = os.getenv("DTS_SANDBOX_ID")
        self._serverless_heartbeat_interval_seconds = 2.0
        self._serverless_registration_stop = threading.Event()
        self._serverless_registration_thread: Optional[threading.Thread] = None
        self._serverless_active_activities = 0
        self._serverless_active_activities_lock = threading.Lock()

    def start(self) -> None:
        if self._is_running:
            raise RuntimeError("The worker is already running.")

        self._configure_serverless_activity_filters()
        super().start()
        self._start_serverless_registration()

    def stop(self) -> None:
        self._stop_serverless_registration()
        super().stop()

    def _execute_activity(self, req, stub, completionToken):
        with self._serverless_active_activities_lock:
            self._serverless_active_activities += 1
        try:
            return super()._execute_activity(req, stub, completionToken)
        finally:
            with self._serverless_active_activities_lock:
                self._serverless_active_activities = max(0, self._serverless_active_activities - 1)

    def _configure_serverless_activity_filters(self) -> None:
        activity_names = resolve_activity_names(self._registry.activities.keys())
        if not activity_names:
            raise RuntimeError(
                "Serverless worker requires at least one registered activity before it can register.")

        self._serverless_activity_names = activity_names
        self.use_work_item_filters(WorkItemFilters(
            orchestrations=[],
            activities=[ActivityWorkItemFilter(name=name) for name in activity_names],
            entities=[]))

    def _start_serverless_registration(self) -> None:
        self._serverless_registration_stop.clear()
        self._serverless_registration_thread = threading.Thread(
            target=self._run_serverless_registration_loop,
            name="dts-serverless-worker-registration",
            daemon=True)
        self._serverless_registration_thread.start()

    def _stop_serverless_registration(self) -> None:
        self._serverless_registration_stop.set()
        if self._serverless_registration_thread is not None:
            self._serverless_registration_thread.join(timeout=10)
            self._serverless_registration_thread = None

    def _run_serverless_registration_loop(self) -> None:
        retry_delay = 1.0
        while not self._serverless_registration_stop.is_set():
            try:
                client = ServerlessActivitiesClient(
                    host_address=self._host_address,
                    taskhub=self._serverless_taskhub,
                    token_credential=self._serverless_token_credential,
                    channel=self._channel,
                    secure_channel=self._secure_channel,
                    channel_options=self._channel_options)
                try:
                    result = client.connect_serverless_activity_worker(self._registration_messages())
                    if not result.accepted:
                        self._logger.warning(
                            "Serverless activity worker registration was rejected: %s",
                            result.message)
                    retry_delay = 1.0
                finally:
                    client.close()
            except Exception as ex:
                if self._serverless_registration_stop.is_set():
                    break
                self._logger.warning("Serverless activity worker registration failed: %s", ex)
                delay = random.uniform(0, retry_delay)
                self._serverless_registration_stop.wait(delay)
                retry_delay = min(retry_delay * 2, 30.0)

    def _registration_messages(self) -> Iterator:
        yield build_serverless_worker_start(
            taskhub=self._serverless_taskhub,
            worker_profile_id=self._serverless_worker_profile_id,
            max_activities_count=self._serverless_max_activities,
            activity_names=self._serverless_activity_names,
            substrate=self._serverless_substrate,
            dts_sandbox_identifier=self._serverless_dts_sandbox_identifier)

        while not self._serverless_registration_stop.wait(
                self._serverless_heartbeat_interval_seconds):
            with self._serverless_active_activities_lock:
                active_count = self._serverless_active_activities
            yield build_serverless_worker_heartbeat(active_count)


def _resolve_taskhub() -> str:
    resolved_taskhub = os.getenv("DTS_TASK_HUB")
    if not resolved_taskhub:
        raise ValueError(
            "Serverless worker requires DTS_TASK_HUB to be injected in the "
            "sandbox environment.")
    return resolved_taskhub.strip()


def _resolve_host_address() -> str:
    resolved_host_address = os.getenv("DTS_ENDPOINT")
    if not resolved_host_address:
        raise ValueError(
            "Serverless worker requires DTS_ENDPOINT to be injected in the "
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


def _resolve_max_concurrent_activities() -> int:
    value = os.getenv("DTS_SERVERLESS_MAX_ACTIVITIES")
    max_concurrent_activities = (
        int(value)
        if value
        else DEFAULT_MAX_CONCURRENT_ACTIVITIES)

    if max_concurrent_activities <= 0:
        raise ValueError(
            "Serverless activity worker max concurrent activities must be greater than zero.")
    return max_concurrent_activities
