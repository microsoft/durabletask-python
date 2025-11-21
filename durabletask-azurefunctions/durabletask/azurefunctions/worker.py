# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from threading import Event
from durabletask.worker import _Registry, ConcurrencyOptions
from durabletask.internal import shared
from durabletask.worker import TaskHubGrpcWorker


# Worker class used for Durable Task Scheduler (DTS)
class DurableFunctionsWorker(TaskHubGrpcWorker):
    """TODO: Docs
    """

    def __init__(self):
        # Don't call the parent constructor - we don't actually want to start an AsyncWorkerLoop
        # or recieve work items from anywhere but the method that is creating this worker
        self._registry = _Registry()
        self._host_address = ""
        self._logger = shared.get_logger("worker")
        self._shutdown = Event()
        self._is_running = False
        self._secure_channel = False

        self._concurrency_options = ConcurrencyOptions()

        self._interceptors = None

    def add_named_orchestrator(self, name: str, func):
        """TODO: Docs
        """
        self._registry.add_named_orchestrator(name, func)
