# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Remote worker image entrypoint for the Durable Task Scheduler sandbox activities sample."""

import os
import threading

from durabletask import task
from durabletask.azuremanaged.preview.sandboxes import SandboxWorker

from activities import REMOTE_HELLO


def _remote_hello(ctx: task.ActivityContext, name: str) -> str:
    """Activity function that runs inside the sandbox worker container."""
    sandbox_id = os.getenv("DTS_SANDBOX_ID", "unknown-sandbox")
    marker = os.getenv("SANDBOX_SAMPLE_MARKER", "<missing>")
    return f"Hello {name} from Python sandbox worker {sandbox_id}! SANDBOX_SAMPLE_MARKER={marker}"


_remote_hello.__name__ = REMOTE_HELLO.name


with SandboxWorker() as worker:
    worker.add_activity(_remote_hello)
    worker.start()
    print("Python sandbox remote worker is running. Press Ctrl+C to stop.")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        # Expected on Ctrl+C: exit loop and let the context manager stop the worker gracefully.
        pass
