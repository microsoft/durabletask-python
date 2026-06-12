"""Remote worker image entrypoint for the DTS serverless activities sample."""

import os
import time

from durabletask import task
from durabletask.azuremanaged.extensions.serverless import ServerlessWorker

from activity_names import REMOTE_HELLO


def _remote_hello(ctx: task.ActivityContext, name: str) -> str:
    """Activity function that runs inside the serverless worker container."""
    sandbox_id = os.getenv("DTS_SANDBOX_ID", "unknown-sandbox")
    marker = os.getenv("SERVERLESS_SAMPLE_MARKER", "<missing>")
    return f"Hello {name} from Python serverless worker {sandbox_id}! SERVERLESS_SAMPLE_MARKER={marker}"


_remote_hello.__name__ = REMOTE_HELLO


with ServerlessWorker() as worker:
    worker.add_activity(_remote_hello)
    worker.start()
    print("Python serverless remote worker is running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
