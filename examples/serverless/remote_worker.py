"""Remote worker image entrypoint for the DTS serverless activities sample."""

import os
import time

from durabletask import task
from durabletask.azuremanaged.extensions.serverless import DurableTaskSchedulerServerlessWorker


def remote_hello(ctx: task.ActivityContext, name: str) -> str:
    """Activity function that runs inside the serverless worker container."""
    sandbox_id = os.getenv("DTS_SANDBOX_ID", "unknown-sandbox")
    return f"Hello {name} from Python serverless worker {sandbox_id}!"


with DurableTaskSchedulerServerlessWorker() as worker:
    worker.add_activity(remote_hello)
    worker.start()
    print("Python serverless remote worker is running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
