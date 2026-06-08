"""Remote worker image entrypoint for the DTS on-demand sandbox activities sample."""

import os
import time

from durabletask import task
from durabletask.azuremanaged.preview.on_demand_sandbox import OnDemandSandboxWorker

from activity_names import REMOTE_HELLO


def _remote_hello(ctx: task.ActivityContext, name: str) -> str:
    """Activity function that runs inside the on-demand sandbox worker container."""
    sandbox_id = os.getenv("DTS_SANDBOX_ID", "unknown-sandbox")
    marker = os.getenv("ON_DEMAND_SANDBOX_SAMPLE_MARKER", "<missing>")
    return f"Hello {name} from Python on-demand sandbox worker {sandbox_id}! ON_DEMAND_SANDBOX_SAMPLE_MARKER={marker}"


_remote_hello.__name__ = REMOTE_HELLO


with OnDemandSandboxWorker() as worker:
    worker.add_activity(_remote_hello)
    worker.start()
    print("Python on-demand sandbox remote worker is running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
