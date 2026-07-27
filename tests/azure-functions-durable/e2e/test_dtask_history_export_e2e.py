# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E test for the history-export extension driven through the Functions host.

Exercises ``durabletask.extensions.history_export`` (opt-in via
``app.configure_history_export()``): the export-job entity schedules the driving
orchestrator, which fans out the export activities to serialize each terminal
instance's history to a local-filesystem writer. Verifies the job completes and
the exported blobs contain the instances we produced.
"""

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytestmark = pytest.mark.functions_e2e


def test_history_export_batch(dtask_app):
    # Scope the export to instances completed from just before this test, so the
    # fan-out only covers the instances we create here (not every terminal
    # instance produced by the wider session).
    completed_from = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()

    # Produce a couple of terminal orchestrations to export.
    instance_ids = []
    for _ in range(2):
        iid = dtask_app.start_orchestration("activity_chain")
        dtask_app.wait_for_completion(iid)
        instance_ids.append(iid)

    container = f"exp-{int(time.time() * 1000)}"
    started = dtask_app.start_export(container=container, completed_from=completed_from)
    job_id = started["jobId"]
    export_root = Path(started["exportRoot"])

    final = dtask_app.wait_for_export(job_id)
    assert final["status"] == "Completed", final
    # The narrowed window covers exactly the two instances we produced.
    assert final["exported"] >= 2

    container_dir = export_root / container
    files = list(container_dir.glob("*.json"))
    assert len(files) >= 2, f"expected >=2 exported files in {container_dir}, got {files}"

    # Each exported blob is self-describing (it carries the instance's
    # OrchestrationState metadata), so our instance IDs must appear in the output.
    exported_text = "\n".join(f.read_text(encoding="utf-8") for f in files)
    for iid in instance_ids:
        assert iid in exported_text, f"instance {iid} not found in exported history"


def test_history_export_continuous_is_rejected(dtask_app):
    # Continuous export is unsupported on Azure Functions (the QueryInstances
    # enumeration can't safely tail a growing set). The Functions export entity
    # must reject it at create: the job ends FAILED with an explanatory reason,
    # rather than starting and silently missing histories.
    container = f"exp-cont-{int(time.time() * 1000)}"
    started = dtask_app.start_export(container=container, mode="continuous")
    job_id = started["jobId"]

    final = dtask_app.wait_for_export(job_id)
    assert final["status"] == "Failed", final
    assert "continuous" in (final.get("lastError") or "").lower(), final
