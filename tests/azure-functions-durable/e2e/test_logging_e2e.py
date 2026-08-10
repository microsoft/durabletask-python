# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E validation for Durable Functions SDK logging."""

import re

import pytest

from ._harness import http_request

pytestmark = pytest.mark.functions_e2e


def _assert_host_routed(line: str, message: str) -> None:
    assert re.fullmatch(rf"\[[^\]]+Z\] {re.escape(message)}", line)


def test_client_and_worker_logs_flow_through_host(dtask_app):
    instance_id = dtask_app.start_orchestration("activity_chain")
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"

    client_message = (
        f"Starting new 'activity_chain' instance with ID = '{instance_id}'.")
    worker_message = (
        f"{instance_id}: Orchestration activity_chain completed with status: COMPLETED")
    client_line = dtask_app.wait_for_host_log(client_message)
    worker_line = dtask_app.wait_for_host_log(worker_message)

    _assert_host_routed(client_line, client_message)
    _assert_host_routed(worker_line, worker_message)


def test_host_category_filter_applies_to_client_and_worker_logs(dtask_app):
    result = http_request(
        "POST",
        f"{dtask_app.base_url}/api/start-logging-filtered/logging_filtered",
        data={"input": None},
    )
    assert result.status == 202
    instance_id = result.json()["id"]

    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    assert status["output"] == "logged"

    dtask_app.wait_for_host_log(f"client-filter-anchor {instance_id}")
    dtask_app.wait_for_host_log(f"worker-filter-anchor {instance_id}")
    log = dtask_app.read_host_log()

    assert f"client-info-filter-anchor {instance_id}" not in log
    assert f"worker-info-filter-anchor {instance_id}" not in log
    assert (
        f"Starting new 'logging_filtered' instance with ID = '{instance_id}'."
        not in log
    )
    assert (
        f"{instance_id}: Orchestration logging_filtered completed with status: COMPLETED"
        not in log
    )
