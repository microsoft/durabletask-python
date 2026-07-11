# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for V1-style retries and failure propagation.

Covers ``call_activity_with_retry`` (eventual success and exhausted retries),
``call_sub_orchestrator_with_retry`` (eventual success), activity-failure
propagation, sub-orchestration-failure propagation, and the documented
``histories`` NotImplementedError surfacing as a failed orchestration.
"""

import pytest

pytestmark = pytest.mark.functions_e2e


# ---------------------------------------------------------------------------
# Retries
# ---------------------------------------------------------------------------

def test_activity_retry_eventual_success(v1_app):
    instance_id = v1_app.start_orchestration("retry_then_succeed")
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    # Succeeds on the 3rd attempt (threshold=3).
    assert status["output"] == {"attempts": 3}


def test_activity_retry_exhausted_fails(v1_app):
    instance_id = v1_app.start_orchestration("retry_exhausted")
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Failed"


def test_sub_orchestrator_retry_eventual_success(v1_app):
    instance_id = v1_app.start_orchestration("suborch_retry_then_succeed")
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    # The sub-orchestration is retried; its activity succeeds on attempt 2.
    assert status["output"] == {"attempts": 2}


# ---------------------------------------------------------------------------
# Failure propagation
# ---------------------------------------------------------------------------

def test_activity_failure_fails_orchestration(v1_app):
    instance_id = v1_app.start_orchestration("activity_fails")
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Failed"


def test_sub_orchestration_failure_propagates(v1_app):
    instance_id = v1_app.start_orchestration("sub_orch_fails")
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Failed"


def test_histories_not_implemented_fails(v1_app):
    instance_id = v1_app.start_orchestration("access_histories")
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Failed"
