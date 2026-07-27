# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for durabletask-native retries and failure propagation.

Covers activity ``retry_policy`` (eventual success and exhausted retries),
sub-orchestrator ``retry_policy`` (eventual success), activity-failure
propagation, and sub-orchestration-failure propagation.
"""

import pytest

pytestmark = pytest.mark.functions_e2e


def test_activity_retry_eventual_success(dtask_app):
    instance_id = dtask_app.start_orchestration("retry_then_succeed")
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    assert status["output"] == {"attempts": 3}


def test_activity_retry_exhausted_fails(dtask_app):
    instance_id = dtask_app.start_orchestration("retry_exhausted")
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "FAILED"


def test_sub_orchestrator_retry_eventual_success(dtask_app):
    instance_id = dtask_app.start_orchestration("suborch_retry_then_succeed")
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"
    assert status["output"] == {"attempts": 2}


def test_activity_failure_fails_orchestration(dtask_app):
    instance_id = dtask_app.start_orchestration("activity_fails")
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "FAILED"


def test_sub_orchestration_failure_propagates(dtask_app):
    instance_id = dtask_app.start_orchestration("sub_orch_fails")
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "FAILED"
