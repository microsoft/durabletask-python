# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for the V1-only durable HTTP feature (``context.call_http``).

Covers a request that returns content (POST + echo) and the non-2xx path (the
response is returned to the orchestrator rather than raising). The GET happy
path is covered in ``test_v1_style_e2e.py``.
"""

import json

import pytest

pytestmark = pytest.mark.functions_e2e


def test_call_http_post_with_content(v1_app):
    payload = {"url": f"{v1_app.base_url}/api/echo", "content": {"hello": "world"}}
    instance_id = v1_app.start_orchestration("http_post", body=payload)
    status = v1_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "Completed"
    output = status["output"]
    assert output["status_code"] == 200
    assert json.loads(output["content"]) == {"hello": "world"}


def test_call_http_non_2xx_is_returned(v1_app):
    instance_id = v1_app.start_orchestration("http_call", body=f"{v1_app.base_url}/api/fail")
    status = v1_app.wait_for_completion(instance_id)
    # A non-2xx response is returned to the orchestrator, not raised.
    assert status["runtimeStatus"] == "Completed"
    assert status["output"]["status_code"] == 500
    assert status["output"]["content"] == "nope"
