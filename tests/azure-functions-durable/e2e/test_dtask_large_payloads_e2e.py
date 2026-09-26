# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Large payloads across Functions clients, replay, and activity bindings."""

import json
import os

import pytest

from ._harness import http_request

pytestmark = pytest.mark.functions_e2e


@pytest.mark.parametrize("size", [32, 300_000])
@pytest.mark.parametrize("sync_start", [False, True])
def test_payload_roundtrip(dtask_app, size, sync_start):
    payload = {"data": "x" * size, "stages": []}
    if sync_start:
        response = http_request(
            "POST", f"{dtask_app.base_url}/api/payload-start-sync", data=payload)
        assert response.status == 202, response.body
        instance_id = response.json()["id"]
    else:
        instance_id = dtask_app.start_orchestration("payload_roundtrip", payload)

    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED", status
    expected = {"data": payload["data"], "stages": ["activity", "activity"]}
    assert status["output"] == expected
    assert status["customStatus"] == expected
    response = http_request(
        "GET", f"{dtask_app.base_url}/api/payload-status-sync/{instance_id}")
    assert response.status == 200, response.body
    assert response.json() == {"input": payload, "output": expected}

    if size > 262_144:
        from azure.storage.blob import BlobServiceClient

        with BlobServiceClient.from_connection_string("UseDevelopmentStorage=true") as storage:
            container = storage.get_container_client(os.environ["E2E_PAYLOAD_CONTAINER"])
            blobs = list(container.list_blobs(name_starts_with=f"{instance_id}/"))
            assert len(blobs) >= 4


@pytest.mark.parametrize("orchestrator", [
    "payload_entity_roundtrip", "payload_event_roundtrip", "payload_continue_roundtrip",
])
def test_large_payload_durable_operations(dtask_app, orchestrator):
    payload = {"data": "x" * 300_000, "stages": []}
    instance_id = dtask_app.start_orchestration(orchestrator, payload)
    if orchestrator == "payload_event_roundtrip":
        dtask_app.raise_event(instance_id, "payload", payload)
    status = dtask_app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED", status
    expected = dict(payload)
    if orchestrator == "payload_continue_roundtrip":
        expected["stages"] = ["continued", "activity", "activity"]
    assert status["output"] == expected
    if orchestrator == "payload_entity_roundtrip":
        for mode in ("sync", "async"):
            response = http_request(
                "GET", f"{dtask_app.base_url}/api/payload-history-{mode}/{instance_id}")
            assert response.status == 200, response.body
            envelopes = response.json()
            results = [json.loads(envelope["result"]) for envelope in envelopes
                       if isinstance(envelope, dict) and envelope.get("result")]
            assert results.count(payload) == 2
            inputs = [json.loads(envelope["input"]) for envelope in envelopes
                      if isinstance(envelope, dict) and envelope.get("op") == "set"]
            assert inputs == [payload]
