# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""End-to-end tests for large-payload externalization using Azure Durable Task
Scheduler (DTS) and Azurite.

These tests use the DTS emulator for orchestration and Azurite for blob
storage.  They verify that payloads too large for inline gRPC messages are
transparently offloaded to blob storage and recovered on the other side
when using the ``DurableTaskSchedulerWorker`` and
``DurableTaskSchedulerClient`` classes.

Prerequisites:
    - DTS emulator must be running locally.
      Start it with:
      ``docker run -i -p 8080:8080 -p 8082:8082 -d mcr.microsoft.com/dts/dts-emulator:latest``
    - Azurite must be running locally on the default blob port (10000).
      Start it with:  ``azurite --silent --blobPort 10000``
    - ``azure-storage-blob`` must be installed.
"""

import json
import os
import uuid

import pytest

from durabletask import client, task
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker

# Skip the entire module if azure-storage-blob is not installed.
azure_blob = pytest.importorskip("azure.storage.blob")

from durabletask.extensions.azure_blob_payloads import BlobPayloadStore, BlobPayloadStoreOptions  # noqa: E402

# DTS emulator settings
taskhub_name = os.getenv("TASKHUB", "default")
endpoint = os.getenv("ENDPOINT", "http://localhost:8080")

# Azurite well-known connection string
AZURITE_CONN_STR = "UseDevelopmentStorage=true"

# Use a unique container per test run to avoid collisions.
TEST_CONTAINER = f"dts-payloads-{uuid.uuid4().hex[:8]}"

# A low threshold so we can trigger externalization without massive strings.
# In production the default is 900 KB; here we use 1 KB for fast tests.
THRESHOLD_BYTES = 1_024

# Pin API version to one that Azurite supports.
AZURITE_API_VERSION = "2024-08-04"


# ------------------------------------------------------------------
# Skip checks
# ------------------------------------------------------------------


def _azurite_is_running() -> bool:
    """Return True if Azurite blob service is reachable."""
    try:
        svc = azure_blob.BlobServiceClient.from_connection_string(
            AZURITE_CONN_STR, api_version=AZURITE_API_VERSION,
        )
        next(iter(svc.list_containers(results_per_page=1)), None)
        return True
    except Exception:
        return False


pytestmark = [
    pytest.mark.dts,
    pytest.mark.azurite,
    pytest.mark.skipif(
        not _azurite_is_running(),
        reason="Azurite blob service is not running on 127.0.0.1:10000",
    ),
]


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture(scope="module")
def payload_store():
    """Create a BlobPayloadStore pointing at Azurite with a low threshold."""
    store = BlobPayloadStore(BlobPayloadStoreOptions(
        connection_string=AZURITE_CONN_STR,
        container_name=TEST_CONTAINER,
        threshold_bytes=THRESHOLD_BYTES,
        enable_compression=True,
        api_version=AZURITE_API_VERSION,
    ))
    yield store

    # Clean up: delete the test container.
    try:
        svc = azure_blob.BlobServiceClient.from_connection_string(
            AZURITE_CONN_STR, api_version=AZURITE_API_VERSION,
        )
        svc.delete_container(TEST_CONTAINER)
    except Exception:
        pass


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _make_large_string(size_kb: int = 2) -> str:
    """Return a JSON-serializable string larger than THRESHOLD_BYTES."""
    return "X" * (size_kb * 1024)


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------


class TestDTSLargeInputOutput:
    """Orchestrations whose input/output exceeds the threshold."""

    def test_large_input_round_trips(self, payload_store):
        """A large orchestration input is externalized then recovered."""
        large_input = _make_large_string(5)  # 5 KB > 1 KB threshold

        def echo(ctx: task.OrchestrationContext, inp: str):
            return inp

        with DurableTaskSchedulerWorker(
            host_address=endpoint, secure_channel=True,
            taskhub=taskhub_name, token_credential=None,
            payload_store=payload_store,
        ) as w:
            w.add_orchestrator(echo)
            w.start()

            c = DurableTaskSchedulerClient(
                host_address=endpoint, secure_channel=True,
                taskhub=taskhub_name, token_credential=None,
                payload_store=payload_store,
            )
            inst_id = c.schedule_new_orchestration(echo, input=large_input)
            state = c.wait_for_orchestration_completion(inst_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None
        assert json.loads(state.serialized_output) == large_input

    def test_large_activity_result(self, payload_store):
        """A large activity return value is externalized then recovered."""
        def produce_large(_: task.ActivityContext, size_kb: int) -> str:
            return "Z" * (size_kb * 1024)

        def orchestrator(ctx: task.OrchestrationContext, size_kb: int):
            result = yield ctx.call_activity(produce_large, input=size_kb)
            return result

        with DurableTaskSchedulerWorker(
            host_address=endpoint, secure_channel=True,
            taskhub=taskhub_name, token_credential=None,
            payload_store=payload_store,
        ) as w:
            w.add_orchestrator(orchestrator)
            w.add_activity(produce_large)
            w.start()

            c = DurableTaskSchedulerClient(
                host_address=endpoint, secure_channel=True,
                taskhub=taskhub_name, token_credential=None,
                payload_store=payload_store,
            )
            inst_id = c.schedule_new_orchestration(orchestrator, input=10)  # 10 KB
            state = c.wait_for_orchestration_completion(inst_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None
        output = json.loads(state.serialized_output)
        assert len(output) == 10 * 1024

    def test_large_input_and_output(self, payload_store):
        """Both input and output are large — both directions externalize."""
        large_input = {"data": "Y" * (5 * 1024)}

        def transform(ctx: task.OrchestrationContext, inp: dict):
            return {"echo": inp["data"], "extra": "A" * 3000}

        with DurableTaskSchedulerWorker(
            host_address=endpoint, secure_channel=True,
            taskhub=taskhub_name, token_credential=None,
            payload_store=payload_store,
        ) as w:
            w.add_orchestrator(transform)
            w.start()

            c = DurableTaskSchedulerClient(
                host_address=endpoint, secure_channel=True,
                taskhub=taskhub_name, token_credential=None,
                payload_store=payload_store,
            )
            inst_id = c.schedule_new_orchestration(transform, input=large_input)
            state = c.wait_for_orchestration_completion(inst_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None
        output = json.loads(state.serialized_output)
        assert output["echo"] == large_input["data"]


class TestDTSLargeEvents:
    """External events carrying large payloads."""

    def test_large_event_data(self, payload_store):
        """A large external event payload is externalized and resolved."""
        large_event = _make_large_string(5)

        def wait_for_event(ctx: task.OrchestrationContext, _):
            result = yield ctx.wait_for_external_event("big_event")
            return result

        with DurableTaskSchedulerWorker(
            host_address=endpoint, secure_channel=True,
            taskhub=taskhub_name, token_credential=None,
            payload_store=payload_store,
        ) as w:
            w.add_orchestrator(wait_for_event)
            w.start()

            c = DurableTaskSchedulerClient(
                host_address=endpoint, secure_channel=True,
                taskhub=taskhub_name, token_credential=None,
                payload_store=payload_store,
            )
            inst_id = c.schedule_new_orchestration(wait_for_event)
            c.wait_for_orchestration_start(inst_id, timeout=10)

            c.raise_orchestration_event(inst_id, "big_event", data=large_event)
            state = c.wait_for_orchestration_completion(inst_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None
        assert json.loads(state.serialized_output) == large_event


class TestDTSLargeFanOut:
    """Fan-out/fan-in with multiple large activity results."""

    def test_fan_out_fan_in_large_results(self, payload_store):
        """Multiple activities each return large payloads."""
        def make_large(_: task.ActivityContext, idx: int) -> str:
            return f"result-{idx}-" + ("D" * 2000)

        def fan_out(ctx: task.OrchestrationContext, count: int):
            tasks = [ctx.call_activity(make_large, input=i) for i in range(count)]
            results = yield task.when_all(tasks)
            return results

        with DurableTaskSchedulerWorker(
            host_address=endpoint, secure_channel=True,
            taskhub=taskhub_name, token_credential=None,
            payload_store=payload_store,
        ) as w:
            w.add_orchestrator(fan_out)
            w.add_activity(make_large)
            w.start()

            c = DurableTaskSchedulerClient(
                host_address=endpoint, secure_channel=True,
                taskhub=taskhub_name, token_credential=None,
                payload_store=payload_store,
            )
            inst_id = c.schedule_new_orchestration(fan_out, input=5)
            state = c.wait_for_orchestration_completion(inst_id, timeout=60)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None
        results = json.loads(state.serialized_output)
        assert len(results) == 5
        for i, r in enumerate(results):
            assert r.startswith(f"result-{i}-")


class TestDTSLargeTerminate:
    """Terminate with a large output payload."""

    def test_terminate_with_large_output(self, payload_store):
        """Terminating with a large output externalizes it."""
        large_output = _make_large_string(3)

        def long_running(ctx: task.OrchestrationContext, _):
            yield ctx.wait_for_external_event("never_arrives")

        with DurableTaskSchedulerWorker(
            host_address=endpoint, secure_channel=True,
            taskhub=taskhub_name, token_credential=None,
            payload_store=payload_store,
        ) as w:
            w.add_orchestrator(long_running)
            w.start()

            c = DurableTaskSchedulerClient(
                host_address=endpoint, secure_channel=True,
                taskhub=taskhub_name, token_credential=None,
                payload_store=payload_store,
            )
            inst_id = c.schedule_new_orchestration(long_running)
            c.wait_for_orchestration_start(inst_id, timeout=10)

            c.terminate_orchestration(inst_id, output=large_output)
            state = c.wait_for_orchestration_completion(inst_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.TERMINATED
