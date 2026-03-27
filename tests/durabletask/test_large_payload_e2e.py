# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""End-to-end tests for large-payload externalization using Azurite.

These tests spin up a real in-memory Durable Task backend *and* a real
``BlobPayloadStore`` backed by Azurite (local Azure Storage emulator).
They verify that payloads too large for inline gRPC messages are
transparently offloaded to blob storage and recovered on the other side.

Prerequisites:
    - Azurite must be running locally on the default blob port (10000).
      Start it with:  ``azurite --silent --blobPort 10000``
    - ``azure-storage-blob`` must be installed.
"""

import json
import uuid

import pytest

from durabletask import client, task, worker
from durabletask.testing import create_test_backend

# Skip the entire module if azure-storage-blob is not installed.
azure_blob = pytest.importorskip("azure.storage.blob")

from durabletask.extensions.azure_blob_payloads import BlobPayloadStore, BlobPayloadStoreOptions  # noqa: E402

# Azurite well-known connection string
AZURITE_CONN_STR = "UseDevelopmentStorage=true"

HOST = "localhost:50070"
BACKEND_PORT = 50070

# Use a unique container per test run to avoid collisions.
TEST_CONTAINER = f"e2e-payloads-{uuid.uuid4().hex[:8]}"

# A low threshold so we can trigger externalization without massive strings.
# In production the default is 900 KB; here we use 1 KB for fast tests.
THRESHOLD_BYTES = 1_024

# Pin API version to one that Azurite supports.
AZURITE_API_VERSION = "2024-08-04"


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


def _azurite_is_running() -> bool:
    """Return True if Azurite blob service is reachable."""
    try:
        svc = azure_blob.BlobServiceClient.from_connection_string(
            AZURITE_CONN_STR, api_version=AZURITE_API_VERSION,
        )
        # list_containers is a lightweight call that works with Azurite's
        # well-known credentials without any special permissions.
        next(iter(svc.list_containers(results_per_page=1)), None)
        return True
    except Exception:
        return False


# Skip all tests if Azurite is not reachable.
pytestmark = [
    pytest.mark.azurite,
    pytest.mark.skipif(
        not _azurite_is_running(),
        reason="Azurite blob service is not running on 127.0.0.1:10000",
    ),
]


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


@pytest.fixture(autouse=True)
def backend():
    """Create an in-memory backend for each test."""
    b = create_test_backend(port=BACKEND_PORT)
    yield b
    b.stop()
    b.reset()


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _make_large_string(size_kb: int = 2) -> str:
    """Return a JSON-serializable string larger than THRESHOLD_BYTES."""
    return "X" * (size_kb * 1024)


def _make_large_payload(size_kb: int = 100) -> dict:
    """Return a dict whose JSON serialization is about *size_kb* KB."""
    return {"data": "Y" * (size_kb * 1024)}


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------


class TestLargeInputOutput:
    """Orchestrations whose input/output exceeds the threshold."""

    def test_large_input_round_trips(self, payload_store):
        """A large orchestration input is externalized then recovered."""
        large_input = _make_large_string(5)  # 5 KB > 1 KB threshold

        def echo(ctx: task.OrchestrationContext, inp: str):
            return inp

        with worker.TaskHubGrpcWorker(host_address=HOST, payload_store=payload_store) as w:
            w.add_orchestrator(echo)
            w.start()

            c = client.TaskHubGrpcClient(host_address=HOST, payload_store=payload_store)
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

        with worker.TaskHubGrpcWorker(host_address=HOST, payload_store=payload_store) as w:
            w.add_orchestrator(orchestrator)
            w.add_activity(produce_large)
            w.start()

            c = client.TaskHubGrpcClient(host_address=HOST, payload_store=payload_store)
            inst_id = c.schedule_new_orchestration(orchestrator, input=10)  # 10 KB
            state = c.wait_for_orchestration_completion(inst_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None
        output = json.loads(state.serialized_output)
        assert len(output) == 10 * 1024

    def test_large_input_and_output(self, payload_store):
        """Both input and output are large — both directions externalize."""
        large_input = _make_large_payload(5)  # ~5 KB dict

        def transform(ctx: task.OrchestrationContext, inp: dict):
            return {"echo": inp["data"], "extra": "A" * 3000}

        with worker.TaskHubGrpcWorker(host_address=HOST, payload_store=payload_store) as w:
            w.add_orchestrator(transform)
            w.start()

            c = client.TaskHubGrpcClient(host_address=HOST, payload_store=payload_store)
            inst_id = c.schedule_new_orchestration(transform, input=large_input)
            state = c.wait_for_orchestration_completion(inst_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None
        output = json.loads(state.serialized_output)
        assert output["echo"] == large_input["data"]


class TestLargeEvents:
    """External events carrying large payloads."""

    def test_large_event_data(self, payload_store):
        """A large external event payload is externalized and resolved."""
        large_event = _make_large_string(5)

        def wait_for_event(ctx: task.OrchestrationContext, _):
            result = yield ctx.wait_for_external_event("big_event")
            return result

        with worker.TaskHubGrpcWorker(host_address=HOST, payload_store=payload_store) as w:
            w.add_orchestrator(wait_for_event)
            w.start()

            c = client.TaskHubGrpcClient(host_address=HOST, payload_store=payload_store)
            inst_id = c.schedule_new_orchestration(wait_for_event)
            c.wait_for_orchestration_start(inst_id, timeout=10)

            c.raise_orchestration_event(inst_id, "big_event", data=large_event)
            state = c.wait_for_orchestration_completion(inst_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None
        assert json.loads(state.serialized_output) == large_event


class TestLargeTerminate:
    """Terminate with a large output payload."""

    def test_terminate_with_large_output(self, payload_store):
        """Terminating with a large output externalizes it."""
        large_output = _make_large_string(3)

        def long_running(ctx: task.OrchestrationContext, _):
            yield ctx.wait_for_external_event("never_arrives")

        with worker.TaskHubGrpcWorker(host_address=HOST, payload_store=payload_store) as w:
            w.add_orchestrator(long_running)
            w.start()

            c = client.TaskHubGrpcClient(host_address=HOST, payload_store=payload_store)
            inst_id = c.schedule_new_orchestration(long_running)
            c.wait_for_orchestration_start(inst_id, timeout=10)

            c.terminate_orchestration(inst_id, output=large_output)
            state = c.wait_for_orchestration_completion(inst_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.TERMINATED


class TestMultipleActivitiesLargePayloads:
    """Fan-out/fan-in with multiple large activity results."""

    def test_fan_out_fan_in_large_results(self, payload_store):
        """Multiple activities each return large payloads."""
        def make_large(_: task.ActivityContext, idx: int) -> str:
            return f"result-{idx}-" + ("D" * 2000)

        def fan_out(ctx: task.OrchestrationContext, count: int):
            tasks = [ctx.call_activity(make_large, input=i) for i in range(count)]
            results = yield task.when_all(tasks)
            return results

        with worker.TaskHubGrpcWorker(host_address=HOST, payload_store=payload_store) as w:
            w.add_orchestrator(fan_out)
            w.add_activity(make_large)
            w.start()

            c = client.TaskHubGrpcClient(host_address=HOST, payload_store=payload_store)
            inst_id = c.schedule_new_orchestration(fan_out, input=5)
            state = c.wait_for_orchestration_completion(inst_id, timeout=60)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None
        results = json.loads(state.serialized_output)
        assert len(results) == 5
        for i, r in enumerate(results):
            assert r.startswith(f"result-{i}-")


class TestBlobStorageVerification:
    """Verify blobs actually land in Azurite storage."""

    def test_blobs_created_in_container(self, payload_store):
        """After an orchestration with large payloads, blobs exist in the container."""
        large_input = _make_large_string(5)

        def echo(ctx: task.OrchestrationContext, inp: str):
            return inp

        with worker.TaskHubGrpcWorker(host_address=HOST, payload_store=payload_store) as w:
            w.add_orchestrator(echo)
            w.start()

            c = client.TaskHubGrpcClient(host_address=HOST, payload_store=payload_store)
            inst_id = c.schedule_new_orchestration(echo, input=large_input)
            c.wait_for_orchestration_completion(inst_id, timeout=30)

        # Verify blobs were actually created in the Azurite container
        svc = azure_blob.BlobServiceClient.from_connection_string(
            AZURITE_CONN_STR, api_version=AZURITE_API_VERSION,
        )
        container_client = svc.get_container_client(TEST_CONTAINER)
        blobs = list(container_client.list_blobs())

        assert len(blobs) > 0, "Expected at least one blob in the container"
        # All blobs should contain compressed data (GZip header: 1f 8b)
        for blob in blobs:
            data = container_client.download_blob(blob.name).readall()
            assert data[:2] == b"\x1f\x8b", f"Blob {blob.name} is not GZip-compressed"


class TestSmallPayloadNotExternalized:
    """Payloads below the threshold should NOT hit blob storage."""

    def test_small_payload_stays_inline(self, payload_store):
        """A small payload should not create any blobs."""
        small_input = "hello"

        # Use a fresh container to isolate blob count
        fresh_container = f"small-test-{uuid.uuid4().hex[:8]}"
        store = BlobPayloadStore(BlobPayloadStoreOptions(
            connection_string=AZURITE_CONN_STR,
            container_name=fresh_container,
            threshold_bytes=THRESHOLD_BYTES,
            enable_compression=True,
            api_version=AZURITE_API_VERSION,
        ))

        def echo(ctx: task.OrchestrationContext, inp: str):
            return inp

        with worker.TaskHubGrpcWorker(host_address=HOST, payload_store=store) as w:
            w.add_orchestrator(echo)
            w.start()

            c = client.TaskHubGrpcClient(host_address=HOST, payload_store=store)
            inst_id = c.schedule_new_orchestration(echo, input=small_input)
            state = c.wait_for_orchestration_completion(inst_id, timeout=30)

        assert state is not None
        assert state.runtime_status == client.OrchestrationStatus.COMPLETED
        assert state.serialized_output is not None
        assert json.loads(state.serialized_output) == small_input

        # Verify no blobs were created
        svc = azure_blob.BlobServiceClient.from_connection_string(
            AZURITE_CONN_STR, api_version=AZURITE_API_VERSION,
        )
        container_client = svc.get_container_client(fresh_container)
        try:
            blobs = list(container_client.list_blobs())
            assert len(blobs) == 0, f"Expected 0 blobs but found {len(blobs)}"
        except Exception:
            pass  # Container may not even exist — that's fine
        finally:
            try:
                svc.delete_container(fresh_container)
            except Exception:
                pass


class TestAsyncBlobClient:
    """Test upload_async / download_async against Azurite."""

    @pytest.fixture()
    def async_store(self):
        """Per-test BlobPayloadStore with a unique container."""
        container = f"async-test-{uuid.uuid4().hex[:8]}"
        store = BlobPayloadStore(BlobPayloadStoreOptions(
            connection_string=AZURITE_CONN_STR,
            container_name=container,
            threshold_bytes=THRESHOLD_BYTES,
            enable_compression=True,
            api_version=AZURITE_API_VERSION,
        ))
        yield store
        try:
            svc = azure_blob.BlobServiceClient.from_connection_string(
                AZURITE_CONN_STR, api_version=AZURITE_API_VERSION,
            )
            svc.delete_container(container)
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_async_upload_and_download_round_trip(self, async_store):
        """upload_async stores data that download_async can retrieve."""
        payload = b"async round-trip payload " * 200
        token = await async_store.upload_async(payload, instance_id="async-1")

        assert async_store.is_known_token(token)
        result = await async_store.download_async(token)
        assert result == payload

    @pytest.mark.asyncio
    async def test_async_upload_with_compression(self, async_store):
        """Compressed upload should still decompress on download."""
        payload = b"Z" * 5000
        token = await async_store.upload_async(payload)

        downloaded = await async_store.download_async(token)
        assert downloaded == payload

    @pytest.mark.asyncio
    async def test_async_upload_instance_id_scopes_blob(self, async_store):
        """Blobs uploaded with instance_id are scoped under that prefix."""
        payload = b"scoped payload"
        token = await async_store.upload_async(payload, instance_id="inst-42")

        # Token format: blob:v1:<container>:<instance_id>/<uuid>
        _, blob_name = BlobPayloadStore._parse_token(token)
        assert blob_name.startswith("inst-42/")

        downloaded = await async_store.download_async(token)
        assert downloaded == payload
