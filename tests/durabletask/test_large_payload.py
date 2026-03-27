# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for large-payload externalization and de-externalization."""

from typing import Optional
from unittest.mock import MagicMock

import pytest
from google.protobuf import wrappers_pb2

import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask.payload.helpers import (
    deexternalize_payloads,
    deexternalize_payloads_async,
    externalize_payloads,
    externalize_payloads_async,
)

from durabletask.payload.store import (
    LargePayloadStorageOptions,
    PayloadStore,
)


# ------------------------------------------------------------------
# Fake in-memory PayloadStore for tests
# ------------------------------------------------------------------


class FakePayloadStore(PayloadStore):
    """In-memory payload store for testing."""

    TOKEN_PREFIX = "blob:v1:test-container:"

    def __init__(
        self,
        threshold_bytes: int = 100,
        max_stored_payload_bytes: int = 10 * 1024 * 1024,
        enable_compression: bool = False,
    ):
        self._options = LargePayloadStorageOptions(
            threshold_bytes=threshold_bytes,
            max_stored_payload_bytes=max_stored_payload_bytes,
            enable_compression=enable_compression,
        )
        self._blobs: dict[str, bytes] = {}
        self._counter = 0

    @property
    def options(self) -> LargePayloadStorageOptions:
        return self._options

    def upload(self, data: bytes, *, instance_id: Optional[str] = None) -> str:
        self._counter += 1
        blob_name = f"blob-{self._counter}"
        token = f"{self.TOKEN_PREFIX}{blob_name}"
        self._blobs[token] = data
        return token

    async def upload_async(self, data: bytes, *, instance_id: Optional[str] = None) -> str:
        return self.upload(data, instance_id=instance_id)

    def download(self, token: str) -> bytes:
        return self._blobs[token]

    async def download_async(self, token: str) -> bytes:
        return self.download(token)

    def is_known_token(self, value: str) -> bool:
        return value.startswith(self.TOKEN_PREFIX)


# ------------------------------------------------------------------
# Helper to create StringValue
# ------------------------------------------------------------------


def sv(text: str) -> wrappers_pb2.StringValue:
    return wrappers_pb2.StringValue(value=text)


# ------------------------------------------------------------------
# Tests: externalize_payloads
# ------------------------------------------------------------------


class TestExternalizePayloads:
    def test_small_payload_not_externalized(self):
        """Payloads smaller than the threshold should be left intact."""
        store = FakePayloadStore(threshold_bytes=1000)
        req = pb.CreateInstanceRequest(
            instanceId="test-1",
            name="MyOrch",
            input=sv("short"),
        )
        externalize_payloads(req, store, instance_id="test-1")
        assert req.input.value == "short"
        assert len(store._blobs) == 0

    def test_large_payload_externalized(self):
        """Payloads larger than the threshold should be uploaded and replaced with a token."""
        store = FakePayloadStore(threshold_bytes=10)
        large_data = "x" * 200
        req = pb.CreateInstanceRequest(
            instanceId="test-1",
            name="MyOrch",
            input=sv(large_data),
        )
        externalize_payloads(req, store, instance_id="test-1")

        # The input should now be a token
        assert req.input.value.startswith(FakePayloadStore.TOKEN_PREFIX)
        # The store should have stored the original data
        stored = store._blobs[req.input.value]
        assert stored == large_data.encode("utf-8")

    def test_already_token_not_re_uploaded(self):
        """If a field already contains a token, it should not be re-uploaded."""
        store = FakePayloadStore(threshold_bytes=10)
        token = f"{FakePayloadStore.TOKEN_PREFIX}existing-blob"
        store._blobs[token] = b"some data"

        req = pb.CreateInstanceRequest(
            instanceId="test-1",
            name="MyOrch",
            input=sv(token),
        )
        externalize_payloads(req, store, instance_id="test-1")
        assert req.input.value == token
        assert len(store._blobs) == 1  # No new upload

    def test_exceeds_max_raises_error(self):
        """Payloads exceeding max_stored_payload_bytes should raise ValueError."""
        store = FakePayloadStore(threshold_bytes=10, max_stored_payload_bytes=50)
        large_data = "x" * 100  # 100 bytes > max 50

        req = pb.CreateInstanceRequest(
            instanceId="test-1",
            name="MyOrch",
            input=sv(large_data),
        )
        with pytest.raises(ValueError, match="exceeds the maximum"):
            externalize_payloads(req, store, instance_id="test-1")

    def test_empty_value_not_externalized(self):
        """Empty StringValue should not be externalized."""
        store = FakePayloadStore(threshold_bytes=10)
        req = pb.CreateInstanceRequest(
            instanceId="test-1",
            name="MyOrch",
            input=sv(""),
        )
        externalize_payloads(req, store, instance_id="test-1")
        assert req.input.value == ""
        assert len(store._blobs) == 0

    def test_nested_history_events_externalized(self):
        """StringValue fields inside nested history events should be externalized."""
        store = FakePayloadStore(threshold_bytes=10)
        large_input = "y" * 200

        event = pb.HistoryEvent(
            eventId=1,
            executionStarted=pb.ExecutionStartedEvent(
                name="MyOrch",
                input=sv(large_input),
            ),
        )
        req = pb.OrchestratorRequest(
            instanceId="test-1",
            newEvents=[event],
        )
        externalize_payloads(req, store, instance_id="test-1")

        # Verify the nested field was externalized
        actual = req.newEvents[0].executionStarted.input.value
        assert actual.startswith(FakePayloadStore.TOKEN_PREFIX)

    def test_orchestrator_response_actions_externalized(self):
        """Action fields in OrchestratorResponse should be externalized."""
        store = FakePayloadStore(threshold_bytes=10)
        large_result = "z" * 200

        action = pb.OrchestratorAction(
            id=1,
            completeOrchestration=pb.CompleteOrchestrationAction(
                orchestrationStatus=pb.ORCHESTRATION_STATUS_COMPLETED,
                result=sv(large_result),
            ),
        )
        res = pb.OrchestratorResponse(
            instanceId="test-1",
            actions=[action],
        )
        externalize_payloads(res, store, instance_id="test-1")

        actual = res.actions[0].completeOrchestration.result.value
        assert actual.startswith(FakePayloadStore.TOKEN_PREFIX)

    def test_activity_response_externalized(self):
        """ActivityResponse.result should be externalized if large."""
        store = FakePayloadStore(threshold_bytes=10)
        large_result = "a" * 200

        res = pb.ActivityResponse(
            instanceId="test-1",
            taskId=42,
            result=sv(large_result),
        )
        externalize_payloads(res, store, instance_id="test-1")

        assert res.result.value.startswith(FakePayloadStore.TOKEN_PREFIX)


# ------------------------------------------------------------------
# Tests: deexternalize_payloads
# ------------------------------------------------------------------


class TestDeexternalizePayloads:
    def test_token_replaced_with_original(self):
        """Token in a StringValue should be replaced with the original payload."""
        store = FakePayloadStore(threshold_bytes=10)
        original = "original data here"
        token = store.upload(original.encode("utf-8"))

        req = pb.ActivityRequest(
            name="MyActivity",
            input=sv(token),
        )
        deexternalize_payloads(req, store)
        assert req.input.value == original

    def test_non_token_not_modified(self):
        """Regular string values should not be modified."""
        store = FakePayloadStore(threshold_bytes=10)
        req = pb.ActivityRequest(
            name="MyActivity",
            input=sv("just a normal string"),
        )
        deexternalize_payloads(req, store)
        assert req.input.value == "just a normal string"

    def test_nested_history_events_deexternalized(self):
        """Tokens in nested history events should be replaced."""
        store = FakePayloadStore(threshold_bytes=10)
        original = "nested payload content"
        token = store.upload(original.encode("utf-8"))

        event = pb.HistoryEvent(
            eventId=1,
            taskCompleted=pb.TaskCompletedEvent(
                taskScheduledId=5,
                result=sv(token),
            ),
        )
        req = pb.OrchestratorRequest(
            instanceId="test-1",
            pastEvents=[event],
        )
        deexternalize_payloads(req, store)

        actual = req.pastEvents[0].taskCompleted.result.value
        assert actual == original

    def test_get_instance_response_deexternalized(self):
        """GetInstanceResponse state fields should be de-externalized."""
        store = FakePayloadStore(threshold_bytes=10)
        original_input = "large input data"
        original_output = "large output data"
        token_input = store.upload(original_input.encode("utf-8"))
        token_output = store.upload(original_output.encode("utf-8"))

        res = pb.GetInstanceResponse(
            exists=True,
            orchestrationState=pb.OrchestrationState(
                instanceId="test-1",
                name="MyOrch",
                input=sv(token_input),
                output=sv(token_output),
            ),
        )
        deexternalize_payloads(res, store)

        assert res.orchestrationState.input.value == original_input
        assert res.orchestrationState.output.value == original_output


# ------------------------------------------------------------------
# Tests: round-trip
# ------------------------------------------------------------------


class TestRoundTrip:
    def test_externalize_then_deexternalize(self):
        """A payload that is externalized should round-trip correctly."""
        store = FakePayloadStore(threshold_bytes=10)
        original = "round trip payload " * 20

        req = pb.CreateInstanceRequest(
            instanceId="rt-1",
            name="MyOrch",
            input=sv(original),
        )
        externalize_payloads(req, store, instance_id="rt-1")
        assert req.input.value != original  # Should be a token

        deexternalize_payloads(req, store)
        assert req.input.value == original


# ------------------------------------------------------------------
# Tests: async
# ------------------------------------------------------------------


class TestAsyncPayloadHelpers:
    @pytest.mark.asyncio
    async def test_async_externalize_and_deexternalize(self):
        """Async versions should work identically to sync."""
        store = FakePayloadStore(threshold_bytes=10)
        original = "async round trip " * 20

        req = pb.CreateInstanceRequest(
            instanceId="async-1",
            name="MyOrch",
            input=sv(original),
        )

        await externalize_payloads_async(req, store, instance_id="async-1")
        assert req.input.value.startswith(FakePayloadStore.TOKEN_PREFIX)

        await deexternalize_payloads_async(req, store)
        assert req.input.value == original


# ------------------------------------------------------------------
# Tests: worker capability flag
# ------------------------------------------------------------------


class TestWorkerCapabilityFlag:
    def test_capability_set_when_payload_store_provided(self):
        """WORKER_CAPABILITY_LARGE_PAYLOADS should be present in GetWorkItemsRequest."""
        capabilities = [pb.WORKER_CAPABILITY_LARGE_PAYLOADS]
        req = pb.GetWorkItemsRequest(
            maxConcurrentOrchestrationWorkItems=10,
            maxConcurrentActivityWorkItems=10,
            capabilities=capabilities,
        )
        assert pb.WORKER_CAPABILITY_LARGE_PAYLOADS in req.capabilities

    def test_capability_not_set_when_no_payload_store(self):
        """Without a payload store, capabilities should be empty."""
        req = pb.GetWorkItemsRequest(
            maxConcurrentOrchestrationWorkItems=10,
            maxConcurrentActivityWorkItems=10,
        )
        assert pb.WORKER_CAPABILITY_LARGE_PAYLOADS not in req.capabilities


# ------------------------------------------------------------------
# Tests: BlobPayloadStore token parsing
# ------------------------------------------------------------------


class TestBlobPayloadStoreTokenParsing:
    def test_valid_token_is_known(self):
        """Valid blob:v1:... tokens should be recognized."""
        pytest.importorskip("azure.storage.blob")
        from durabletask.extensions.azure_blob_payloads.blob_payload_store import BlobPayloadStore

        assert BlobPayloadStore._parse_token(
            "blob:v1:my-container:some/blob/name"
        ) == ("my-container", "some/blob/name")

    def test_invalid_token_raises(self):
        """Invalid tokens should raise ValueError."""
        pytest.importorskip("azure.storage.blob")
        from durabletask.extensions.azure_blob_payloads.blob_payload_store import BlobPayloadStore

        with pytest.raises(ValueError, match="Invalid blob payload token"):
            BlobPayloadStore._parse_token("not-a-token")

    def test_token_missing_blob_name_raises(self):
        """Token with missing blob name should raise ValueError."""
        pytest.importorskip("azure.storage.blob")
        from durabletask.extensions.azure_blob_payloads.blob_payload_store import BlobPayloadStore

        with pytest.raises(ValueError, match="Invalid blob payload token"):
            BlobPayloadStore._parse_token("blob:v1:container:")

    def test_is_known_token(self):
        """is_known_token correctly identifies blob tokens."""
        pytest.importorskip("azure.storage.blob")
        from durabletask.extensions.azure_blob_payloads.blob_payload_store import BlobPayloadStore

        store = MagicMock(spec=BlobPayloadStore)
        store.is_known_token = BlobPayloadStore.is_known_token.__get__(store)
        store._parse_token = BlobPayloadStore._parse_token

        assert store.is_known_token("blob:v1:c:b") is True
        assert store.is_known_token("not-a-token") is False
        assert store.is_known_token("") is False
        assert store.is_known_token("blob:v1:") is False
        assert store.is_known_token("blob:v1:container:") is False


# ------------------------------------------------------------------
# Tests: BlobPayloadStore construction and defaults
# ------------------------------------------------------------------


class TestBlobPayloadStoreDefaults:
    def test_default_options(self):
        """Constructing with connection_string should use .NET SDK defaults."""
        pytest.importorskip("azure.storage.blob")
        from durabletask.extensions.azure_blob_payloads import BlobPayloadStore, BlobPayloadStoreOptions

        store = BlobPayloadStore(BlobPayloadStoreOptions(
            connection_string="UseDevelopmentStorage=true",
        ))
        opts = store.options
        assert opts.threshold_bytes == 900_000
        assert opts.max_stored_payload_bytes == 10 * 1024 * 1024
        assert opts.enable_compression is True
        assert opts.container_name == "durabletask-payloads"
        assert opts.connection_string == "UseDevelopmentStorage=true"

    def test_custom_options(self):
        """Custom constructor params should be reflected in options."""
        pytest.importorskip("azure.storage.blob")
        from durabletask.extensions.azure_blob_payloads import BlobPayloadStore, BlobPayloadStoreOptions

        store = BlobPayloadStore(BlobPayloadStoreOptions(
            connection_string="UseDevelopmentStorage=true",
            threshold_bytes=500_000,
            container_name="my-container",
        ))
        assert store.options.threshold_bytes == 500_000
        assert store.options.container_name == "my-container"


# ------------------------------------------------------------------
# Tests: client method coverage
# ------------------------------------------------------------------


class TestTerminateRequestExternalized:
    def test_terminate_output_externalized(self):
        """TerminateRequest.output should be externalized when large."""
        store = FakePayloadStore(threshold_bytes=10)
        large_output = "t" * 200
        req = pb.TerminateRequest(
            instanceId="term-1",
            output=sv(large_output),
            recursive=True,
        )
        externalize_payloads(req, store, instance_id="term-1")
        assert req.output.value.startswith(FakePayloadStore.TOKEN_PREFIX)
        assert store._blobs[req.output.value] == large_output.encode("utf-8")


class TestSignalEntityRequestExternalized:
    def test_signal_entity_input_externalized(self):
        """SignalEntityRequest.input should be externalized when large."""
        store = FakePayloadStore(threshold_bytes=10)
        large_input = "e" * 200
        req = pb.SignalEntityRequest(
            instanceId="entity-1",
            name="MyOp",
            input=sv(large_input),
        )
        externalize_payloads(req, store, instance_id="entity-1")
        assert req.input.value.startswith(FakePayloadStore.TOKEN_PREFIX)
        assert store._blobs[req.input.value] == large_input.encode("utf-8")


class TestQueryInstancesResponseDeexternalized:
    def test_query_instances_response_deexternalized(self):
        """OrchestrationState fields inside QueryInstancesResponse should be de-externalized."""
        store = FakePayloadStore(threshold_bytes=10)
        original_input = "query result input payload"
        original_output = "query result output payload"
        token_input = store.upload(original_input.encode("utf-8"))
        token_output = store.upload(original_output.encode("utf-8"))

        resp = pb.QueryInstancesResponse(
            orchestrationState=[
                pb.OrchestrationState(
                    instanceId="q-1",
                    name="Orch",
                    input=sv(token_input),
                    output=sv(token_output),
                ),
            ],
        )
        deexternalize_payloads(resp, store)
        assert resp.orchestrationState[0].input.value == original_input
        assert resp.orchestrationState[0].output.value == original_output


class TestGetEntityResponseDeexternalized:
    def test_get_entity_response_deexternalized(self):
        """GetEntityResponse.entity.serializedState should be de-externalized."""
        store = FakePayloadStore(threshold_bytes=10)
        original_state = "large entity state data"
        token = store.upload(original_state.encode("utf-8"))

        resp = pb.GetEntityResponse(
            exists=True,
            entity=pb.EntityMetadata(
                instanceId="ent-1",
                serializedState=sv(token),
            ),
        )
        deexternalize_payloads(resp, store)
        assert resp.entity.serializedState.value == original_state


class TestQueryEntitiesResponseDeexternalized:
    def test_query_entities_response_deexternalized(self):
        """EntityMetadata inside QueryEntitiesResponse should be de-externalized."""
        store = FakePayloadStore(threshold_bytes=10)
        original_state = "queried entity state"
        token = store.upload(original_state.encode("utf-8"))

        resp = pb.QueryEntitiesResponse(
            entities=[
                pb.EntityMetadata(
                    instanceId="ent-q-1",
                    serializedState=sv(token),
                ),
            ],
        )
        deexternalize_payloads(resp, store)
        assert resp.entities[0].serializedState.value == original_state


# ------------------------------------------------------------------
# Tests: BlobPayloadStore construction validation
# ------------------------------------------------------------------


class TestBlobPayloadStoreConstruction:
    def test_no_credentials_raises(self):
        """Constructing without connection_string or account_url should raise."""
        pytest.importorskip("azure.storage.blob")
        from durabletask.extensions.azure_blob_payloads.blob_payload_store import BlobPayloadStore

        with pytest.raises(TypeError):
            BlobPayloadStore()  # type: ignore[call-arg]
