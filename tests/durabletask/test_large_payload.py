# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for large-payload externalization and de-externalization."""

import asyncio
import gzip
import threading
from unittest.mock import AsyncMock, MagicMock

import pytest
from google.protobuf import wrappers_pb2

import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask.internal.exceptions import PayloadSizeExceededError
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

    def upload(self, data: bytes, *, instance_id: str | None = None) -> str:
        self._counter += 1
        blob_name = f"blob-{self._counter}"
        token = f"{self.TOKEN_PREFIX}{blob_name}"
        self._blobs[token] = data
        return token

    async def upload_async(self, data: bytes, *, instance_id: str | None = None) -> str:
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


@pytest.fixture(params=["sync", "async"])
def externalize(request):
    def run(msg, store, *, instance_id=None):
        if request.param == "async":
            asyncio.run(externalize_payloads_async(msg, store, instance_id=instance_id))
        else:
            externalize_payloads(msg, store, instance_id=instance_id)
    return run


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
        with pytest.raises(PayloadSizeExceededError, match="exceeds the maximum") as failure:
            externalize_payloads(req, store, instance_id="test-1")
        assert isinstance(failure.value, ValueError)

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

    def test_send_event_action_data_externalized(self):
        """Large orchestration-sent event payloads should be externalized."""
        store = FakePayloadStore(threshold_bytes=10)
        large_data = "e" * 200

        action = pb.OrchestratorAction(
            id=1,
            sendEvent=pb.SendEventAction(
                instance=pb.OrchestrationInstance(instanceId="target"),
                name="Approval",
                data=sv(large_data),
            ),
        )
        res = pb.OrchestratorResponse(
            instanceId="source",
            actions=[action],
        )
        externalize_payloads(res, store, instance_id="source")

        actual = res.actions[0].sendEvent.data.value
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


class TestExternalizePreflight:
    @pytest.mark.parametrize("message_kind", ["actions", "carryover", "entity-results", "entity-state"])
    @pytest.mark.parametrize("upload_fails", [False, True])
    def test_later_oversized_payload_has_no_side_effects(self, externalize, message_kind, upload_fails):
        store = FakePayloadStore(threshold_bytes=8, max_stored_payload_bytes=32)
        if message_kind == "actions":
            response = pb.OrchestratorResponse(
                instanceId="size-preflight",
                actions=[
                    pb.OrchestratorAction(id=1, scheduleTask=pb.ScheduleTaskAction(input=sv("a" * 20))),
                    pb.OrchestratorAction(id=2, scheduleTask=pb.ScheduleTaskAction(input=sv("b" * 33))),
                ],
            )
        elif message_kind == "carryover":
            response = pb.OrchestratorResponse(
                instanceId="size-preflight",
                actions=[pb.OrchestratorAction(
                    id=1,
                    completeOrchestration=pb.CompleteOrchestrationAction(
                        result=sv("a" * 20),
                        carryoverEvents=[pb.HistoryEvent(
                            eventId=2,
                            eventRaised=pb.EventRaisedEvent(name="event", input=sv("b" * 33)),
                        )],
                    ),
                )],
            )
        elif message_kind == "entity-results":
            response = pb.EntityBatchResult(results=[
                pb.OperationResult(success=pb.OperationResultSuccess(result=sv("a" * 20))),
                pb.OperationResult(success=pb.OperationResultSuccess(result=sv("b" * 33))),
            ])
        else:
            response = pb.EntityBatchResult(
                results=[pb.OperationResult(success=pb.OperationResultSuccess(result=sv("a" * 20)))],
                actions=[pb.OperationAction(sendSignal=pb.SendSignalAction(input=sv("a" * 20)))],
                entityState=sv("b" * 33),
            )
        before = response.SerializeToString()
        error = OSError("must not upload") if upload_fails else None
        store.upload = MagicMock(wraps=store.upload, side_effect=error)
        store.upload_async = AsyncMock(wraps=store.upload_async, side_effect=error)

        with pytest.raises(ValueError, match="Payload size 33 bytes exceeds the maximum allowed size of 32 bytes"):
            externalize(response, store, instance_id="size-preflight")

        store.upload.assert_not_called()
        store.upload_async.assert_not_called()
        assert not store._blobs
        assert response.SerializeToString() == before

    @pytest.mark.parametrize("payload", ["a" * 33, "\u00e9" * 16 + "a", "\U0001f600" * 8 + "a"])
    def test_utf8_maximum_uses_bytes(self, externalize, payload):
        store = FakePayloadStore(threshold_bytes=8, max_stored_payload_bytes=32)
        response = pb.OrchestrationState(input=sv("a" * 20), output=sv(payload))
        before = response.SerializeToString()

        with pytest.raises(ValueError, match="Payload size 33 bytes exceeds the maximum"):
            externalize(response, store)

        assert not store._blobs
        assert response.SerializeToString() == before

    @pytest.mark.parametrize("payload", [
        "a" * 7, "a" * 8, "a" * 9, "a" * 32,
        "\u00e9" * 4, "\u00e9" * 4 + "a", "\u00e9" * 16, "\U0001f600" * 8,
    ])
    def test_valid_utf8_threshold_and_maximum_boundaries(self, externalize, payload):
        store = FakePayloadStore(threshold_bytes=8, max_stored_payload_bytes=32)
        response = pb.ActivityResponse(result=sv(payload))

        externalize(response, store)

        if len(payload.encode("utf-8")) > 8:
            assert store._blobs[response.result.value] == payload.encode("utf-8")
            assert len(store._blobs) == 1
        else:
            assert response.result.value == payload
            assert not store._blobs

    def test_all_valid_payloads_preserve_order_tokens_and_presence(self, externalize):
        store = FakePayloadStore(threshold_bytes=8, max_stored_payload_bytes=32)
        token = f"{FakePayloadStore.TOKEN_PREFIX}existing-blob"
        response = pb.OrchestratorResponse(actions=[
            pb.OrchestratorAction(id=1, scheduleTask=pb.ScheduleTaskAction(input=sv("a" * 20))),
            pb.OrchestratorAction(id=2, scheduleTask=pb.ScheduleTaskAction(input=sv("\u00e9" * 16))),
            pb.OrchestratorAction(id=3, scheduleTask=pb.ScheduleTaskAction(input=sv(token))),
            pb.OrchestratorAction(id=4, scheduleTask=pb.ScheduleTaskAction(input=sv(""))),
            pb.OrchestratorAction(id=5, scheduleTask=pb.ScheduleTaskAction()),
        ])
        upload = store.upload = MagicMock(wraps=store.upload)

        externalize(response, store, instance_id="size-preflight")

        assert list(store._blobs.values()) == [b"a" * 20, ("\u00e9" * 16).encode("utf-8")]
        assert [call.kwargs for call in upload.call_args_list] == [
            {"instance_id": "size-preflight"}, {"instance_id": "size-preflight"},
        ]
        assert response.actions[2].scheduleTask.input.value == token
        assert response.actions[3].scheduleTask.HasField("input")
        assert response.actions[3].scheduleTask.input.value == ""
        assert not response.actions[4].scheduleTask.HasField("input")
        before = response.SerializeToString()
        externalize(response, store)
        assert upload.call_count == 2
        assert response.SerializeToString() == before

    def test_inline_payload_above_maximum_is_still_ignored(self, externalize):
        store = FakePayloadStore(threshold_bytes=32, max_stored_payload_bytes=8)
        response = pb.ActivityResponse(result=sv("a" * 20))
        before = response.SerializeToString()

        externalize(response, store)

        assert not store._blobs
        assert response.SerializeToString() == before

    def test_uploads_use_snapshots(self, externalize):
        store = FakePayloadStore(threshold_bytes=8, max_stored_payload_bytes=32)
        response = pb.OrchestrationState(input=sv("a" * 20), output=sv("b" * 20))
        upload = store.upload

        def mutate_later_field(data, *, instance_id=None):
            response.output.value = "changed during upload"
            return upload(data, instance_id=instance_id)

        store.upload = MagicMock(side_effect=mutate_later_field)

        externalize(response, store)

        assert list(store._blobs.values()) == [b"a" * 20, b"b" * 20]
        assert store._blobs[response.output.value] == b"b" * 20

    @pytest.mark.parametrize("error", [OSError("storage failure"), ValueError("custom store failure")])
    def test_upload_errors_propagate_without_rollback(self, externalize, error):
        store = FakePayloadStore(threshold_bytes=8, max_stored_payload_bytes=32)
        response = pb.OrchestrationState(input=sv("a" * 20), output=sv("b" * 20))
        upload = store.upload

        def upload_or_fail(data, *, instance_id=None):
            if store._blobs:
                raise error
            return upload(data, instance_id=instance_id)

        store.upload = MagicMock(side_effect=upload_or_fail)

        with pytest.raises(type(error)) as raised:
            externalize(response, store)

        assert raised.value is error
        assert len(store._blobs) == 1
        assert store.is_known_token(response.input.value)
        assert response.output.value == "b" * 20
        assert store.upload.call_count == 2

    @pytest.mark.asyncio
    async def test_async_cancellation_propagates_without_later_uploads(self):
        store = FakePayloadStore(threshold_bytes=8, max_stored_payload_bytes=32)
        response = pb.OrchestrationState(input=sv("a" * 20), output=sv("b" * 20))
        before = response.SerializeToString()
        error = asyncio.CancelledError("cancelled upload")
        store.upload_async = AsyncMock(side_effect=error)

        with pytest.raises(asyncio.CancelledError) as raised:
            await externalize_payloads_async(response, store)

        assert raised.value is error
        assert store.upload_async.await_count == 1
        assert not store._blobs
        assert response.SerializeToString() == before


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

    @pytest.mark.asyncio
    async def test_async_cap_failure_is_a_value_error(self):
        store = FakePayloadStore(threshold_bytes=1, max_stored_payload_bytes=10)
        req = pb.CreateInstanceRequest(input=sv("x" * 11))
        with pytest.raises(PayloadSizeExceededError) as failure:
            await externalize_payloads_async(req, store)
        assert isinstance(failure.value, ValueError)
        assert not store._blobs

    @pytest.mark.asyncio
    @pytest.mark.parametrize("use_async", [False, True])
    async def test_token_larger_than_cap_is_not_reuploaded(self, use_async):
        store = FakePayloadStore(threshold_bytes=1, max_stored_payload_bytes=10)
        token = f"{FakePayloadStore.TOKEN_PREFIX}existing-blob"
        assert len(token.encode("utf-8")) > store.options.max_stored_payload_bytes
        req = pb.CreateInstanceRequest(input=sv(token))
        if use_async:
            await externalize_payloads_async(req, store)
        else:
            externalize_payloads(req, store)
        assert req.input.value == token
        assert not store._blobs

    @pytest.mark.asyncio
    @pytest.mark.parametrize("error", [OSError("unavailable"), ValueError("store error"), asyncio.CancelledError()])
    async def test_async_store_error_propagates_unchanged(self, error):
        store = FakePayloadStore(threshold_bytes=1)
        store.upload_async = AsyncMock(side_effect=error)
        req = pb.CreateInstanceRequest(input=sv("large enough"))
        with pytest.raises(type(error)) as failure:
            await externalize_payloads_async(req, store)
        assert failure.value is error
        assert req.input.value == "large enough"


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


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["upload", "download"])
@pytest.mark.parametrize("enable_compression", [False, True])
async def test_async_blob_compression_yields(monkeypatch, operation, enable_compression):
    pytest.importorskip("azure.storage.blob")
    from durabletask.extensions.azure_blob_payloads import blob_payload_store as module
    from durabletask.extensions.azure_blob_payloads import BlobPayloadStoreOptions

    data = b"payload" * 1000
    stored_data = gzip.compress(data) if enable_compression else data
    loop = asyncio.get_running_loop()
    loop_thread = threading.get_ident()
    entered = asyncio.Event()
    release = threading.Event()
    gzip_method = "compress" if operation == "upload" else "decompress"
    original = getattr(gzip, gzip_method)

    def gated_gzip(value):
        assert enable_compression
        assert threading.get_ident() != loop_thread
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5), "Event loop did not release compression"
        return original(value)

    async def upload_blob(**kwargs):
        assert threading.get_ident() == loop_thread

    async def readall():
        assert threading.get_ident() == loop_thread
        return stored_data

    stream = MagicMock()
    stream.readall = AsyncMock(side_effect=readall)
    container = MagicMock()
    container.create_container = AsyncMock()
    container.upload_blob = AsyncMock(side_effect=upload_blob)
    container.download_blob = AsyncMock(return_value=stream)
    service = MagicMock()
    service.get_container_client.return_value = container
    service.close = AsyncMock()
    monkeypatch.setattr(module, "BlobServiceClient", MagicMock())
    monkeypatch.setattr(module, "AsyncBlobServiceClient", MagicMock(return_value=service))
    monkeypatch.setattr(gzip, gzip_method, gated_gzip)
    store = module.BlobPayloadStore(BlobPayloadStoreOptions(
        account_url="https://example.blob.core.windows.net", enable_compression=enable_compression))
    transfer = asyncio.create_task(store.upload_async(data) if operation == "upload"
                                   else store.download_async("blob:v1:durabletask-payloads:example"))
    try:
        if enable_compression:
            await asyncio.wait_for(entered.wait(), timeout=2)
            assert not transfer.done()
    finally:
        release.set()
        try:
            result = await transfer
        finally:
            store.close()
            await store.close_async()
    if operation == "upload":
        uploaded = container.upload_blob.call_args.kwargs["data"]
        assert (gzip.decompress(uploaded) if enable_compression else uploaded) == data
        assert store.is_known_token(result)
    else:
        assert result == data


class TestBlobPayloadStoreDefaults:
    def test_default_options(self):
        """Constructing with connection_string should use .NET SDK defaults."""
        pytest.importorskip("azure.storage.blob")
        from durabletask.extensions.azure_blob_payloads import BlobPayloadStore, BlobPayloadStoreOptions

        store = BlobPayloadStore(BlobPayloadStoreOptions(
            connection_string="UseDevelopmentStorage=true",
        ))
        opts = store.options
        assert opts.threshold_bytes == 262_144
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
