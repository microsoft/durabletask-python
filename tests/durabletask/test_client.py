import json
import pytest
from unittest.mock import ANY, MagicMock, patch
from google.protobuf import wrappers_pb2

from durabletask.client import AsyncTaskHubGrpcClient, TaskHubGrpcClient
from durabletask.grpc_options import GrpcChannelOptions, GrpcRetryPolicyOptions
from datetime import datetime, timezone
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import durabletask.history as history
import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask.client import AsyncTaskHubGrpcClient, OrchestrationStatus, TaskHubGrpcClient
from durabletask.payload.store import LargePayloadStorageOptions, PayloadStore

from durabletask.internal.grpc_interceptor import (
    DefaultAsyncClientInterceptorImpl,
    DefaultClientInterceptorImpl,
)
from durabletask.internal.shared import (
    get_async_grpc_channel,
    get_default_host_address,
    get_grpc_channel,
)

HOST_ADDRESS = 'localhost:50051'
METADATA = [('key1', 'value1'), ('key2', 'value2')]
INTERCEPTORS = [DefaultClientInterceptorImpl(METADATA)]


class FakePayloadStore(PayloadStore):
    TOKEN_PREFIX = 'fake://'

    def __init__(self):
        self._options = LargePayloadStorageOptions(threshold_bytes=1, max_stored_payload_bytes=1024 * 1024)
        self._blobs: dict[str, bytes] = {}
        self._counter = 0

    @property
    def options(self) -> LargePayloadStorageOptions:
        return self._options

    def upload(self, payload: bytes, *, instance_id=None) -> str:
        self._counter += 1
        token = f'{self.TOKEN_PREFIX}{self._counter}'
        self._blobs[token] = payload
        return token

    def download(self, token: str) -> bytes:
        return self._blobs[token]

    def is_known_token(self, value: str) -> bool:
        return value.startswith(self.TOKEN_PREFIX)

    async def upload_async(self, payload: bytes, *, instance_id=None) -> str:
        return self.upload(payload, instance_id=instance_id)

    async def download_async(self, token: str) -> bytes:
        return self.download(token)


# ==== Sync channel tests ====


def test_get_grpc_channel_insecure():
    with patch('grpc.insecure_channel') as mock_channel:
        get_grpc_channel(HOST_ADDRESS, False, interceptors=INTERCEPTORS)
        mock_channel.assert_called_once_with(HOST_ADDRESS)


def test_get_grpc_channel_secure():
    with patch('grpc.secure_channel') as mock_channel, patch(
            'grpc.ssl_channel_credentials') as mock_credentials:
        get_grpc_channel(HOST_ADDRESS, True, interceptors=INTERCEPTORS)
        mock_channel.assert_called_once_with(HOST_ADDRESS, mock_credentials.return_value)


def test_get_grpc_channel_with_channel_options():
    options = GrpcChannelOptions(max_receive_message_length=1234)
    with patch('grpc.insecure_channel') as mock_channel:
        get_grpc_channel(HOST_ADDRESS, False, channel_options=options)
        mock_channel.assert_called_once_with(
            HOST_ADDRESS,
            options=[('grpc.max_receive_message_length', 1234)],
        )


def test_get_grpc_channel_with_retry_policy_service_config():
    retry = GrpcRetryPolicyOptions(max_attempts=5, retryable_status_codes=['UNAVAILABLE'])
    options = GrpcChannelOptions(retry_policy=retry)
    with patch('grpc.insecure_channel') as mock_channel:
        get_grpc_channel(HOST_ADDRESS, False, channel_options=options)
        _, kwargs = mock_channel.call_args
        channel_options = kwargs['options']
        assert ('grpc.enable_retries', 1) in channel_options
        service_config_value = next(
            option[1] for option in channel_options if option[0] == 'grpc.service_config'
        )
        service_config = json.loads(service_config_value)
        retry_policy = service_config['methodConfig'][0]['retryPolicy']
        assert retry_policy['maxAttempts'] == 5
        assert retry_policy['retryableStatusCodes'] == ['UNAVAILABLE']


def test_retry_policy_format_duration_raises_on_zero():
    with pytest.raises(ValueError, match="rounds to zero"):
        GrpcRetryPolicyOptions(
            max_attempts=2,
            initial_backoff_seconds=1e-15,
            max_backoff_seconds=1e-15,
        )


def test_get_grpc_channel_default_host_address():
    with patch('grpc.insecure_channel') as mock_channel:
        get_grpc_channel(None, False, interceptors=INTERCEPTORS)
        mock_channel.assert_called_once_with(get_default_host_address())


def test_get_grpc_channel_with_metadata():
    with patch('grpc.insecure_channel') as mock_channel, patch(
            'grpc.intercept_channel') as mock_intercept_channel:
        get_grpc_channel(HOST_ADDRESS, False, interceptors=INTERCEPTORS)
        mock_channel.assert_called_once_with(HOST_ADDRESS)
        mock_intercept_channel.assert_called_once()

        # Capture and check the arguments passed to intercept_channel()
        args, kwargs = mock_intercept_channel.call_args
        assert args[0] == mock_channel.return_value
        assert isinstance(args[1], DefaultClientInterceptorImpl)
        assert args[1]._metadata == METADATA


def test_grpc_channel_with_host_name_protocol_stripping():
    with patch('grpc.insecure_channel') as mock_insecure_channel, patch(
            'grpc.secure_channel') as mock_secure_channel:

        host_name = "myserver.com:1234"

        prefix = "grpc://"
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        mock_insecure_channel.assert_called_once_with(host_name)
        mock_insecure_channel.reset_mock()

        prefix = "http://"
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        mock_insecure_channel.assert_called_once_with(host_name)
        mock_insecure_channel.reset_mock()

        prefix = "HTTP://"
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        mock_insecure_channel.assert_called_once_with(host_name)
        mock_insecure_channel.reset_mock()

        prefix = "GRPC://"
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        mock_insecure_channel.assert_called_once_with(host_name)
        mock_insecure_channel.reset_mock()

        prefix = ""
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        mock_insecure_channel.assert_called_once_with(host_name)
        mock_insecure_channel.reset_mock()

        prefix = "grpcs://"
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        mock_secure_channel.assert_called_once_with(host_name, ANY)
        mock_secure_channel.reset_mock()

        prefix = "https://"
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        mock_secure_channel.assert_called_once_with(host_name, ANY)
        mock_secure_channel.reset_mock()

        prefix = "HTTPS://"
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        mock_secure_channel.assert_called_once_with(host_name, ANY)
        mock_secure_channel.reset_mock()

        prefix = "GRPCS://"
        get_grpc_channel(prefix + host_name, interceptors=INTERCEPTORS)
        mock_secure_channel.assert_called_once_with(host_name, ANY)
        mock_secure_channel.reset_mock()

        prefix = ""
        get_grpc_channel(prefix + host_name, True, interceptors=INTERCEPTORS)
        mock_secure_channel.assert_called_once_with(host_name, ANY)
        mock_secure_channel.reset_mock()


# ==== Async channel tests ====


def test_get_async_grpc_channel_insecure():
    with patch('grpc.aio.insecure_channel') as mock_channel:
        get_async_grpc_channel(HOST_ADDRESS, False)
        mock_channel.assert_called_once_with(HOST_ADDRESS, interceptors=None)


def test_get_async_grpc_channel_secure():
    with patch('grpc.aio.secure_channel') as mock_channel, patch(
            'grpc.ssl_channel_credentials') as mock_credentials:
        get_async_grpc_channel(HOST_ADDRESS, True)
        mock_channel.assert_called_once_with(
            HOST_ADDRESS, mock_credentials.return_value, interceptors=None)


def test_get_async_grpc_channel_default_host_address():
    with patch('grpc.aio.insecure_channel') as mock_channel:
        get_async_grpc_channel(None, False)
        mock_channel.assert_called_once_with(get_default_host_address(), interceptors=None)


def test_get_async_grpc_channel_with_interceptors():
    async_interceptors = [DefaultAsyncClientInterceptorImpl(METADATA)]
    with patch('grpc.aio.insecure_channel') as mock_channel:
        get_async_grpc_channel(HOST_ADDRESS, False, interceptors=async_interceptors)
        mock_channel.assert_called_once_with(HOST_ADDRESS, interceptors=async_interceptors)


def test_get_async_grpc_channel_with_channel_options():
    options = GrpcChannelOptions(max_send_message_length=4321)
    with patch('grpc.aio.insecure_channel') as mock_channel:
        get_async_grpc_channel(HOST_ADDRESS, False, channel_options=options)
        mock_channel.assert_called_once_with(
            HOST_ADDRESS,
            interceptors=None,
            options=[('grpc.max_send_message_length', 4321)],
        )


def test_async_grpc_channel_protocol_stripping():
    with patch('grpc.aio.insecure_channel') as mock_insecure, patch(
            'grpc.aio.secure_channel') as mock_secure:
        host_name = "myserver.com:1234"

        get_async_grpc_channel("http://" + host_name)
        mock_insecure.assert_called_once_with(host_name, interceptors=None)
        mock_insecure.reset_mock()

        get_async_grpc_channel("grpc://" + host_name)
        mock_insecure.assert_called_once_with(host_name, interceptors=None)
        mock_insecure.reset_mock()

        get_async_grpc_channel("https://" + host_name)
        mock_secure.assert_called_once_with(host_name, ANY, interceptors=None)
        mock_secure.reset_mock()

        get_async_grpc_channel("grpcs://" + host_name)
        mock_secure.assert_called_once_with(host_name, ANY, interceptors=None)
        mock_secure.reset_mock()


# ==== Async client construction tests ====


def test_async_client_creates_with_defaults():
    with patch('grpc.aio.insecure_channel') as mock_channel:
        mock_channel.return_value = MagicMock()
        _ = AsyncTaskHubGrpcClient()
        mock_channel.assert_called_once_with(
            get_default_host_address(), interceptors=None)


def test_async_client_creates_with_metadata():
    with patch('grpc.aio.insecure_channel') as mock_channel:
        mock_channel.return_value = MagicMock()
        _ = AsyncTaskHubGrpcClient(
            host_address=HOST_ADDRESS, metadata=METADATA)
        mock_channel.assert_called_once()
        args, kwargs = mock_channel.call_args
        assert args[0] == HOST_ADDRESS
        interceptors = kwargs.get('interceptors')
        assert interceptors is not None
        assert len(interceptors) == 1
        assert isinstance(interceptors[0], DefaultAsyncClientInterceptorImpl)


def test_client_uses_provided_channel_directly():
    provided_channel = MagicMock()
    with patch('durabletask.internal.shared.get_grpc_channel') as mock_get_channel:
        client = TaskHubGrpcClient(channel=provided_channel, host_address=HOST_ADDRESS)
        assert client._channel is provided_channel
        mock_get_channel.assert_not_called()


def test_async_client_uses_provided_channel_directly():
    provided_channel = MagicMock()
    with patch('durabletask.internal.shared.get_async_grpc_channel') as mock_get_channel:
        client = AsyncTaskHubGrpcClient(channel=provided_channel, host_address=HOST_ADDRESS)
        assert client._channel is provided_channel
        mock_get_channel.assert_not_called()
def test_get_orchestration_history_aggregates_chunks_and_deexternalizes_payloads():
    store = FakePayloadStore()
    token = store.upload(b'history payload')
    stream = [
        pb.HistoryChunk(events=[
            pb.HistoryEvent(
                eventId=1,
                taskCompleted=pb.TaskCompletedEvent(
                    taskScheduledId=42,
                    result=wrappers_pb2.StringValue(value=token),
                ),
            )
        ]),
        pb.HistoryChunk(events=[pb.HistoryEvent(eventId=2, executionCompleted=pb.ExecutionCompletedEvent())]),
    ]

    stub = MagicMock()
    stub.StreamInstanceHistory.return_value = stream

    with patch('durabletask.client.shared.get_grpc_channel', return_value=MagicMock()), patch(
            'durabletask.client.stubs.TaskHubSidecarServiceStub', return_value=stub):
        history_client = TaskHubGrpcClient(payload_store=store)
        events = history_client.get_orchestration_history('abc')

    assert [event.event_id for event in events] == [1, 2]
    assert isinstance(events[0], history.TaskCompletedEvent)
    assert events[0].result == 'history payload'
    req = stub.StreamInstanceHistory.call_args.args[0]
    assert req.instanceId == 'abc'


def test_list_instance_ids_returns_page():
    stub = MagicMock()
    stub.ListInstanceIds.return_value = pb.ListInstanceIdsResponse(
        instanceIds=['a', 'b'],
        lastInstanceKey=wrappers_pb2.StringValue(value='b'),
    )

    with patch('durabletask.client.shared.get_grpc_channel', return_value=MagicMock()), patch(
            'durabletask.client.stubs.TaskHubSidecarServiceStub', return_value=stub):
        history_client = TaskHubGrpcClient()
        page = history_client.list_instance_ids(
            runtime_status=[OrchestrationStatus.COMPLETED],
            completed_time_from=datetime(2025, 1, 1, tzinfo=timezone.utc),
            page_size=2,
            continuation_token='prev',
        )

    assert page.items == ['a', 'b']
    assert page.continuation_token == 'b'
    req = stub.ListInstanceIds.call_args.args[0]
    assert list(req.runtimeStatus) == [pb.ORCHESTRATION_STATUS_COMPLETED]
    assert req.pageSize == 2
    assert req.lastInstanceKey.value == 'prev'


@pytest.mark.asyncio
async def test_async_get_orchestration_history_aggregates_chunks_and_deexternalizes_payloads():
    store = FakePayloadStore()
    token = store.upload(b'async history payload')

    async def stream():
        yield pb.HistoryChunk(events=[
            pb.HistoryEvent(
                eventId=3,
                taskCompleted=pb.TaskCompletedEvent(
                    taskScheduledId=43,
                    result=wrappers_pb2.StringValue(value=token),
                ),
            )
        ])

    stub = MagicMock()
    stub.StreamInstanceHistory.return_value = stream()

    with patch('durabletask.client.shared.get_async_grpc_channel', return_value=MagicMock()), patch(
            'durabletask.client.stubs.TaskHubSidecarServiceStub', return_value=stub):
        history_client = AsyncTaskHubGrpcClient(payload_store=store)
        events = await history_client.get_orchestration_history('async-abc')

    assert [event.event_id for event in events] == [3]
    assert isinstance(events[0], history.TaskCompletedEvent)
    assert events[0].result == 'async history payload'


@pytest.mark.asyncio
async def test_async_list_instance_ids_returns_page():
    stub = MagicMock()
    stub.ListInstanceIds = AsyncMock(return_value=pb.ListInstanceIdsResponse(
        instanceIds=['one'],
        lastInstanceKey=wrappers_pb2.StringValue(value='one'),
    ))

    with patch('durabletask.client.shared.get_async_grpc_channel', return_value=MagicMock()), patch(
            'durabletask.client.stubs.TaskHubSidecarServiceStub', return_value=stub):
        history_client = AsyncTaskHubGrpcClient()
        page = await history_client.list_instance_ids(page_size=1)

    assert page.items == ['one']
    assert page.continuation_token == 'one'
