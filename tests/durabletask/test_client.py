import asyncio
import json
import grpc
import pytest
from datetime import datetime, timezone
from unittest.mock import ANY, AsyncMock, MagicMock, call, patch

from google.protobuf import wrappers_pb2

import durabletask.history as history
import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask.client import AsyncTaskHubGrpcClient, OrchestrationStatus, TaskHubGrpcClient
from durabletask.grpc_options import (
    GrpcChannelOptions,
    GrpcClientResiliencyOptions,
    GrpcRetryPolicyOptions,
    GrpcWorkerResiliencyOptions,
)
from durabletask.payload.store import LargePayloadStorageOptions, PayloadStore
from durabletask.worker import TaskHubGrpcWorker

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


class FakeRpcError(grpc.RpcError):
    def __init__(self, status_code: grpc.StatusCode):
        super().__init__()
        self._status_code = status_code

    def code(self):
        return self._status_code


def make_aio_rpc_error(status_code: grpc.StatusCode) -> grpc.aio.AioRpcError:
    return grpc.aio.AioRpcError(
        status_code,
        grpc.aio.Metadata(),
        grpc.aio.Metadata(),
    )


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


def test_client_stores_resiliency_options_for_recreation():
    resiliency = GrpcClientResiliencyOptions(channel_recreate_failure_threshold=7)
    channel_options = GrpcChannelOptions(max_receive_message_length=1234)
    interceptors = [DefaultClientInterceptorImpl(METADATA)]
    with patch("durabletask.client.shared.get_grpc_channel", return_value=MagicMock()), patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=MagicMock()
    ):
        client = TaskHubGrpcClient(
            host_address="localhost:4001",
            secure_channel=True,
            interceptors=interceptors,
            channel_options=channel_options,
            resiliency_options=resiliency,
        )
    assert client._resiliency_options is resiliency
    assert client._host_address == "localhost:4001"
    assert client._secure_channel is True
    assert client._channel_options is channel_options
    assert client._interceptors == interceptors


def test_sync_client_recreates_sdk_owned_channel_with_original_transport_inputs():
    first_channel = MagicMock(name="first-channel")
    second_channel = MagicMock(name="second-channel")
    first_stub = MagicMock()
    second_stub = MagicMock()
    second_stub.GetInstance.return_value = MagicMock(exists=False)
    host_address = "localhost:4001"
    interceptors = [DefaultClientInterceptorImpl(METADATA)]
    channel_options = GrpcChannelOptions(max_receive_message_length=1234)

    rpc_error = FakeRpcError(grpc.StatusCode.UNAVAILABLE)
    first_stub.GetInstance.side_effect = rpc_error

    timer = MagicMock()

    with patch(
            "durabletask.client.shared.get_grpc_channel",
            side_effect=[first_channel, second_channel],
    ) as mock_get_channel, patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub", side_effect=[first_stub, second_stub]
    ), patch("threading.Timer", return_value=timer) as mock_timer:
        client = TaskHubGrpcClient(
            host_address=host_address,
            secure_channel=True,
            interceptors=interceptors,
            channel_options=channel_options,
            resiliency_options=GrpcClientResiliencyOptions(
                channel_recreate_failure_threshold=1,
                min_recreate_interval_seconds=0.0,
            ),
        )
        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        client.get_orchestration_state("abc")

    expected_channel_call = call(
        host_address=host_address,
        secure_channel=True,
        interceptors=interceptors,
        channel_options=channel_options,
    )
    assert mock_get_channel.call_args_list == [
        expected_channel_call,
        expected_channel_call,
    ]
    assert client._channel is second_channel
    mock_timer.assert_called_once()
    timer_call = mock_timer.call_args
    assert timer_call.args[0] == 30.0
    assert timer_call.args[1].__self__ is client
    assert timer_call.args[1].__func__ is TaskHubGrpcClient._close_retired_channel
    assert timer_call.kwargs == {"args": (first_channel,)}
    assert timer.daemon is True
    timer.start.assert_called_once_with()


def test_sync_client_close_closes_retired_channels_immediately():
    first_channel = MagicMock(name="first-channel")
    second_channel = MagicMock(name="second-channel")
    first_stub = MagicMock()
    first_stub.GetInstance.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE)
    second_stub = MagicMock()
    second_stub.GetInstance.return_value = MagicMock(exists=False)
    close_timer = MagicMock(name="close-timer")

    with patch(
            "durabletask.client.shared.get_grpc_channel",
            side_effect=[first_channel, second_channel],
    ), patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub", side_effect=[first_stub, second_stub]
    ), patch("threading.Timer", return_value=close_timer):
        client = TaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(
                channel_recreate_failure_threshold=1,
                min_recreate_interval_seconds=0.0,
            )
        )
        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")

        client.close()

    close_timer.cancel.assert_called_once_with()
    first_channel.close.assert_called_once_with()
    second_channel.close.assert_called_once_with()
    assert client._retired_channels == {}


def test_sync_client_close_closes_all_retired_sdk_channels_immediately():
    first_channel = MagicMock(name="first-channel")
    second_channel = MagicMock(name="second-channel")
    third_channel = MagicMock(name="third-channel")
    first_stub = MagicMock()
    first_stub.GetInstance.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE)
    second_stub = MagicMock()
    second_stub.GetInstance.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE)
    third_stub = MagicMock()
    timer1 = MagicMock(name="close-timer-1")
    timer2 = MagicMock(name="close-timer-2")

    with patch(
            "durabletask.client.shared.get_grpc_channel",
            side_effect=[first_channel, second_channel, third_channel],
    ), patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub",
            side_effect=[first_stub, second_stub, third_stub],
    ), patch("threading.Timer", side_effect=[timer1, timer2]):
        client = TaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(
                channel_recreate_failure_threshold=1,
                min_recreate_interval_seconds=0.0,
            )
        )
        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")

        client.close()

    timer1.cancel.assert_called_once_with()
    timer2.cancel.assert_called_once_with()
    first_channel.close.assert_called_once_with()
    second_channel.close.assert_called_once_with()
    third_channel.close.assert_called_once_with()
    assert client._retired_channels == {}


@pytest.mark.parametrize(
    ("stub_method_name", "client_method_name"),
    [
        ("WaitForInstanceStart", "wait_for_orchestration_start"),
        ("WaitForInstanceCompletion", "wait_for_orchestration_completion"),
    ],
)
def test_sync_client_resets_failure_tracking_after_long_poll_deadline(
        stub_method_name: str,
        client_method_name: str,
):
    stub = MagicMock()
    stub.GetInstance.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE)
    getattr(stub, stub_method_name).side_effect = FakeRpcError(grpc.StatusCode.DEADLINE_EXCEEDED)

    with patch("durabletask.client.shared.get_grpc_channel", return_value=MagicMock()), patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub
    ):
        client = TaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(channel_recreate_failure_threshold=2)
        )
        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        with pytest.raises(TimeoutError):
            getattr(client, client_method_name)("abc")
        assert client._client_failure_tracker.consecutive_failures == 0


def test_sync_client_does_not_recreate_caller_owned_channel():
    provided_channel = MagicMock(name="provided-channel")
    stub = MagicMock()
    stub.GetInstance.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE)

    with patch("durabletask.client.shared.get_grpc_channel") as mock_get_channel, patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub
    ) as mock_stub, patch("threading.Timer") as mock_timer:
        client = TaskHubGrpcClient(
            channel=provided_channel,
            resiliency_options=GrpcClientResiliencyOptions(channel_recreate_failure_threshold=1),
        )
        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        client.close()

    assert client._channel is provided_channel
    mock_get_channel.assert_not_called()
    mock_stub.assert_called_once_with(provided_channel)
    mock_timer.assert_not_called()
    provided_channel.close.assert_not_called()


def test_sync_client_recreate_cooldown_prevents_immediate_repeated_recreation():
    first_channel = MagicMock(name="first-channel")
    second_channel = MagicMock(name="second-channel")
    third_channel = MagicMock(name="third-channel")
    first_stub = MagicMock()
    second_stub = MagicMock()
    third_stub = MagicMock()
    first_stub.GetInstance.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE)
    second_stub.GetInstance.side_effect = [
        FakeRpcError(grpc.StatusCode.UNAVAILABLE),
        FakeRpcError(grpc.StatusCode.UNAVAILABLE),
    ]
    timer1 = MagicMock(name="close-timer-1")
    timer2 = MagicMock(name="close-timer-2")

    with patch(
            "durabletask.client.shared.get_grpc_channel",
            side_effect=[first_channel, second_channel, third_channel],
    ) as mock_get_channel, patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub",
            side_effect=[first_stub, second_stub, third_stub],
    ), patch(
            "durabletask.client.time.monotonic", side_effect=[100.0, 101.0, 131.0]
    ), patch("threading.Timer", side_effect=[timer1, timer2]) as mock_timer:
        client = TaskHubGrpcClient(
            host_address=HOST_ADDRESS,
            resiliency_options=GrpcClientResiliencyOptions(
                channel_recreate_failure_threshold=1,
                min_recreate_interval_seconds=30.0,
            ),
        )
        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        assert client._channel is second_channel
        assert mock_get_channel.call_count == 2

        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        assert client._channel is second_channel
        assert mock_get_channel.call_count == 2

        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        assert client._channel is third_channel

    expected_channel_call = call(
        host_address=HOST_ADDRESS,
        secure_channel=False,
        interceptors=None,
        channel_options=None,
    )
    assert mock_get_channel.call_args_list == [
        expected_channel_call,
        expected_channel_call,
        expected_channel_call,
    ]
    assert mock_timer.call_count == 2
    first_timer_call, second_timer_call = mock_timer.call_args_list
    assert first_timer_call.args[0] == 30.0
    assert first_timer_call.args[1].__self__ is client
    assert first_timer_call.args[1].__func__ is TaskHubGrpcClient._close_retired_channel
    assert first_timer_call.kwargs == {"args": (first_channel,)}
    assert second_timer_call.args[0] == 30.0
    assert second_timer_call.args[1].__self__ is client
    assert second_timer_call.args[1].__func__ is TaskHubGrpcClient._close_retired_channel
    assert second_timer_call.kwargs == {"args": (second_channel,)}
    assert timer1.daemon is True
    assert timer2.daemon is True
    timer1.start.assert_called_once_with()
    timer2.start.assert_called_once_with()


def test_sync_client_resets_failure_tracking_after_success():
    stub = MagicMock()
    stub.GetInstance.side_effect = [
        FakeRpcError(grpc.StatusCode.UNAVAILABLE),
        MagicMock(exists=False),
    ]

    with patch("durabletask.client.shared.get_grpc_channel", return_value=MagicMock()), patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub
    ):
        client = TaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(channel_recreate_failure_threshold=2)
        )
        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        assert client.get_orchestration_state("abc") is None
        assert client._client_failure_tracker.consecutive_failures == 0


def test_sync_client_resets_failure_tracking_after_application_error():
    stub = MagicMock()
    stub.GetInstance.side_effect = [
        FakeRpcError(grpc.StatusCode.UNAVAILABLE),
        FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT),
    ]

    with patch("durabletask.client.shared.get_grpc_channel", return_value=MagicMock()), patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub
    ):
        client = TaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(channel_recreate_failure_threshold=2)
        )
        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        assert client._client_failure_tracker.consecutive_failures == 0


@pytest.mark.asyncio
async def test_async_client_recreates_sdk_owned_channel_with_original_transport_inputs():
    rpc_error = make_aio_rpc_error(grpc.StatusCode.UNAVAILABLE)
    first_channel = MagicMock(name="first-channel")
    first_channel.close = AsyncMock()
    second_channel = MagicMock(name="second-channel")
    second_channel.close = AsyncMock()
    first_stub = MagicMock()
    first_stub.GetInstance = AsyncMock(side_effect=rpc_error)
    second_stub = MagicMock()
    second_stub.GetInstance = AsyncMock(return_value=MagicMock(exists=False))
    host_address = "localhost:4001"
    interceptors = [DefaultAsyncClientInterceptorImpl(METADATA)]
    channel_options = GrpcChannelOptions(max_send_message_length=4321)

    with patch(
            "durabletask.client.shared.get_async_grpc_channel",
            side_effect=[first_channel, second_channel],
    ) as mock_get_channel, patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub", side_effect=[first_stub, second_stub]
    ):
        client = AsyncTaskHubGrpcClient(
            host_address=host_address,
            secure_channel=True,
            interceptors=interceptors,
            channel_options=channel_options,
            resiliency_options=GrpcClientResiliencyOptions(
                channel_recreate_failure_threshold=1,
                min_recreate_interval_seconds=0.0,
            ),
        )
        try:
            with pytest.raises(grpc.aio.AioRpcError):
                await client.get_orchestration_state("abc")
            await client.get_orchestration_state("abc")
        finally:
            await client.close()

    expected_channel_call = call(
        host_address=host_address,
        secure_channel=True,
        interceptors=interceptors,
        channel_options=channel_options,
    )
    assert mock_get_channel.call_args_list == [
        expected_channel_call,
        expected_channel_call,
    ]


@pytest.mark.asyncio
async def test_async_client_does_not_count_wait_for_orchestration_deadline():
    stub = MagicMock()
    stub.GetInstance = AsyncMock(side_effect=make_aio_rpc_error(grpc.StatusCode.UNAVAILABLE))
    stub.WaitForInstanceCompletion = AsyncMock(side_effect=make_aio_rpc_error(grpc.StatusCode.DEADLINE_EXCEEDED))

    with patch("durabletask.client.shared.get_async_grpc_channel", return_value=MagicMock()), patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub
    ):
        client = AsyncTaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(channel_recreate_failure_threshold=2)
        )
        with pytest.raises(grpc.aio.AioRpcError):
            await client.get_orchestration_state("abc")
        with pytest.raises(TimeoutError):
            await client.wait_for_orchestration_completion("abc")
        assert client._client_failure_tracker.consecutive_failures == 0


@pytest.mark.asyncio
async def test_async_client_close_closes_retired_channels_immediately():
    rpc_error = make_aio_rpc_error(grpc.StatusCode.UNAVAILABLE)
    first_channel = MagicMock(name="first-channel")
    first_channel.close = AsyncMock()
    second_channel = MagicMock(name="second-channel")
    second_channel.close = AsyncMock()
    first_stub = MagicMock()
    first_stub.GetInstance = AsyncMock(side_effect=rpc_error)
    second_stub = MagicMock()
    second_stub.GetInstance = AsyncMock(return_value=MagicMock(exists=False))
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()

    async def blocked_close_retired_channel(self, channel):
        cleanup_started.set()
        await release_cleanup.wait()
        await channel.close()

    with patch(
            "durabletask.client.shared.get_async_grpc_channel",
            side_effect=[first_channel, second_channel],
    ), patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub", side_effect=[first_stub, second_stub]
    ), patch.object(
            AsyncTaskHubGrpcClient,
            "_close_retired_channel",
            new=blocked_close_retired_channel,
    ):
        client = AsyncTaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(
                channel_recreate_failure_threshold=1,
                min_recreate_interval_seconds=0.0,
            )
        )
        with pytest.raises(grpc.aio.AioRpcError):
            await client.get_orchestration_state("abc")
        await cleanup_started.wait()

        try:
            await client.close()
            first_channel.close.assert_awaited_once()
            second_channel.close.assert_awaited_once()
        finally:
            release_cleanup.set()
            await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_async_client_does_not_recreate_caller_owned_channel():
    provided_channel = MagicMock(name="provided-channel")
    stub = MagicMock()
    stub.GetInstance = AsyncMock(side_effect=make_aio_rpc_error(grpc.StatusCode.UNAVAILABLE))

    with patch("durabletask.client.shared.get_async_grpc_channel") as mock_get_channel, patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub
    ):
        client = AsyncTaskHubGrpcClient(
            channel=provided_channel,
            resiliency_options=GrpcClientResiliencyOptions(channel_recreate_failure_threshold=1),
        )
        with pytest.raises(grpc.aio.AioRpcError):
            await client.get_orchestration_state("abc")
        with pytest.raises(grpc.aio.AioRpcError):
            await client.get_orchestration_state("abc")

    assert client._channel is provided_channel
    mock_get_channel.assert_not_called()


@pytest.mark.asyncio
async def test_async_client_close_prevents_channel_recreation_race():
    first_channel = MagicMock(name="first-channel")
    first_channel.close = AsyncMock()
    second_channel = MagicMock(name="second-channel")
    second_channel.close = AsyncMock()
    first_stub = MagicMock()
    first_stub.GetInstance = AsyncMock(side_effect=make_aio_rpc_error(grpc.StatusCode.UNAVAILABLE))
    second_stub = MagicMock()
    second_stub.GetInstance = AsyncMock(return_value=MagicMock(exists=False))

    with patch(
            "durabletask.client.shared.get_async_grpc_channel",
            side_effect=[first_channel, second_channel],
    ) as mock_get_channel, patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub", side_effect=[first_stub, second_stub]
    ):
        client = AsyncTaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(
                channel_recreate_failure_threshold=1,
                min_recreate_interval_seconds=0.0,
            )
        )
        await client._recreate_lock.acquire()
        try:
            rpc_task = asyncio.create_task(client.get_orchestration_state("abc"))
            while first_stub.GetInstance.await_count == 0:
                await asyncio.sleep(0)
            close_task = asyncio.create_task(client.close())
            await asyncio.sleep(0)
        finally:
            client._recreate_lock.release()

        with pytest.raises(grpc.aio.AioRpcError):
            await rpc_task
        await close_task

    assert mock_get_channel.call_count == 1
    first_channel.close.assert_awaited_once()
    second_channel.close.assert_not_awaited()


def test_worker_stores_resiliency_options():
    resiliency = GrpcWorkerResiliencyOptions(channel_recreate_failure_threshold=9)
    worker = TaskHubGrpcWorker(resiliency_options=resiliency)
    assert worker._resiliency_options is resiliency


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
