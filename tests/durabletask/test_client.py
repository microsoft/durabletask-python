import asyncio
import inspect
import json
import grpc
import pytest
from collections import namedtuple
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


# ----- Test helpers for the gRPC resiliency interceptor -----
#
# In production the resiliency interceptor is wired into the channel via
# ``grpc.intercept_channel`` (sync) or by passing ``interceptors=`` to
# ``grpc.aio.{secure,insecure}_channel`` (async). Tests in this file mock the
# underlying stub class (``stubs.TaskHubSidecarServiceStub``) with
# ``MagicMock`` instances so they can configure ``side_effect``s and assert
# call counts without standing up a real gRPC server. Those mocks bypass the
# real channel/interceptor wiring, so failure tracking and channel-recreate
# triggering need a small test-only shim that mimics the interceptor pipeline.


_TEST_SERVICE_PREFIX = "/TaskHubSidecarService"


class _SimpleCallDetails(
        namedtuple(
            "_SimpleCallDetails",
            ["method", "timeout", "metadata", "credentials", "wait_for_ready", "compression"],
        )):
    """Minimal stand-in for ``grpc.ClientCallDetails`` used by interceptor tests."""
    pass


class _SimulatedSyncCall:
    """Mimics a sync gRPC unary Call-Future enough for the resiliency interceptor.

    The interceptor inspects ``.exception()`` to learn the call outcome. We
    eagerly evaluate the wrapped mock the first time either ``exception()`` or
    ``result()`` is called and cache the result.
    """

    def __init__(self, method, request, timeout):
        self._method = method
        self._request = request
        self._timeout = timeout
        self._evaluated = False
        self._result = None
        self._error = None

    def _evaluate(self):
        if self._evaluated:
            return
        try:
            if self._timeout is None:
                self._result = self._method(self._request)
            else:
                self._result = self._method(self._request, timeout=self._timeout)
        except grpc.RpcError as rpc_error:
            self._error = rpc_error
        self._evaluated = True

    def exception(self):
        self._evaluate()
        return self._error

    def result(self):
        self._evaluate()
        if self._error is not None:
            raise self._error
        return self._result


class _ResilientSyncTestStub:
    """Wraps a ``MagicMock`` stub so calls flow through the sync interceptor."""

    def __init__(self, mock_stub, interceptor):
        self._stub = mock_stub
        self._interceptor = interceptor

    def __getattr__(self, name):
        method = getattr(self._stub, name)
        method_path = f"{_TEST_SERVICE_PREFIX}/{name}"
        interceptor = self._interceptor

        def wrapped(request, *, timeout=None):
            details = _SimpleCallDetails(
                method=method_path,
                timeout=timeout,
                metadata=None,
                credentials=None,
                wait_for_ready=False,
                compression=None,
            )
            call = interceptor.intercept_unary_unary(
                lambda d, r: _SimulatedSyncCall(method, r, timeout),
                details,
                request,
            )
            return call.result()

        return wrapped


class _SimulatedAsyncCall:
    """Awaitable that mimics ``grpc.aio`` unary call resolution."""

    def __init__(self, method, request, timeout):
        self._method = method
        self._request = request
        self._timeout = timeout

    def __await__(self):
        if self._timeout is None:
            coro_or_value = self._method(self._request)
        else:
            coro_or_value = self._method(self._request, timeout=self._timeout)
        if asyncio.iscoroutine(coro_or_value):
            return coro_or_value.__await__()

        async def _wrap():
            return coro_or_value

        return _wrap().__await__()


class _ResilientAsyncTestStub:
    """Wraps a ``MagicMock`` stub so calls flow through the async interceptor."""

    def __init__(self, mock_stub, interceptor):
        self._stub = mock_stub
        self._interceptor = interceptor

    def __getattr__(self, name):
        method = getattr(self._stub, name)
        method_path = f"{_TEST_SERVICE_PREFIX}/{name}"
        interceptor = self._interceptor

        async def wrapped(request, *, timeout=None):
            details = _SimpleCallDetails(
                method=method_path,
                timeout=timeout,
                metadata=None,
                credentials=None,
                wait_for_ready=False,
                compression=None,
            )

            async def continuation(d, r):
                if timeout is None:
                    return await method(r)
                return await method(r, timeout=timeout)

            return await interceptor.intercept_unary_unary(continuation, details, request)

        return wrapped


def install_resilient_test_stubs(client):
    """Route the client's mocked stub through its resiliency interceptor.

    Tests in this module patch ``stubs.TaskHubSidecarServiceStub`` to return
    ``MagicMock`` stubs whose method calls bypass gRPC's real channel
    interception pipeline. This helper wraps the current stub in a thin
    interceptor-aware shim, and re-wraps after each ``_maybe_recreate_channel``
    call so newly created stubs continue to participate in failure tracking.

    The interceptor's ``_on_recreate`` callback is intentionally left alone:
    it is the client's ``_schedule_recreate`` (fire-and-forget), and tests
    that need to observe the recreate's completion can wait on
    ``client._recreate_done_event``.
    """
    is_async = inspect.iscoroutinefunction(client._maybe_recreate_channel)
    wrapper_cls = _ResilientAsyncTestStub if is_async else _ResilientSyncTestStub

    def wrap_if_needed():
        if not isinstance(client._stub, wrapper_cls):
            client._stub = wrapper_cls(client._stub, client._resiliency_interceptor)

    wrap_if_needed()

    original_recreate = client._maybe_recreate_channel

    if is_async:
        async def wrapped_recreate():
            await original_recreate()
            wrap_if_needed()
    else:
        def wrapped_recreate():
            original_recreate()
            wrap_if_needed()

    client._maybe_recreate_channel = wrapped_recreate


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
        client = AsyncTaskHubGrpcClient()
        mock_channel.assert_called_once_with(
            get_default_host_address(),
            interceptors=[client._resiliency_interceptor])


def test_async_client_creates_with_metadata():
    with patch('grpc.aio.insecure_channel') as mock_channel:
        mock_channel.return_value = MagicMock()
        client = AsyncTaskHubGrpcClient(
            host_address=HOST_ADDRESS, metadata=METADATA)
        mock_channel.assert_called_once()
        args, kwargs = mock_channel.call_args
        assert args[0] == HOST_ADDRESS
        interceptors = kwargs.get('interceptors')
        assert interceptors is not None
        # Resiliency interceptor is prepended, followed by the metadata interceptor.
        assert len(interceptors) == 2
        assert interceptors[0] is client._resiliency_interceptor
        assert isinstance(interceptors[1], DefaultAsyncClientInterceptorImpl)


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
    with (
        patch("durabletask.client.shared.get_grpc_channel", return_value=MagicMock()),
        patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=MagicMock()),
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
    # The resiliency interceptor is prepended automatically; user interceptors follow.
    assert client._interceptors == [client._resiliency_interceptor, *interceptors]


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

    with (
        patch(
            "durabletask.client.shared.get_grpc_channel",
            side_effect=[first_channel, second_channel],
        ) as mock_get_channel,
        patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub",
            side_effect=[first_stub, second_stub],
        ),
        patch("threading.Timer", return_value=timer) as mock_timer,
    ):
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
        install_resilient_test_stubs(client)
        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        # Fire-and-forget recreate runs on a daemon thread; wait for it to
        # complete before issuing the call that should see the new channel.
        assert client._recreate_done_event.wait(timeout=5.0)
        client.get_orchestration_state("abc")

    expected_channel_call = call(
        host_address=host_address,
        secure_channel=True,
        interceptors=[client._resiliency_interceptor, *interceptors],
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

    with (
        patch(
            "durabletask.client.shared.get_grpc_channel",
            side_effect=[first_channel, second_channel],
        ),
        patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub",
            side_effect=[first_stub, second_stub],
        ),
        patch("threading.Timer", return_value=close_timer),
    ):
        client = TaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(
                channel_recreate_failure_threshold=1,
                min_recreate_interval_seconds=0.0,
            )
        )
        install_resilient_test_stubs(client)
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

    with (
        patch(
            "durabletask.client.shared.get_grpc_channel",
            side_effect=[first_channel, second_channel, third_channel],
        ),
        patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub",
            side_effect=[first_stub, second_stub, third_stub],
        ),
        patch("threading.Timer", side_effect=[timer1, timer2]),
    ):
        client = TaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(
                channel_recreate_failure_threshold=1,
                min_recreate_interval_seconds=0.0,
            )
        )
        install_resilient_test_stubs(client)
        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        # Wait for the first fire-and-forget recreate to complete so the
        # single-flight guard in _schedule_recreate does not drop the second
        # trigger.
        assert client._recreate_done_event.wait(timeout=5.0)
        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        assert client._recreate_done_event.wait(timeout=5.0)

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

    with (
        patch("durabletask.client.shared.get_grpc_channel", return_value=MagicMock()),
        patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub),
    ):
        client = TaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(channel_recreate_failure_threshold=2)
        )
        install_resilient_test_stubs(client)
        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        with pytest.raises(TimeoutError):
            getattr(client, client_method_name)("abc")
        assert client._client_failure_tracker.consecutive_failures == 0


def test_sync_client_does_not_recreate_caller_owned_channel():
    provided_channel = MagicMock(name="provided-channel")
    stub = MagicMock()
    stub.GetInstance.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE)

    with (
        patch("durabletask.client.shared.get_grpc_channel") as mock_get_channel,
        patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub
        ) as mock_stub,
        patch("threading.Timer") as mock_timer,
    ):
        client = TaskHubGrpcClient(
            channel=provided_channel,
            resiliency_options=GrpcClientResiliencyOptions(channel_recreate_failure_threshold=1),
        )
        # Note: caller-owned channels are intentionally NOT wrapped with the
        # resiliency interceptor (the channel is never recreated, so failure
        # tracking against it would have no observable effect). The mocked
        # stub therefore raises directly without triggering the interceptor.
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


def test_sync_client_context_manager_returns_self_and_calls_close():
    with (
        patch("durabletask.client.shared.get_grpc_channel", return_value=MagicMock()),
        patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=MagicMock()),
    ):
        client = TaskHubGrpcClient(host_address=HOST_ADDRESS)
        with patch.object(client, "close", wraps=client.close) as spy_close:
            with client as entered:
                assert entered is client
            spy_close.assert_called_once_with()


def test_sync_client_context_manager_closes_sdk_owned_channel():
    channel = MagicMock(name="sdk-owned-channel")
    with (
        patch("durabletask.client.shared.get_grpc_channel", return_value=channel),
        patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=MagicMock()),
    ):
        with TaskHubGrpcClient(host_address=HOST_ADDRESS) as client:
            assert client._closing is False
            assert client._owns_channel is True

    assert client._closing is True
    channel.close.assert_called_once_with()


def test_sync_client_context_manager_preserves_caller_owned_channel():
    provided_channel = MagicMock(name="caller-owned-channel")
    with patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=MagicMock()):
        with TaskHubGrpcClient(
                channel=provided_channel, host_address=HOST_ADDRESS) as client:
            assert client._channel is provided_channel
            assert client._owns_channel is False

    # close() is a no-op for caller-owned channels: the caller retains
    # ownership and is responsible for closing the channel themselves.
    provided_channel.close.assert_not_called()
    assert client._closing is False


def test_sync_client_context_manager_propagates_exception_and_calls_close():
    channel = MagicMock(name="sdk-owned-channel")
    with (
        patch("durabletask.client.shared.get_grpc_channel", return_value=channel),
        patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=MagicMock()),
    ):
        client = TaskHubGrpcClient(host_address=HOST_ADDRESS)
        raised = False
        try:
            with client:
                raise RuntimeError("boom")
        except RuntimeError as exc:
            raised = True
            assert str(exc) == "boom"
        assert raised, "RuntimeError raised inside the with block must propagate"

    assert client._closing is True
    channel.close.assert_called_once_with()


def test_sync_client_context_manager_cleans_up_resiliency_state():
    """Regression: exiting the ``with`` block tears down resiliency state
    introduced by PR #135 (retired channels + recreate thread).
    """
    first_channel = MagicMock(name="first-channel")
    second_channel = MagicMock(name="second-channel")
    first_stub = MagicMock()
    first_stub.GetInstance.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE)
    second_stub = MagicMock()
    second_stub.GetInstance.return_value = MagicMock(exists=False)
    close_timer = MagicMock(name="close-timer")

    with (
        patch(
            "durabletask.client.shared.get_grpc_channel",
            side_effect=[first_channel, second_channel],
        ),
        patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub",
            side_effect=[first_stub, second_stub],
        ),
        patch("threading.Timer", return_value=close_timer),
    ):
        with TaskHubGrpcClient(
                resiliency_options=GrpcClientResiliencyOptions(
                    channel_recreate_failure_threshold=1,
                    min_recreate_interval_seconds=0.0,
                )) as client:
            install_resilient_test_stubs(client)
            with pytest.raises(FakeRpcError):
                client.get_orchestration_state("abc")
            # Wait for the fire-and-forget recreate to finish so the retired
            # channel + timer are registered before the context manager exits.
            assert client._recreate_done_event.wait(timeout=5.0)

    assert client._closing is True
    close_timer.cancel.assert_called_once_with()
    first_channel.close.assert_called_once_with()
    second_channel.close.assert_called_once_with()
    assert client._retired_channels == {}
    # The recreate thread (if any) must have been joined during __exit__.
    recreate_thread = client._recreate_thread
    if recreate_thread is not None:
        assert not recreate_thread.is_alive()


def test_sync_client_supports_context_manager_reentry_after_use():
    """``with`` is idempotent against repeated ``close()`` calls."""
    channel = MagicMock(name="sdk-owned-channel")
    with (
        patch("durabletask.client.shared.get_grpc_channel", return_value=channel),
        patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=MagicMock()),
    ):
        client = TaskHubGrpcClient(host_address=HOST_ADDRESS)
        with client:
            pass
        # Calling close() again after the context manager has already torn
        # down the channel must not raise (mirrors how ``AsyncTaskHubGrpcClient``
        # tolerates explicit close-after-aexit during shutdown sequences).
        client.close()

    assert channel.close.call_count >= 1


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

    with (
        patch(
            "durabletask.client.shared.get_grpc_channel",
            side_effect=[first_channel, second_channel, third_channel],
        ) as mock_get_channel,
        patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub",
            side_effect=[first_stub, second_stub, third_stub],
        ),
        patch(
            "durabletask.client.time.monotonic", side_effect=[100.0, 101.0, 131.0]
        ),
        patch("threading.Timer", side_effect=[timer1, timer2]) as mock_timer,
    ):
        client = TaskHubGrpcClient(
            host_address=HOST_ADDRESS,
            resiliency_options=GrpcClientResiliencyOptions(
                channel_recreate_failure_threshold=1,
                min_recreate_interval_seconds=30.0,
            ),
        )
        install_resilient_test_stubs(client)
        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        # Wait for the fire-and-forget recreate to complete before asserting
        # the channel was swapped.
        assert client._recreate_done_event.wait(timeout=5.0)
        assert client._channel is second_channel
        assert mock_get_channel.call_count == 2

        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        # Cooldown should fire-and-forget but exit without recreating; wait
        # for the no-op recreate to complete so the assertion is deterministic.
        assert client._recreate_done_event.wait(timeout=5.0)
        assert client._channel is second_channel
        assert mock_get_channel.call_count == 2

        with pytest.raises(FakeRpcError):
            client.get_orchestration_state("abc")
        assert client._recreate_done_event.wait(timeout=5.0)
        assert client._channel is third_channel

    expected_channel_call = call(
        host_address=HOST_ADDRESS,
        secure_channel=False,
        interceptors=[client._resiliency_interceptor],
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

    with (
        patch("durabletask.client.shared.get_grpc_channel", return_value=MagicMock()),
        patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub),
    ):
        client = TaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(channel_recreate_failure_threshold=2)
        )
        install_resilient_test_stubs(client)
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

    with (
        patch("durabletask.client.shared.get_grpc_channel", return_value=MagicMock()),
        patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub),
    ):
        client = TaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(channel_recreate_failure_threshold=2)
        )
        install_resilient_test_stubs(client)
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
        install_resilient_test_stubs(client)
        try:
            with pytest.raises(grpc.aio.AioRpcError):
                await client.get_orchestration_state("abc")
            # Fire-and-forget recreate runs as an asyncio task; await its
            # completion before issuing the call that should see the new
            # channel.
            await asyncio.wait_for(client._recreate_done_event.wait(), timeout=5.0)
            await client.get_orchestration_state("abc")
        finally:
            await client.close()

    expected_channel_call = call(
        host_address=host_address,
        secure_channel=True,
        interceptors=[client._resiliency_interceptor, *interceptors],
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

    with (
        patch("durabletask.client.shared.get_async_grpc_channel", return_value=MagicMock()),
        patch("durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub),
    ):
        client = AsyncTaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(channel_recreate_failure_threshold=2)
        )
        install_resilient_test_stubs(client)
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

    with (
        patch(
            "durabletask.client.shared.get_async_grpc_channel",
            side_effect=[first_channel, second_channel],
        ),
        patch(
            "durabletask.client.stubs.TaskHubSidecarServiceStub",
            side_effect=[first_stub, second_stub],
        ),
        patch.object(
            AsyncTaskHubGrpcClient,
            "_close_retired_channel",
            new=blocked_close_retired_channel,
        ),
    ):
        client = AsyncTaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(
                channel_recreate_failure_threshold=1,
                min_recreate_interval_seconds=0.0,
            )
        )
        install_resilient_test_stubs(client)
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
        # Caller-owned channels intentionally bypass the resiliency interceptor
        # (we never recreate them).
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
        install_resilient_test_stubs(client)
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
            _ = await rpc_task
        _ = await close_task

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
