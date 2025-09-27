from unittest.mock import ANY, patch

from durabletask.aio.internal.grpc_interceptor import DefaultAioClientInterceptorImpl
from durabletask.internal.shared import get_default_host_address
from durabletask.aio.internal.shared import get_grpc_aio_channel
from durabletask.aio.client import AsyncTaskHubGrpcClient


HOST_ADDRESS = 'localhost:50051'
METADATA = [('key1', 'value1'), ('key2', 'value2')]
INTERCEPTORS_AIO = [DefaultAioClientInterceptorImpl(METADATA)]


def test_get_grpc_aio_channel_insecure():
    with patch('durabletask.aio.internal.shared.grpc_aio.insecure_channel') as mock_channel:
        get_grpc_aio_channel(HOST_ADDRESS, False, interceptors=INTERCEPTORS_AIO)
        mock_channel.assert_called_once_with(HOST_ADDRESS, interceptors=INTERCEPTORS_AIO)


def test_get_grpc_aio_channel_secure():
    with patch('durabletask.aio.internal.shared.grpc_aio.secure_channel') as mock_channel, patch(
            'grpc.ssl_channel_credentials') as mock_credentials:
        get_grpc_aio_channel(HOST_ADDRESS, True, interceptors=INTERCEPTORS_AIO)
        mock_channel.assert_called_once_with(HOST_ADDRESS, mock_credentials.return_value, interceptors=INTERCEPTORS_AIO)


def test_get_grpc_aio_channel_default_host_address():
    with patch('durabletask.aio.internal.shared.grpc_aio.insecure_channel') as mock_channel:
        get_grpc_aio_channel(None, False, interceptors=INTERCEPTORS_AIO)
        mock_channel.assert_called_once_with(get_default_host_address(), interceptors=INTERCEPTORS_AIO)


def test_get_grpc_aio_channel_with_interceptors():
    with patch('durabletask.aio.internal.shared.grpc_aio.insecure_channel') as mock_channel:
        get_grpc_aio_channel(HOST_ADDRESS, False, interceptors=INTERCEPTORS_AIO)
        mock_channel.assert_called_once_with(HOST_ADDRESS, interceptors=INTERCEPTORS_AIO)

        # Capture and check the arguments passed to insecure_channel()
        args, kwargs = mock_channel.call_args
        assert args[0] == HOST_ADDRESS
        assert 'interceptors' in kwargs
        interceptors = kwargs['interceptors']
        assert isinstance(interceptors[0], DefaultAioClientInterceptorImpl)
        assert interceptors[0]._metadata == METADATA


def test_grpc_aio_channel_with_host_name_protocol_stripping():
    with patch('durabletask.aio.internal.shared.grpc_aio.insecure_channel') as mock_insecure_channel, patch(
            'durabletask.aio.internal.shared.grpc_aio.secure_channel') as mock_secure_channel:

        host_name = "myserver.com:1234"

        prefix = "grpc://"
        get_grpc_aio_channel(prefix + host_name, interceptors=INTERCEPTORS_AIO)
        mock_insecure_channel.assert_called_with(host_name, interceptors=INTERCEPTORS_AIO)

        prefix = "http://"
        get_grpc_aio_channel(prefix + host_name, interceptors=INTERCEPTORS_AIO)
        mock_insecure_channel.assert_called_with(host_name, interceptors=INTERCEPTORS_AIO)

        prefix = "HTTP://"
        get_grpc_aio_channel(prefix + host_name, interceptors=INTERCEPTORS_AIO)
        mock_insecure_channel.assert_called_with(host_name, interceptors=INTERCEPTORS_AIO)

        prefix = "GRPC://"
        get_grpc_aio_channel(prefix + host_name, interceptors=INTERCEPTORS_AIO)
        mock_insecure_channel.assert_called_with(host_name, interceptors=INTERCEPTORS_AIO)

        prefix = ""
        get_grpc_aio_channel(prefix + host_name, interceptors=INTERCEPTORS_AIO)
        mock_insecure_channel.assert_called_with(host_name, interceptors=INTERCEPTORS_AIO)

        prefix = "grpcs://"
        get_grpc_aio_channel(prefix + host_name, interceptors=INTERCEPTORS_AIO)
        mock_secure_channel.assert_called_with(host_name, ANY, interceptors=INTERCEPTORS_AIO)

        prefix = "https://"
        get_grpc_aio_channel(prefix + host_name, interceptors=INTERCEPTORS_AIO)
        mock_secure_channel.assert_called_with(host_name, ANY, interceptors=INTERCEPTORS_AIO)

        prefix = "HTTPS://"
        get_grpc_aio_channel(prefix + host_name, interceptors=INTERCEPTORS_AIO)
        mock_secure_channel.assert_called_with(host_name, ANY, interceptors=INTERCEPTORS_AIO)

        prefix = "GRPCS://"
        get_grpc_aio_channel(prefix + host_name, interceptors=INTERCEPTORS_AIO)
        mock_secure_channel.assert_called_with(host_name, ANY, interceptors=INTERCEPTORS_AIO)

        prefix = ""
        get_grpc_aio_channel(prefix + host_name, True, interceptors=INTERCEPTORS_AIO)
        mock_secure_channel.assert_called_with(host_name, ANY, interceptors=INTERCEPTORS_AIO)


def test_async_client_construct_with_metadata():
    with patch('durabletask.aio.internal.shared.grpc_aio.insecure_channel') as mock_channel:
        AsyncTaskHubGrpcClient(host_address=HOST_ADDRESS, metadata=METADATA)
        # Ensure channel created with an interceptor that has the expected metadata
        args, kwargs = mock_channel.call_args
        assert args[0] == HOST_ADDRESS
        assert 'interceptors' in kwargs
        interceptors = kwargs['interceptors']
        assert isinstance(interceptors[0], DefaultAioClientInterceptorImpl)
        assert interceptors[0]._metadata == METADATA


