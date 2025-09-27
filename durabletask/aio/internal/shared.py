from typing import Optional, Sequence, Union

import grpc
from grpc import aio as grpc_aio

from durabletask.internal.shared import (
    get_default_host_address,
    SECURE_PROTOCOLS,
    INSECURE_PROTOCOLS,
)


AioClientInterceptor = Union[
    grpc_aio.UnaryUnaryClientInterceptor,
    grpc_aio.UnaryStreamClientInterceptor,
    grpc_aio.StreamUnaryClientInterceptor,
    grpc_aio.StreamStreamClientInterceptor
]


def get_grpc_aio_channel(
        host_address: Optional[str],
        secure_channel: bool = False,
        interceptors: Optional[Sequence[AioClientInterceptor]] = None) -> grpc_aio.Channel:

    if host_address is None:
        host_address = get_default_host_address()

    for protocol in SECURE_PROTOCOLS:
        if host_address.lower().startswith(protocol):
            secure_channel = True
            host_address = host_address[len(protocol):]
            break

    for protocol in INSECURE_PROTOCOLS:
        if host_address.lower().startswith(protocol):
            secure_channel = False
            host_address = host_address[len(protocol):]
            break

    if secure_channel:
        channel = grpc_aio.secure_channel(host_address, grpc.ssl_channel_credentials(), interceptors=interceptors)
    else:
        channel = grpc_aio.insecure_channel(host_address, interceptors=interceptors)

    return channel


