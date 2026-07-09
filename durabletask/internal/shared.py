# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import warnings
from collections.abc import Sequence
from typing import Any, TypeAlias

import grpc
import grpc.aio

# Backwards-compatibility shims. The JSON codec moved into
# ``durabletask.serialization`` and its functions are now private; the supported
# surface is the pluggable ``DataConverter`` (and the default
# ``JsonDataConverter``). These thin wrappers keep older imports from
# ``durabletask.internal.shared`` working while steering callers to the new API.
# They deliberately reach into the now-private serialization mechanism.
from durabletask import serialization as _serialization
from durabletask.grpc_options import GrpcChannelOptions

# Legacy marker constant, re-exported for backwards compatibility.
AUTO_SERIALIZED = _serialization._AUTO_SERIALIZED  # pyright: ignore[reportPrivateUsage]

_SERIALIZATION_DEPRECATION = (
    "durabletask.internal.shared.{name} is deprecated and will be removed in a "
    "future release. Use a durabletask.serialization.DataConverter (e.g. the "
    "default JsonDataConverter) instead."
)


def to_json(obj: Any) -> str:
    """Deprecated. Use a ``durabletask.serialization.DataConverter`` instead."""
    warnings.warn(
        _SERIALIZATION_DEPRECATION.format(name="to_json"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _serialization._to_json(obj)  # pyright: ignore[reportPrivateUsage]


def from_json(json_str: str | bytes | bytearray, expected_type: type | None = None) -> Any:
    """Deprecated. Use a ``durabletask.serialization.DataConverter`` instead.

    This legacy shim does not thread a ``DataConverter`` into reconstruction, so
    a converter-aware ``from_json(cls, value, converter)`` hook is invoked
    without the converter (its single-argument form). Call
    ``JsonDataConverter().deserialize(...)`` to get the converter-aware path.
    """
    warnings.warn(
        _SERIALIZATION_DEPRECATION.format(name="from_json"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _serialization._from_json(json_str, expected_type)  # pyright: ignore[reportPrivateUsage]


ClientInterceptor: TypeAlias = (
    grpc.UnaryUnaryClientInterceptor
    | grpc.UnaryStreamClientInterceptor
    | grpc.StreamUnaryClientInterceptor
    | grpc.StreamStreamClientInterceptor
)

AsyncClientInterceptor: TypeAlias = (
    grpc.aio.UnaryUnaryClientInterceptor
    | grpc.aio.UnaryStreamClientInterceptor
    | grpc.aio.StreamUnaryClientInterceptor
    | grpc.aio.StreamStreamClientInterceptor
)

SECURE_PROTOCOLS = ["https://", "grpcs://"]
INSECURE_PROTOCOLS = ["http://", "grpc://"]


def get_default_host_address() -> str:
    return "localhost:4001"


def get_grpc_channel(
        host_address: str | None,
        secure_channel: bool = False,
        interceptors: Sequence[ClientInterceptor] | None = None,
        channel_options: GrpcChannelOptions | None = None) -> grpc.Channel:

    if host_address is None:
        host_address = get_default_host_address()

    for protocol in SECURE_PROTOCOLS:
        if host_address.lower().startswith(protocol):
            secure_channel = True
            # remove the protocol from the host name
            host_address = host_address[len(protocol):]
            break

    for protocol in INSECURE_PROTOCOLS:
        if host_address.lower().startswith(protocol):
            secure_channel = False
            # remove the protocol from the host name
            host_address = host_address[len(protocol):]
            break

    # Create the base channel
    options = channel_options.to_grpc_options() if channel_options is not None else None
    if secure_channel:
        if options is None:
            channel = grpc.secure_channel(host_address, grpc.ssl_channel_credentials())
        else:
            channel = grpc.secure_channel(
                host_address,
                grpc.ssl_channel_credentials(),
                options=options,
            )
    else:
        if options is None:
            channel = grpc.insecure_channel(host_address)
        else:
            channel = grpc.insecure_channel(host_address, options=options)

    # Apply interceptors ONLY if they exist
    if interceptors:
        channel = grpc.intercept_channel(channel, *interceptors)
    return channel


def get_async_grpc_channel(
        host_address: str | None,
        secure_channel: bool = False,
        interceptors: Sequence[AsyncClientInterceptor] | None = None,
        channel_options: GrpcChannelOptions | None = None) -> grpc.aio.Channel:

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

    options = channel_options.to_grpc_options() if channel_options is not None else None

    if secure_channel:
        if options is None:
            channel = grpc.aio.secure_channel(
                host_address,
                grpc.ssl_channel_credentials(),
                interceptors=interceptors,
            )
        else:
            channel = grpc.aio.secure_channel(
                host_address,
                grpc.ssl_channel_credentials(),
                interceptors=interceptors,
                options=options,
            )
    else:
        if options is None:
            channel = grpc.aio.insecure_channel(
                host_address,
                interceptors=interceptors,
            )
        else:
            channel = grpc.aio.insecure_channel(
                host_address,
                interceptors=interceptors,
                options=options,
            )

    return channel


def get_logger(
        name_suffix: str,
        log_handler: logging.Handler | None = None,
        log_formatter: logging.Formatter | None = None) -> logging.Logger:
    logger = logging.Logger(f"durabletask-{name_suffix}")

    # Add a default log handler if none is provided
    if log_handler is None:
        log_handler = logging.StreamHandler()
        log_handler.setLevel(logging.INFO)
    logger.handlers.append(log_handler)

    # Set a default log formatter to our handler if none is provided
    if log_formatter is None:
        log_formatter = logging.Formatter(
            fmt="%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s",
            datefmt='%Y-%m-%d %H:%M:%S')
    log_handler.setFormatter(log_formatter)
    return logger
