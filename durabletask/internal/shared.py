# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import dataclasses
import json
import logging
from types import SimpleNamespace
from typing import Any, Optional, Sequence, Union

import grpc

ClientInterceptor = Union[
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor
]

# Field name used to indicate that an object was automatically serialized
# and should be deserialized as a SimpleNamespace
AUTO_SERIALIZED = "__durabletask_autoobject__"

SECURE_PROTOCOLS = ["https://", "grpcs://"]
INSECURE_PROTOCOLS = ["http://", "grpc://"]


def get_default_host_address() -> str:
    return "localhost:4001"


def get_grpc_channel(
        host_address: Optional[str],
        secure_channel: bool = False,
        interceptors: Optional[Sequence[ClientInterceptor]] = None) -> grpc.Channel:

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
    if secure_channel:
        channel = grpc.secure_channel(host_address, grpc.ssl_channel_credentials())
    else:
        channel = grpc.insecure_channel(host_address)

    # Apply interceptors ONLY if they exist
    if interceptors:
        channel = grpc.intercept_channel(channel, *interceptors)
    return channel


def get_logger(
        name_suffix: str,
        log_handler: Optional[logging.Handler] = None,
        log_formatter: Optional[logging.Formatter] = None) -> logging.Logger:
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


def to_json(obj):
    return json.dumps(obj, cls=InternalJSONEncoder)


def from_json(json_str):
    return json.loads(json_str, cls=InternalJSONDecoder)


class InternalJSONEncoder(json.JSONEncoder):
    """JSON encoder that supports serializing specific Python types."""

    def encode(self, obj: Any) -> str:
        # if the object is a namedtuple, convert it to a dict with the AUTO_SERIALIZED key added
        if isinstance(obj, tuple) and hasattr(obj, "_fields") and hasattr(obj, "_asdict"):
            d = obj._asdict()  # type: ignore
            d[AUTO_SERIALIZED] = True
            obj = d
        return super().encode(obj)

    def default(self, obj):
        if dataclasses.is_dataclass(obj):
            # Dataclasses are not serializable by default, so we convert them to a dict and mark them for
            # automatic deserialization by the receiver
            d = dataclasses.asdict(obj)  # type: ignore
            d[AUTO_SERIALIZED] = True
            return d
        elif isinstance(obj, SimpleNamespace):
            # Most commonly used for serializing custom objects that were previously serialized using our encoder
            d = vars(obj)
            d[AUTO_SERIALIZED] = True
            return d
        # This will typically raise a TypeError
        return json.JSONEncoder.default(self, obj)


class InternalJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.dict_to_object, *args, **kwargs)

    def dict_to_object(self, d: dict[str, Any]):
        # If the object was serialized by the InternalJSONEncoder, deserialize it as a SimpleNamespace
        if d.pop(AUTO_SERIALIZED, False):
            return SimpleNamespace(**d)
        return d
