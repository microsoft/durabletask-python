# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import dataclasses
import json
import logging
import types
import typing
from collections.abc import Sequence
from types import SimpleNamespace
from typing import Any, TypeAlias, cast

import grpc
import grpc.aio
from durabletask.grpc_options import GrpcChannelOptions

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

# Marker formerly added to JSON payloads to flag objects for automatic
# deserialization into a SimpleNamespace. New code no longer emits this marker
# (objects are serialized as plain JSON), but the decoder still recognizes it so
# that orchestration histories produced by older SDK versions continue to replay.
AUTO_SERIALIZED = "__durabletask_autoobject__"

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


def to_json(obj: Any) -> str:
    """Serialize a value to a JSON string.

    Builtins serialize to plain JSON. Dataclasses, ``SimpleNamespace``
    instances, and objects exposing a ``to_json()`` method are serialized to
    plain JSON as well (without any type marker); custom objects can be
    reconstructed on the receiving side by passing ``expected_type`` to
    :func:`from_json`.
    """
    try:
        return json.dumps(obj, default=_encode_custom_object)
    except TypeError as e:
        # Preserve the original error as the cause so serialization failures are
        # easier to diagnose, while naming the offending top-level type.
        raise TypeError(
            f"Failed to serialize object of type '{type(obj).__name__}' to JSON: {e}"
        ) from e


def from_json(json_str: str | bytes | bytearray, expected_type: type | None = None) -> Any:
    """Deserialize a JSON string, optionally coercing the result to a type.

    When ``expected_type`` is ``None`` (the default) the raw parsed JSON is
    returned. For backwards compatibility, payloads carrying the legacy
    :data:`AUTO_SERIALIZED` marker are reconstructed as ``SimpleNamespace``
    instances so that in-flight orchestrations produced by older SDK versions
    continue to replay.

    When ``expected_type`` is provided, the legacy marker (if present) is
    stripped and the parsed value is coerced to ``expected_type`` -- dataclasses
    are constructed from their dict payloads, types exposing a ``from_json()``
    classmethod are reconstructed via that hook, and ``Optional``/``Union`` and
    ``list`` type hints are honored recursively. The destination type is always
    supplied by the caller; it is never read from the payload.
    """
    if expected_type is None:
        return json.loads(json_str, object_hook=_legacy_object_hook)
    raw = json.loads(json_str, object_hook=_strip_legacy_marker)
    return coerce_to_type(raw, expected_type)


def _encode_custom_object(o: Any) -> Any:
    """``default`` hook for :func:`json.dumps` that emits plain JSON.

    Called only for values the JSON encoder cannot natively serialize. Note that
    namedtuples are handled natively by the encoder (serialized as JSON arrays)
    and never reach this hook.
    """
    if dataclasses.is_dataclass(o) and not isinstance(o, type):
        return dataclasses.asdict(o)
    if isinstance(o, SimpleNamespace):
        return vars(o)
    to_json_hook = getattr(o, "to_json", None)
    if callable(to_json_hook):
        return to_json_hook()
    # This will raise a TypeError describing the unsupported type.
    raise TypeError(f"Object of type '{type(o).__name__}' is not JSON serializable")


def _legacy_object_hook(d: dict[str, Any]) -> Any:
    # If the object carries the legacy marker, deserialize it as a SimpleNamespace.
    if d.pop(AUTO_SERIALIZED, False):
        return SimpleNamespace(**d)
    return d


def _strip_legacy_marker(d: dict[str, Any]) -> dict[str, Any]:
    # Discard the legacy marker so typed coercion sees a plain dict.
    d.pop(AUTO_SERIALIZED, None)
    return d


def coerce_to_type(value: Any, expected_type: Any) -> Any:
    """Coerce an already-parsed JSON value to ``expected_type``.

    Handles ``None``/``Optional``/``Union`` and ``list`` type hints recursively,
    types exposing a ``from_json()`` classmethod, and dataclasses (including
    nested dataclass fields). The destination type is always caller-supplied and
    never derived from the payload, keeping deserialization secure.
    """
    if expected_type is None or value is None:
        return value

    origin = typing.get_origin(expected_type)
    if origin is not None:
        return _coerce_generic(value, expected_type, origin)

    if not isinstance(expected_type, type):
        # Not a concrete, instantiable type (e.g. a typing special form we don't
        # special-case) -- return the value unchanged.
        return value

    if isinstance(value, expected_type):
        return value

    from_json_hook = getattr(expected_type, "from_json", None)
    if callable(from_json_hook):
        return from_json_hook(value)

    if dataclasses.is_dataclass(expected_type) and isinstance(value, dict):
        return _build_dataclass(expected_type, cast(dict[str, Any], value))

    type_ctor = cast(Any, expected_type)
    try:
        return type_ctor(value)
    except Exception as e:
        type_name = getattr(type_ctor, "__name__", None) or str(type_ctor)
        raise TypeError(
            f"Could not coerce value of type '{type(value).__name__}' to "
            f"'{type_name}'"
        ) from e


def _coerce_generic(value: Any, expected_type: Any, origin: Any) -> Any:
    args = typing.get_args(expected_type)
    if origin is typing.Union or origin is types.UnionType:
        # Optional[T] / Union[...]: if the value already matches a member type,
        # keep it; otherwise coerce to the first non-None member.
        non_none = [a for a in args if a is not type(None)]
        for arg in non_none:
            if isinstance(arg, type) and isinstance(value, arg):
                return value
        if non_none:
            return coerce_to_type(value, non_none[0])
        return value
    if origin in (list, Sequence) and isinstance(value, list):
        elem_type = args[0] if args else None
        return [coerce_to_type(item, elem_type) for item in cast(list[Any], value)]
    # Other generics (dict, tuple, ...) are returned as parsed JSON.
    return value


def _build_dataclass(cls: Any, data: dict[str, Any]) -> Any:
    """Construct a dataclass from its dict payload, recursing into typed fields."""
    try:
        hints = typing.get_type_hints(cls)
    except Exception:
        hints = {}
    kwargs: dict[str, Any] = {}
    for field in dataclasses.fields(cls):
        if field.name not in data:
            continue
        field_type = hints.get(field.name)
        kwargs[field.name] = coerce_to_type(data[field.name], field_type)
    return cls(**kwargs)
