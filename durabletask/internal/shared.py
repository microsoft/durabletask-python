import logging

import grpc


def get_default_host_address() -> str:
    return "localhost:4001"


def get_grpc_channel(host_address: str | None) -> grpc.Channel:
    if host_address is None:
        host_address = get_default_host_address()
    channel = grpc.insecure_channel(host_address)
    return channel


def get_logger(log_handler: logging.Handler | None = None, log_formatter: logging.Formatter | None = None) -> logging.Logger:
    logger = logging.Logger("durabletask")

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
