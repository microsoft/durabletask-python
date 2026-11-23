# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import asyncio
from typing import cast

import durabletask.internal.orchestrator_service_pb2 as pb


def is_control_flow_exception(error: BaseException) -> bool:
    control_flow_types = (asyncio.CancelledError, GeneratorExit, KeyboardInterrupt, SystemExit)
    if isinstance(error, BaseExceptionGroup):
        group = cast(BaseExceptionGroup[BaseException], error)
        return group.subgroup(control_flow_types) is not None
    return isinstance(error, control_flow_types)


class VersionFailureException(Exception):
    def __init__(self, error_details: pb.TaskFailureDetails) -> None:
        super().__init__()
        self.error_details = error_details


class AbandonOrchestrationError(Exception):
    pass


class PayloadSizeExceededError(ValueError):
    """A raw payload exceeds the configured external storage limit."""


class PayloadValidationError(ValueError):
    """A payload path would change its selected storage destination."""
