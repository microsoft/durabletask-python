# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# This module intentionally re-exports deprecated v1 compatibility aliases.
# pyright: reportDeprecated=false

from durabletask.task import RetryPolicy

from .decorators.durable_app import Blueprint, DFApp
from .client import DurableFunctionsClient
from .http.models import DurableHttpRequest, DurableHttpResponse
from .orchestrator import Orchestrator
from .internal.compat.retry_options import RetryOptions
from .internal.compat.orchestration_runtime_status import OrchestrationRuntimeStatus
from .internal.compat.durable_orchestration_status import DurableOrchestrationStatus
from .internal.compat.purge_history_result import PurgeHistoryResult
from .internal.compat.entity_state_response import EntityStateResponse
from .internal.compat.entity_id import EntityId
from .internal.compat.token_source import ManagedIdentityTokenSource, TokenSource
from .internal.compat.orchestration_context import DurableOrchestrationContext
from .internal.compat.entity_context import DurableEntityContext
from .internal.compat.compat_aliases import (
    DurableOrchestrationClient,
    Entity,
)
from .internal.converters import register_durable_converters

# Register this package's binding converters with azure-functions, overriding
# the SDK's built-in durable converters. The SDK exposes ``register_converter``
# for exactly this purpose; importing this package (which the Functions host
# does when it loads a durable app) installs the durabletask-based converters
# before the host indexes any functions.
register_durable_converters()

__all__ = [
    "Blueprint",
    "DFApp",
    "DurableEntityContext",
    "DurableFunctionsClient",
    "DurableHttpRequest",
    "DurableHttpResponse",
    "DurableOrchestrationClient",
    "DurableOrchestrationContext",
    "DurableOrchestrationStatus",
    "Entity",
    "EntityId",
    "EntityStateResponse",
    "ManagedIdentityTokenSource",
    "Orchestrator",
    "OrchestrationRuntimeStatus",
    "PurgeHistoryResult",
    "RetryOptions",
    "RetryPolicy",
    "TokenSource",
    "version",
]
