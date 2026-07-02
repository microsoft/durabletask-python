# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# This module intentionally re-exports deprecated v1 compatibility aliases.
# pyright: reportDeprecated=false

from durabletask.task import RetryPolicy

from .decorators.durable_app import Blueprint, DFApp
from .client import DurableFunctionsClient
from .orchestrator import Orchestrator
from .internal.compat.retry_options import RetryOptions
from .internal.compat.orchestration_runtime_status import OrchestrationRuntimeStatus
from .internal.compat.durable_orchestration_status import DurableOrchestrationStatus
from .internal.compat.purge_history_result import PurgeHistoryResult
from .internal.compat.entity_state_response import EntityStateResponse
from .internal.compat.entity_id import EntityId
from .internal.compat.token_source import ManagedIdentityTokenSource, TokenSource
from .internal.compat.compat_aliases import (
    DurableEntityContext,
    DurableOrchestrationClient,
    DurableOrchestrationContext,
    Entity,
)

# IMPORTANT: DO NOT REMOVE. `azure-functions` relies on the presence and value of this variable
# for version detection
version = "2.x"

__all__ = [
    "Blueprint",
    "DFApp",
    "DurableEntityContext",
    "DurableFunctionsClient",
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
