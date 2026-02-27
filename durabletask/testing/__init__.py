# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Testing utilities for the Durable Task Python SDK."""

from durabletask.testing.in_memory_backend import (
    InMemoryOrchestrationBackend,
    create_test_backend,
)

__all__ = [
    "InMemoryOrchestrationBackend",
    "create_test_backend",
]
