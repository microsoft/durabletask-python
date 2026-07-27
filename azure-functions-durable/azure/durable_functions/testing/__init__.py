# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit-testing utilities for Azure Durable Functions.

These helpers let you exercise orchestrator business logic in a plain unit test
without a running Functions host or a Durable Task backend. They mirror the
``azure.durable_functions.testing`` surface from the v1 SDK so existing tests
keep working against v2.x.
"""

from .orchestrator_generator_wrapper import orchestrator_generator_wrapper

__all__ = [
    "orchestrator_generator_wrapper",
]
