# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Shared markers for the Azure Functions Durable E2E suite."""

import pytest


azurite_delayed_visibility = pytest.mark.skip(
    reason=(
        "Azurite intermittently fails to return queue messages after an initial "
        "visibility delay; see https://github.com/Azure/Azurite/issues/2343."
    ),
)
