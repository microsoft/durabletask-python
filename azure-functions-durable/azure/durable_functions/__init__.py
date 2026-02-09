# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# This import ensures that the replacement of the global JSON encoder/decoder
# happens as soon as the durabletask.azurefunctions package is imported.
from .internal import functions_json as _

from .decorators.durable_app import Blueprint, DFApp
from .client import DurableFunctionsClient
from .orchestrator import Orchestrator

# IMPORTANT: DO NOT REMOVE. `azure-functions` relies on the presence and value of this variable
# for version detection
version = "2.x"

__all__ = ["Blueprint", "DFApp", "DurableFunctionsClient", "Orchestrator", "version"]
