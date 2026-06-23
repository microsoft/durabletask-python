# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from .internal.functions_json import install_custom_serialization
from .decorators.durable_app import Blueprint, DFApp
from .client import DurableFunctionsClient
from .orchestrator import Orchestrator

# Ensure the durabletask JSON encoder/decoder is replaced as soon as the
# durable_functions package is imported.
install_custom_serialization()

# IMPORTANT: DO NOT REMOVE. `azure-functions` relies on the presence and value of this variable
# for version detection
version = "2.x"

__all__ = ["Blueprint", "DFApp", "DurableFunctionsClient", "Orchestrator", "version"]
