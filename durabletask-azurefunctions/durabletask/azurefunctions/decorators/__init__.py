# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Durable Task SDK for Python entities component"""

import durabletask.azurefunctions.decorators.durable_app as durable_app
import durabletask.azurefunctions.decorators.metadata as metadata

__all__ = ["durable_app", "metadata"]

PACKAGE_NAME = "durabletask.entities"
