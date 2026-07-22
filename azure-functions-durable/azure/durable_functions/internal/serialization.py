# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Azure Functions payload serialization for Durable Task.

Bridges durabletask's pluggable :class:`~durabletask.serialization.DataConverter`
to the azure-functions SDK's centralized ``df_dumps`` / ``df_loads`` serializers
so that payloads round-trip through the **exact** wire format the Durable
Functions host extension (and the SDK's ``ActivityTriggerConverter``) expect.
"""

from __future__ import annotations
from azure.functions._durable_functions import df_dumps, df_loads

import logging
from typing import Any

from durabletask.serialization import JsonDataConverter

logger = logging.getLogger("azure.functions.DurableFunctions")


class FunctionsDataConverter(JsonDataConverter):
    """:class:`DataConverter` that serializes via azure-functions' codec.

    Overrides only the string boundary (:meth:`serialize` / :meth:`deserialize`)
    to route through ``df_dumps`` / ``df_loads`` -- producing the
    ``{"__class__", "__module__", "__data__"}`` envelope that the Durable
    Functions host expects -- while inheriting :class:`JsonDataConverter`'s
    value-level :meth:`coerce` and reconstruction policy
    (:meth:`can_reconstruct`), which operate on already-parsed values and are
    wire-format agnostic.
    """

    def serialize(self, value: Any) -> str | None:
        if value is None:
            return None
        return df_dumps(value)

    def deserialize(self, data: str | None, target_type: type | None = None) -> Any:
        if data is None or data == "":
            return None
        return df_loads(data, expected_type=target_type)


# Shared instance: the converter is stateless, so a single instance is reused
# across the per-invocation worker/client objects.
DEFAULT_FUNCTIONS_DATA_CONVERTER: FunctionsDataConverter = FunctionsDataConverter()
