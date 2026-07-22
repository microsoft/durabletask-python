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

    Overrides the string boundary (:meth:`serialize` / :meth:`deserialize`) to
    route through ``df_dumps`` / ``df_loads`` -- producing the format that the
    Durable Functions host expects.

    :meth:`coerce` and :meth:`can_reconstruct` are overridden so the codec's
    type rules apply uniformly. :meth:`coerce` round-trips the value through
    ``serialize`` / ``deserialize`` rather than the base converter's permissive
    value-level reconstruction, so a coercion is validated exactly like a wire
    payload. :meth:`can_reconstruct` returns ``True`` so type discovery always
    hands the declared type to ``deserialize`` and lets ``df_loads`` decide what
    it can reconstruct, instead of gating on the base converter's narrower
    dataclass / ``from_json`` policy.
    """

    def serialize(self, value: Any) -> str | None:
        if value is None:
            return None
        return df_dumps(value)

    def deserialize(self, data: str | None, target_type: type | None = None) -> Any:
        if data is None or data == "":
            return None
        return df_loads(data, expected_type=target_type)

    def coerce(self, value: Any, target_type: type | None = None) -> Any:
        if value is None or target_type is None:
            return value
        return self.deserialize(self.serialize(value), target_type)

    def can_reconstruct(self, target_type: Any) -> bool:
        return True


# Shared instance: the converter is stateless, so a single instance is reused
# across the per-invocation worker/client objects.
DEFAULT_FUNCTIONS_DATA_CONVERTER: FunctionsDataConverter = FunctionsDataConverter()
