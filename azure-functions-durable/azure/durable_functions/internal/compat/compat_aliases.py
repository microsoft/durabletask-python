# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Any

from warnings import deprecated

from ...client import DurableFunctionsClient


@deprecated(
    "DurableOrchestrationClient is deprecated; use DurableFunctionsClient instead.")
class DurableOrchestrationClient(DurableFunctionsClient):
    """Deprecated alias for :class:`DurableFunctionsClient`."""


@deprecated(
    "The Entity class is deprecated and unsupported in v2; register entities "
    "with the entity_trigger decorator instead.")
class Entity:
    """Deprecated placeholder for the v1 ``Entity`` executor class.

    Entities in v2 are registered with the ``entity_trigger`` decorator and
    executed by the durabletask worker; there is no user-facing ``Entity``
    class. This placeholder is retained only so existing imports do not fail.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        raise NotImplementedError(
            "The Entity class is not supported in v2. Register entities with "
            "the entity_trigger decorator instead.")
