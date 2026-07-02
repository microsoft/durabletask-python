# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Any, Optional

from typing_extensions import deprecated

from durabletask.entities import EntityContext
from durabletask.task import OrchestrationContext

from ...client import DurableFunctionsClient
from .token_source import TokenSource


@deprecated(
    "DurableOrchestrationClient is deprecated; use DurableFunctionsClient instead.")
class DurableOrchestrationClient(DurableFunctionsClient):
    """Deprecated alias for :class:`DurableFunctionsClient`."""


@deprecated(
    "DurableOrchestrationContext is deprecated; use "
    "durabletask.task.OrchestrationContext instead.")
class DurableOrchestrationContext(OrchestrationContext):
    """Deprecated alias for :class:`durabletask.task.OrchestrationContext`.

    Retained so v1 type hints (``def my_orchestrator(context: DurableOrchestrationContext)``)
    continue to import. At runtime the durabletask worker passes an
    ``OrchestrationContext`` instance.
    """

    def call_http(self,
                  method: str,
                  uri: str,
                  content: Optional[str] = None,
                  headers: Optional[dict[str, str]] = None,
                  token_source: Optional[TokenSource] = None,
                  is_raw_str: bool = False) -> Any:
        """Schedule a durable HTTP call (v1 API).

        Not yet supported: durabletask has no durable-HTTP (``call_http``)
        equivalent, so this raises ``NotImplementedError``. It is present to
        document the v1 API surface and the current gap.
        """
        raise NotImplementedError(
            "call_http is not yet supported by durabletask. The durable-HTTP "
            "API (and its TokenSource auth) has no durabletask equivalent yet.")


@deprecated(
    "DurableEntityContext is deprecated; use "
    "durabletask.entities.EntityContext instead.")
class DurableEntityContext(EntityContext):
    """Deprecated alias for :class:`durabletask.entities.EntityContext`."""


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
