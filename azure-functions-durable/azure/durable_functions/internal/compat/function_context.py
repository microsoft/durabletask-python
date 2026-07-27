# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Any


class FunctionContext:
    """Holds additional function-level attributes not used by Durable.

    Backwards-compatible with the v1 ``FunctionContext``, which was populated
    from any *extra* fields present in the orchestration-trigger JSON payload
    beyond those the ``DurableOrchestrationContext`` consumed directly.

    In v2 the orchestration request is a protobuf that carries no such arbitrary
    extra fields, so this is typically empty -- matching the common v1 case,
    where the base trigger payload (``instanceId``, ``parentInstanceId``,
    ``isReplaying``, ``input``, ``upperSchemaVersion``, ``history``) is fully
    consumed and nothing is left over.
    """

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)
