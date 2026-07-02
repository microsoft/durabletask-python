# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from datetime import datetime
from typing import Any, Optional

from durabletask.client import OrchestrationState

from .orchestration_runtime_status import (
    OrchestrationRuntimeStatus,
    from_durabletask_status,
)


class DurableOrchestrationStatus:
    """Represents the status of a durable orchestration instance.

    Backwards-compatible wrapper around the durabletask
    :class:`~durabletask.client.OrchestrationState`. It exposes the v1
    ``DurableOrchestrationStatus`` attribute surface so existing code that reads
    ``status.runtime_status``, ``status.output``, ``status.input_``, etc. keeps
    working. New code should use ``OrchestrationState`` directly.

    A status wrapping ``None`` (i.e. a non-existent instance) is falsy, matching
    the v1 behaviour where ``get_status`` never returned ``None``.
    """

    def __init__(self, state: Optional[OrchestrationState] = None):
        self._state = state

    @classmethod
    def from_orchestration_state(
            cls, state: Optional[OrchestrationState]) -> "DurableOrchestrationStatus":
        """Wrap a durabletask ``OrchestrationState`` (or ``None``)."""
        return cls(state)

    def __bool__(self) -> bool:
        return self._state is not None

    @property
    def orchestration_state(self) -> Optional[OrchestrationState]:
        """Get the underlying durabletask ``OrchestrationState`` (or ``None``)."""
        return self._state

    @property
    def name(self) -> Optional[str]:
        """Get the orchestrator function name."""
        return self._state.name if self._state is not None else None

    @property
    def instance_id(self) -> Optional[str]:
        """Get the unique ID of the instance."""
        return self._state.instance_id if self._state is not None else None

    @property
    def created_time(self) -> Optional[datetime]:
        """Get the time at which the orchestration instance was created."""
        return self._state.created_at if self._state is not None else None

    @property
    def last_updated_time(self) -> Optional[datetime]:
        """Get the time at which the orchestration instance last updated."""
        return self._state.last_updated_at if self._state is not None else None

    @property
    def input_(self) -> Any:
        """Get the (deserialized) input of the orchestration instance."""
        return self._state.get_input() if self._state is not None else None

    @property
    def output(self) -> Any:
        """Get the (deserialized) output of the orchestration instance."""
        return self._state.get_output() if self._state is not None else None

    @property
    def runtime_status(self) -> Optional[OrchestrationRuntimeStatus]:
        """Get the runtime status as a v1 ``OrchestrationRuntimeStatus``."""
        if self._state is None:
            return None
        return from_durabletask_status(self._state.runtime_status)

    @property
    def custom_status(self) -> Any:
        """Get the (deserialized) custom status payload, if any."""
        return self._state.get_custom_status() if self._state is not None else None

    @property
    def history(self) -> Optional[list[Any]]:
        """Get the execution history.

        History is not available through this compatibility path and is always
        ``None``; use ``get_orchestration_history`` on the client instead.
        """
        return None

    def to_json(self) -> dict[str, Any]:
        """Convert this status into a v1-compatible JSON dictionary."""
        result: dict[str, Any] = {}
        if self.name is not None:
            result["name"] = self.name
        if self.instance_id is not None:
            result["instanceId"] = self.instance_id
        if self.created_time is not None:
            result["createdTime"] = self.created_time.isoformat()
        if self.last_updated_time is not None:
            result["lastUpdatedTime"] = self.last_updated_time.isoformat()
        if self.output is not None:
            result["output"] = self.output
        if self.input_ is not None:
            result["input"] = self.input_
        if self.runtime_status is not None:
            result["runtimeStatus"] = self.runtime_status.name
        if self.custom_status is not None:
            result["customStatus"] = self.custom_status
        return result
