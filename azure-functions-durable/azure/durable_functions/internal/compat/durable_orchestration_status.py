# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
from datetime import datetime, timezone
from typing import Any, Optional, cast

from durabletask.client import OrchestrationState, OrchestrationStatus

from .orchestration_runtime_status import (
    OrchestrationRuntimeStatus,
    from_durabletask_status,
    to_durabletask_status,
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

    def __init__(
            self,
            state: Optional[OrchestrationState] = None,
            history: Optional[list[Any]] = None):
        self._state = state
        self._include_input = True
        self._history = history

    @classmethod
    def from_orchestration_state(
            cls,
            state: Optional[OrchestrationState],
            *,
            include_input: bool = True,
            history: Optional[list[Any]] = None) -> "DurableOrchestrationStatus":
        """Wrap a durabletask ``OrchestrationState`` (or ``None``)."""
        status = cls(state, history)
        status._include_input = include_input
        return status

    @classmethod
    def from_json(cls, json_obj: Any) -> "DurableOrchestrationStatus":
        """Reconstruct a status from its v1 JSON representation.

        Accepts the dictionary produced by :meth:`to_json` (or the equivalent v1
        schema); a JSON string is parsed first. The wrapped
        ``OrchestrationState`` is rebuilt so the resulting object exposes the
        same attribute surface as one returned by the client.
        """
        if isinstance(json_obj, str):
            json_obj = json.loads(json_obj)
        data = dict(json_obj)

        runtime_status = data.get("runtimeStatus")
        dt_status = (
            to_durabletask_status(OrchestrationRuntimeStatus(runtime_status))
            if runtime_status is not None else None)

        def _parse_datetime(value: Any) -> Any:
            return datetime.fromisoformat(value) if isinstance(value, str) else value

        def _reserialize(value: Any) -> Optional[str]:
            return None if value is None else json.dumps(value)

        state = OrchestrationState(
            instance_id=cast(str, data.get("instanceId")),
            name=cast(str, data.get("name")),
            runtime_status=cast(OrchestrationStatus, dt_status),
            created_at=cast(datetime, _parse_datetime(data.get("createdTime"))),
            last_updated_at=cast(datetime, _parse_datetime(data.get("lastUpdatedTime"))),
            serialized_input=_reserialize(data.get("input")),
            serialized_output=_reserialize(data.get("output")),
            serialized_custom_status=_reserialize(data.get("customStatus")),
            failure_details=None,
        )
        history = data.get("historyEvents", data.get("history"))
        return cls(state, history)

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
        if self._state is None or not self._include_input:
            return None
        return self._raw_payload(self._state.serialized_input)

    @property
    def output(self) -> Any:
        """Get the output or failure message of the orchestration instance."""
        if self._state is None:
            return None
        output = self._raw_payload(self._state.serialized_output)
        if output is not None:
            return output
        failure = self._state.failure_details
        return failure.message if failure is not None else None

    @property
    def runtime_status(self) -> Optional[OrchestrationRuntimeStatus]:
        """Get the runtime status as a v1 ``OrchestrationRuntimeStatus``."""
        # ``OrchestrationState.runtime_status`` is typed ``OrchestrationStatus``
        # but is built via ``cast`` from a value that can be ``None`` at runtime
        if self._state is None or self._state.runtime_status is None:  # pyright: ignore[reportUnnecessaryComparison]
            return None
        return from_durabletask_status(self._state.runtime_status)

    @property
    def custom_status(self) -> Any:
        """Get the (deserialized) custom status payload, if any."""
        if self._state is None:
            return None
        return self._raw_payload(self._state.serialized_custom_status)

    @property
    def history(self) -> Optional[list[Any]]:
        """Get the execution history.

        The history is populated only when requested by passing
        ``show_history=True`` to the client's compatibility ``get_status`` API.
        """
        return self._history

    def to_json(self) -> dict[str, Any]:
        """Convert this status into a v1-compatible JSON dictionary.

        Payload fields (``output``, ``input``, ``customStatus``) are emitted as
        their raw JSON representation rather than the reconstructed Python
        objects, so the result is always JSON-serializable even when the
        orchestration payloads are custom types.
        """
        result: dict[str, Any] = {}
        if self.name is not None:
            result["name"] = self.name
        if self.instance_id is not None:
            result["instanceId"] = self.instance_id
        if self.created_time is not None:
            result["createdTime"] = self._format_datetime(self.created_time)
        if self.last_updated_time is not None:
            result["lastUpdatedTime"] = self._format_datetime(self.last_updated_time)
        output = self.output
        if output is not None:
            result["output"] = output
        input_ = self.input_
        if input_ is not None:
            result["input"] = input_
        if self.runtime_status is not None:
            result["runtimeStatus"] = self.runtime_status.name
        custom_status = self._raw_payload(
            self._state.serialized_custom_status if self._state is not None else None)
        if custom_status is not None:
            result["customStatus"] = custom_status
        if self.history is not None:
            result["historyEvents"] = self.history
        return result

    @staticmethod
    def _raw_payload(serialized: Optional[str]) -> Any:
        """Parse a serialized payload as plain JSON without reconstructing types.

        Returns the parsed JSON value (which is always JSON-serializable), or the
        original string if it is not valid JSON, or ``None`` when absent.
        """
        if serialized is None:
            return None
        try:
            return json.loads(serialized)
        except (TypeError, ValueError):
            return serialized

    @staticmethod
    def _format_datetime(value: datetime) -> str:
        """Format a datetime the way v1 did: UTC, microseconds, trailing ``Z``.

        v1 emitted ``%Y-%m-%dT%H:%M:%S.%fZ``; matching it keeps strict v1
        consumers that parse with that exact format working.
        """
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
