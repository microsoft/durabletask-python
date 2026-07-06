# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from enum import Enum
from typing import Union


class ScheduleStatus(str, Enum):
    """Represents the current status of a schedule."""

    UNINITIALIZED = "Uninitialized"
    """Schedule has not been created."""

    ACTIVE = "Active"
    """Schedule is active and running."""

    PAUSED = "Paused"
    """Schedule is paused."""

    def to_dotnet_ordinal(self) -> int:
        """Return the numeric value used by the .NET ``ScheduleStatus`` enum.

        The Durable Task Scheduler dashboard reads the persisted entity state
        with ``System.Text.Json`` (Web defaults, no string-enum converter), so
        the status must be serialized as the enum's ordinal rather than its
        name. The ordinals match the .NET SDK order (``Uninitialized`` = 0,
        ``Active`` = 1, ``Paused`` = 2).
        """
        return _STATUS_TO_ORDINAL[self]

    @classmethod
    def from_dotnet(cls, value: Union[int, str, None]) -> "ScheduleStatus":
        """Reconstruct a status from a persisted value.

        Accepts the numeric ordinal written by the .NET-compatible serializer
        as well as the legacy string name (e.g. ``"Active"``) so that states
        persisted by older Python workers still round-trip.
        """
        if isinstance(value, bool):
            # ``bool`` is a subclass of ``int``; reject it explicitly so a
            # stray boolean cannot be misread as an ordinal.
            return cls.UNINITIALIZED
        if isinstance(value, int):
            return _ORDINAL_TO_STATUS.get(value, cls.UNINITIALIZED)
        if isinstance(value, str):
            text = value.strip()
            if text.isdigit():
                return _ORDINAL_TO_STATUS.get(int(text), cls.UNINITIALIZED)
            for member in cls:
                if member.value.lower() == text.lower():
                    return member
            # The .NET Scheduler client names the zero value "Unknown"; treat
            # it as the equivalent uninitialized state.
            if text.lower() == "unknown":
                return cls.UNINITIALIZED
        return cls.UNINITIALIZED


_STATUS_TO_ORDINAL: dict["ScheduleStatus", int] = {
    ScheduleStatus.UNINITIALIZED: 0,
    ScheduleStatus.ACTIVE: 1,
    ScheduleStatus.PAUSED: 2,
}

_ORDINAL_TO_STATUS: dict[int, "ScheduleStatus"] = {
    ordinal: status for status, ordinal in _STATUS_TO_ORDINAL.items()
}
