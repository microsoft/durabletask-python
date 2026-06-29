# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from durabletask.scheduled.schedule_status import ScheduleStatus

MINIMUM_INTERVAL = timedelta(seconds=1)


def _validate_interval(interval: timedelta) -> timedelta:
    if interval <= timedelta(0):
        raise ValueError("Interval must be positive.")
    if interval < MINIMUM_INTERVAL:
        raise ValueError("Interval must be at least 1 second.")
    return interval


def _to_iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _from_iso(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _interval_to_seconds(value: timedelta | None) -> float | None:
    return value.total_seconds() if value is not None else None


def _interval_from_seconds(value: float | None) -> timedelta | None:
    return timedelta(seconds=value) if value is not None else None


@dataclass
class ScheduleCreationOptions:
    """Options for creating a new schedule."""

    schedule_id: str
    orchestration_name: str
    interval: timedelta
    orchestration_input: Any | None = None
    orchestration_instance_id: str | None = None
    start_at: datetime | None = None
    end_at: datetime | None = None
    start_immediately_if_late: bool = False

    def __post_init__(self):
        if not self.schedule_id:
            raise ValueError("schedule_id cannot be empty.")
        if not self.orchestration_name:
            raise ValueError("orchestration_name cannot be empty.")
        _validate_interval(self.interval)

    def to_json(self) -> dict[str, Any]:
        return {
            "schedule_id": self.schedule_id,
            "orchestration_name": self.orchestration_name,
            "interval_seconds": self.interval.total_seconds(),
            "orchestration_input": self.orchestration_input,
            "orchestration_instance_id": self.orchestration_instance_id,
            "start_at": _to_iso(self.start_at),
            "end_at": _to_iso(self.end_at),
            "start_immediately_if_late": self.start_immediately_if_late,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "ScheduleCreationOptions":
        return cls(
            schedule_id=data["schedule_id"],
            orchestration_name=data["orchestration_name"],
            interval=timedelta(seconds=data["interval_seconds"]),
            orchestration_input=data.get("orchestration_input"),
            orchestration_instance_id=data.get("orchestration_instance_id"),
            start_at=_from_iso(data.get("start_at")),
            end_at=_from_iso(data.get("end_at")),
            start_immediately_if_late=bool(data.get("start_immediately_if_late", False)),
        )


@dataclass
class ScheduleUpdateOptions:
    """Options for updating an existing schedule. Only set fields are applied."""

    orchestration_name: str | None = None
    orchestration_input: Any | None = None
    orchestration_instance_id: str | None = None
    start_at: datetime | None = None
    end_at: datetime | None = None
    interval: timedelta | None = None
    start_immediately_if_late: bool | None = None

    def __post_init__(self):
        if self.interval is not None:
            _validate_interval(self.interval)

    def to_json(self) -> dict[str, Any]:
        return {
            "orchestration_name": self.orchestration_name,
            "orchestration_input": self.orchestration_input,
            "orchestration_instance_id": self.orchestration_instance_id,
            "start_at": _to_iso(self.start_at),
            "end_at": _to_iso(self.end_at),
            "interval_seconds": _interval_to_seconds(self.interval),
            "start_immediately_if_late": self.start_immediately_if_late,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "ScheduleUpdateOptions":
        return cls(
            orchestration_name=data.get("orchestration_name"),
            orchestration_input=data.get("orchestration_input"),
            orchestration_instance_id=data.get("orchestration_instance_id"),
            start_at=_from_iso(data.get("start_at")),
            end_at=_from_iso(data.get("end_at")),
            interval=_interval_from_seconds(data.get("interval_seconds")),
            start_immediately_if_late=data.get("start_immediately_if_late"),
        )


@dataclass
class ScheduleQuery:
    """Query parameters for filtering schedules."""

    DEFAULT_PAGE_SIZE = 100

    status: ScheduleStatus | None = None
    schedule_id_prefix: str | None = None
    created_from: datetime | None = None
    created_to: datetime | None = None
    page_size: int | None = None


@dataclass
class ScheduleDescription:
    """A read-only snapshot of a schedule's configuration and runtime state."""

    schedule_id: str
    orchestration_name: str | None = None
    orchestration_input: Any | None = None
    orchestration_instance_id: str | None = None
    start_at: datetime | None = None
    end_at: datetime | None = None
    interval: timedelta | None = None
    start_immediately_if_late: bool | None = None
    status: ScheduleStatus = ScheduleStatus.UNINITIALIZED
    execution_token: str = ""
    last_run_at: datetime | None = None
    next_run_at: datetime | None = None


class ScheduleConfiguration:
    """Internal configuration for a scheduled task. Persisted as part of the entity state."""

    def __init__(self, schedule_id: str, orchestration_name: str, interval: timedelta):
        if not schedule_id:
            raise ValueError("schedule_id cannot be empty.")
        if not orchestration_name:
            raise ValueError("orchestration_name cannot be empty.")
        self.schedule_id = schedule_id
        self.orchestration_name = orchestration_name
        self.interval = _validate_interval(interval)
        self.orchestration_input: Any | None = None
        self.orchestration_instance_id: str | None = None
        self.start_at: datetime | None = None
        self.end_at: datetime | None = None
        self.start_immediately_if_late: bool = False

    @staticmethod
    def from_create_options(options: ScheduleCreationOptions) -> "ScheduleConfiguration":
        config = ScheduleConfiguration(options.schedule_id, options.orchestration_name, options.interval)
        config.orchestration_input = options.orchestration_input
        config.orchestration_instance_id = options.orchestration_instance_id
        config.start_at = options.start_at
        config.end_at = options.end_at
        config.start_immediately_if_late = options.start_immediately_if_late
        config._validate()
        return config

    def update(self, options: ScheduleUpdateOptions) -> set[str]:
        """Apply the update options and return the set of changed field names."""
        updated: set[str] = set()

        if options.orchestration_name and options.orchestration_name != self.orchestration_name:
            self.orchestration_name = options.orchestration_name
            updated.add("orchestration_name")

        if options.orchestration_input is not None and options.orchestration_input != self.orchestration_input:
            self.orchestration_input = options.orchestration_input
            updated.add("orchestration_input")

        if options.orchestration_instance_id and options.orchestration_instance_id != self.orchestration_instance_id:
            self.orchestration_instance_id = options.orchestration_instance_id
            updated.add("orchestration_instance_id")

        if options.start_at is not None and options.start_at != self.start_at:
            self.start_at = options.start_at
            updated.add("start_at")

        if options.end_at is not None and options.end_at != self.end_at:
            self.end_at = options.end_at
            updated.add("end_at")

        if options.interval is not None and options.interval != self.interval:
            self.interval = _validate_interval(options.interval)
            updated.add("interval")

        if options.start_immediately_if_late is not None \
                and options.start_immediately_if_late != self.start_immediately_if_late:
            self.start_immediately_if_late = options.start_immediately_if_late
            updated.add("start_immediately_if_late")

        self._validate()
        return updated

    def _validate(self):
        if self.start_at is not None and self.end_at is not None and self.start_at > self.end_at:
            raise ValueError("start_at cannot be later than end_at.")

    def to_json(self) -> dict[str, Any]:
        return {
            "schedule_id": self.schedule_id,
            "orchestration_name": self.orchestration_name,
            "interval_seconds": self.interval.total_seconds(),
            "orchestration_input": self.orchestration_input,
            "orchestration_instance_id": self.orchestration_instance_id,
            "start_at": _to_iso(self.start_at),
            "end_at": _to_iso(self.end_at),
            "start_immediately_if_late": self.start_immediately_if_late,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "ScheduleConfiguration":
        config = cls(
            data["schedule_id"],
            data["orchestration_name"],
            timedelta(seconds=data["interval_seconds"]),
        )
        config.orchestration_input = data.get("orchestration_input")
        config.orchestration_instance_id = data.get("orchestration_instance_id")
        config.start_at = _from_iso(data.get("start_at"))
        config.end_at = _from_iso(data.get("end_at"))
        config.start_immediately_if_late = bool(data.get("start_immediately_if_late", False))
        return config


class ScheduleState:
    """Internal runtime state for a schedule. Persisted as the entity state."""

    def __init__(self):
        self.status: ScheduleStatus = ScheduleStatus.UNINITIALIZED
        self.execution_token: str = _new_token()
        self.last_run_at: datetime | None = None
        self.next_run_at: datetime | None = None
        self.schedule_created_at: datetime | None = None
        self.schedule_last_modified_at: datetime | None = None
        self.schedule_configuration: ScheduleConfiguration | None = None

    def refresh_execution_token(self):
        self.execution_token = _new_token()

    def to_json(self) -> dict[str, Any]:
        # ``schedule_configuration`` is returned as the object itself; the
        # serializer recurses into it and fires its own ``to_json`` hook. Only
        # this type's non-JSON-native leaves (datetimes) are converted here.
        return {
            "status": self.status.value,
            "execution_token": self.execution_token,
            "last_run_at": _to_iso(self.last_run_at),
            "next_run_at": _to_iso(self.next_run_at),
            "schedule_created_at": _to_iso(self.schedule_created_at),
            "schedule_last_modified_at": _to_iso(self.schedule_last_modified_at),
            "schedule_configuration": self.schedule_configuration,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "ScheduleState":
        # The nested configuration is reconstructed by calling its own
        # ``from_json`` hook directly. ``ScheduleConfiguration`` is an internal
        # type, so there is no need to route it through a (possibly custom)
        # converter -- keeping this hook converter-free means it round-trips
        # under any code path, not only the worker's threaded converter.
        state = cls()
        state.status = ScheduleStatus(data["status"])
        state.execution_token = data["execution_token"]
        state.last_run_at = _from_iso(data.get("last_run_at"))
        state.next_run_at = _from_iso(data.get("next_run_at"))
        state.schedule_created_at = _from_iso(data.get("schedule_created_at"))
        state.schedule_last_modified_at = _from_iso(data.get("schedule_last_modified_at"))
        config_data = data.get("schedule_configuration")
        state.schedule_configuration = (
            ScheduleConfiguration.from_json(config_data) if config_data is not None else None)
        return state

    def to_description(self) -> ScheduleDescription:
        config = self.schedule_configuration
        return ScheduleDescription(
            schedule_id=config.schedule_id if config else "",
            orchestration_name=config.orchestration_name if config else None,
            orchestration_input=config.orchestration_input if config else None,
            orchestration_instance_id=config.orchestration_instance_id if config else None,
            start_at=config.start_at if config else None,
            end_at=config.end_at if config else None,
            interval=config.interval if config else None,
            start_immediately_if_late=config.start_immediately_if_late if config else None,
            status=self.status,
            execution_token=self.execution_token,
            last_run_at=self.last_run_at,
            next_run_at=self.next_run_at,
        )


def _new_token() -> str:
    return uuid.uuid4().hex
