# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from durabletask.internal.helpers import ensure_aware
from durabletask.scheduled.schedule_status import ScheduleStatus
from durabletask.serialization import DataConverter, JsonDataConverter

MINIMUM_INTERVAL = timedelta(seconds=1)

# Serializer used to (de)serialize the orchestration input to/from a JSON string
# for persistence. Matches the .NET SDK, which stores ``OrchestrationInput`` as a
# string so the Durable Task Scheduler dashboard can read the raw entity state.
_INPUT_CONVERTER = JsonDataConverter()


def _validate_interval(interval: timedelta) -> timedelta:
    if interval <= timedelta(0):
        raise ValueError("Interval must be positive.")
    if interval < MINIMUM_INTERVAL:
        raise ValueError("Interval must be at least 1 second.")
    return interval


def _to_iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _from_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    # .NET serializes ``DateTimeOffset`` with a numeric offset, but tolerate a
    # trailing ``Z`` so states written by other producers still parse on the
    # Python versions that predate ``fromisoformat`` accepting ``Z``. Only the
    # trailing designator is normalized so an interior ``Z`` is left untouched.
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def _interval_to_seconds(value: timedelta | None) -> float | None:
    return value.total_seconds() if value is not None else None


def _interval_from_seconds(value: float | None) -> timedelta | None:
    return timedelta(seconds=value) if value is not None else None


# Number of 100-nanosecond ticks per second, matching the .NET ``TimeSpan``
# resolution used when formatting the fractional component.
_TICKS_PER_SECOND = 10_000_000


def _interval_to_timespan(value: timedelta | None) -> str | None:
    """Format a ``timedelta`` as a .NET ``TimeSpan`` (``[-][d.]hh:mm:ss[.fffffff]``).

    The Durable Task Scheduler dashboard deserializes the schedule interval into
    a .NET ``TimeSpan``, whose JSON converter only accepts this constant format.
    """
    if value is None:
        return None
    negative = value < timedelta(0)
    value = abs(value)
    days = value.days
    hours, remainder = divmod(value.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if days:
        formatted = f"{days}.{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        formatted = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    if value.microseconds:
        ticks = value.microseconds * 10  # microseconds -> 100-ns ticks
        formatted += f".{ticks:07d}"
    return f"-{formatted}" if negative else formatted


def _interval_from_timespan(value: str) -> timedelta:
    """Parse a .NET ``TimeSpan`` string (``[-][d.]hh:mm:ss[.fffffff]``)."""
    text = value.strip()
    negative = text.startswith("-")
    if negative:
        text = text[1:]

    fraction = 0.0
    if "." in text:
        head, _, tail = text.rpartition(".")
        # A dot before the first ``:`` is the day separator, not a fraction.
        if ":" in tail:
            # e.g. ``1.02:03:04`` -- the ``.`` separates days from the clock.
            days_part, _, clock = text.partition(".")
            days = int(days_part)
            hours, minutes, seconds = (int(p) for p in clock.split(":"))
        else:
            # ``head`` holds ``[d.]hh:mm:ss`` and ``tail`` the ticks fraction.
            fraction = int(tail.ljust(7, "0")[:7]) / _TICKS_PER_SECOND
            days, hours, minutes, seconds = _split_clock(head)
    else:
        days, hours, minutes, seconds = _split_clock(text)

    result = timedelta(days=days, hours=hours, minutes=minutes,
                       seconds=seconds) + timedelta(seconds=fraction)
    return -result if negative else result


def _split_clock(text: str) -> tuple[int, int, int, int]:
    """Split ``[d.]hh:mm:ss`` into ``(days, hours, minutes, seconds)``."""
    days = 0
    if "." in text:
        days_part, _, text = text.partition(".")
        days = int(days_part)
    hours, minutes, seconds = (int(part) for part in text.split(":"))
    return days, hours, minutes, seconds


def _get(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Return the first present key from ``data``.

    Reads tolerate both the .NET-compatible PascalCase keys and the legacy
    snake_case keys written by earlier Python workers.
    """
    for key in keys:
        if key in data:
            return data[key]
    return default


def _parse_interval(data: dict[str, Any]) -> timedelta:
    """Read the interval from either the .NET ``Interval`` or legacy field."""
    timespan = _get(data, "Interval", "interval")
    if isinstance(timespan, str):
        return _interval_from_timespan(timespan)
    seconds = _get(data, "interval_seconds")
    if seconds is not None:
        return timedelta(seconds=seconds)
    raise KeyError("interval")


def _encode_orchestration_input(value: Any) -> str | None:
    """Serialize the orchestration input to a JSON string for persistence.

    The Durable Task Scheduler dashboard (and the .NET SDK) model the persisted
    ``OrchestrationInput`` as a string, so the raw input object is serialized to
    a JSON string here and parsed back by :func:`_decode_orchestration_input`.
    """
    if value is None:
        return None
    return _INPUT_CONVERTER.serialize(value)


def _decode_orchestration_input(value: Any) -> Any:
    """Reconstruct the raw orchestration input from its persisted JSON string."""
    if value is None or not isinstance(value, str):
        return value
    try:
        return _INPUT_CONVERTER.deserialize(value)
    except (ValueError, TypeError):
        # Not a JSON document (an unexpected shape); return it unchanged rather
        # than failing to load the schedule.
        return value


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

    def __post_init__(self):
        # Coerce the time-window bounds to timezone-aware UTC. Schedule
        # timestamps are always stored as aware UTC, so normalizing here ensures
        # a naive bound supplied by a caller can never reach the filter
        # comparison and raise "can't compare offset-naive and offset-aware".
        self.created_from = ensure_aware(self.created_from)
        self.created_to = ensure_aware(self.created_to)


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
        # Serialized with .NET-compatible property names and value shapes so the
        # Durable Task Scheduler dashboard can deserialize the raw entity state:
        # PascalCase keys and the interval as a .NET ``TimeSpan`` string.
        return {
            "ScheduleId": self.schedule_id,
            "OrchestrationName": self.orchestration_name,
            "Interval": _interval_to_timespan(self.interval),
            "OrchestrationInput": _encode_orchestration_input(self.orchestration_input),
            "OrchestrationInstanceId": self.orchestration_instance_id,
            "StartAt": _to_iso(self.start_at),
            "EndAt": _to_iso(self.end_at),
            "StartImmediatelyIfLate": self.start_immediately_if_late,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "ScheduleConfiguration":
        config = cls(
            _get(data, "ScheduleId", "schedule_id"),
            _get(data, "OrchestrationName", "orchestration_name"),
            _parse_interval(data),
        )
        if "OrchestrationInput" in data:
            # New .NET-compatible states store the input as a JSON string.
            config.orchestration_input = _decode_orchestration_input(data["OrchestrationInput"])
        else:
            # Legacy states stored the raw (already-parsed) input object.
            config.orchestration_input = data.get("orchestration_input")
        config.orchestration_instance_id = _get(
            data, "OrchestrationInstanceId", "orchestration_instance_id")
        config.start_at = _from_iso(_get(data, "StartAt", "start_at"))
        config.end_at = _from_iso(_get(data, "EndAt", "end_at"))
        config.start_immediately_if_late = bool(
            _get(data, "StartImmediatelyIfLate", "start_immediately_if_late", default=False))
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
        # serializer recurses into it and fires its own ``to_json`` hook. Keys
        # and value shapes mirror the .NET ``ScheduleState`` so the Durable Task
        # Scheduler dashboard can deserialize the raw entity state: PascalCase
        # names, the status as its numeric ordinal, and datetimes as ISO strings.
        return {
            "Status": self.status.to_dotnet_ordinal(),
            "ExecutionToken": self.execution_token,
            "LastRunAt": _to_iso(self.last_run_at),
            "NextRunAt": _to_iso(self.next_run_at),
            "ScheduleCreatedAt": _to_iso(self.schedule_created_at),
            "ScheduleLastModifiedAt": _to_iso(self.schedule_last_modified_at),
            "ScheduleConfiguration": self.schedule_configuration,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any],
                  converter: DataConverter | None = None) -> "ScheduleState":
        # Reads accept both the .NET-compatible and legacy snake_case shapes.
        #
        # The nested ``ScheduleConfiguration`` must round-trip under any
        # conforming converter. Converters differ in how they hand nested
        # custom objects to this hook:
        #   * The default JSON converter leaves nested values as plain dicts and
        #     expects the parent hook to rebuild them; it passes itself as
        #     ``converter`` (a hook that declares the parameter opts in) so the
        #     reconstruction can defer to ``converter.coerce``.
        #   * The Azure Functions ``df`` codec rebuilds nested ``to_json`` /
        #     ``from_json`` envelopes bottom-up, so it invokes this hook with the
        #     configuration *already* reconstructed (and without a converter).
        # Handle both: skip reconstruction when it is already a
        # ``ScheduleConfiguration``, otherwise route a raw dict through the
        # converter when one was supplied, falling back to the hook directly.
        state = cls()
        state.status = ScheduleStatus.from_dotnet(_get(data, "Status", "status"))
        # Preserve the token generated by ``__init__`` when the field is absent;
        # overwriting it with ``None`` would make every ``run_schedule`` signal
        # look stale and silently stop the schedule.
        token = _get(data, "ExecutionToken", "execution_token")
        if token is not None:
            state.execution_token = token
        state.last_run_at = _from_iso(_get(data, "LastRunAt", "last_run_at"))
        state.next_run_at = _from_iso(_get(data, "NextRunAt", "next_run_at"))
        state.schedule_created_at = _from_iso(_get(data, "ScheduleCreatedAt", "schedule_created_at"))
        state.schedule_last_modified_at = _from_iso(
            _get(data, "ScheduleLastModifiedAt", "schedule_last_modified_at"))
        config_data = _get(data, "ScheduleConfiguration", "schedule_configuration")
        state.schedule_configuration = _rebuild_configuration(config_data, converter)
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


def _rebuild_configuration(
        config_data: Any, converter: DataConverter | None) -> "ScheduleConfiguration | None":
    """Reconstruct a nested ``ScheduleConfiguration`` for any converter.

    ``config_data`` may be ``None``, an already-reconstructed
    ``ScheduleConfiguration`` (codecs that rebuild nested envelopes bottom-up,
    e.g. the Azure Functions ``df`` codec), or a plain mapping (the default JSON
    converter, which leaves nested values as dicts). When a ``converter`` is
    supplied its ``coerce`` drives reconstruction; otherwise the hook is called
    directly.
    """
    if config_data is None or isinstance(config_data, ScheduleConfiguration):
        return config_data
    if converter is not None:
        return converter.coerce(config_data, ScheduleConfiguration)
    return ScheduleConfiguration.from_json(config_data)
