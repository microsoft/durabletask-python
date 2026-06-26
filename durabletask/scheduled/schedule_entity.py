# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from durabletask.entities import DurableEntity, EntityInstanceId
from durabletask.scheduled import transitions
from durabletask.scheduled.exceptions import ScheduleInvalidTransitionError
from durabletask.scheduled.models import (ScheduleConfiguration,
                                          ScheduleCreationOptions, ScheduleState,
                                          ScheduleUpdateOptions)
from durabletask.scheduled.schedule_status import ScheduleStatus

ENTITY_NAME = "schedule"
"""The lowercased entity name used for schedule entity instances."""

DELETE_OPERATION = "delete"
RUN_SCHEDULE_OPERATION = "run_schedule"

logger = logging.getLogger("durabletask.scheduled")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


class Schedule(DurableEntity):
    """Entity that manages the state and execution of a scheduled task.

    The Schedule entity maintains the configuration and runtime state of a scheduled
    task, handling operations like creation, updates, pausing/resuming, and executing
    the target orchestration according to the defined schedule.
    """

    def _load_state(self) -> ScheduleState:
        raw = self.get_state()
        if raw is None:
            return ScheduleState()
        if isinstance(raw, SimpleNamespace):
            raw = vars(raw)
        if isinstance(raw, dict):
            return ScheduleState.from_dict(raw)
        raise TypeError(f"Unexpected schedule state type: {type(raw).__name__}")

    def _save_state(self, state: ScheduleState) -> None:
        self.set_state(state.to_dict())

    def _entity_id(self, schedule_id: str) -> EntityInstanceId:
        return EntityInstanceId(ENTITY_NAME, schedule_id)

    def _can_transition_to(self, state: ScheduleState, operation_name: str,
                           target_status: ScheduleStatus) -> bool:
        return transitions.is_valid_transition(operation_name, state.status, target_status)

    def create_schedule(self, options: ScheduleCreationOptions) -> None:
        """Create a new schedule. If one already exists, update it in place."""
        state = self._load_state()

        if not self._can_transition_to(state, transitions.CREATE_SCHEDULE, ScheduleStatus.ACTIVE):
            raise ScheduleInvalidTransitionError(
                options.schedule_id if options else "", state.status, ScheduleStatus.ACTIVE,
                transitions.CREATE_SCHEDULE)

        already_exists = state.schedule_created_at is not None
        state.schedule_configuration = ScheduleConfiguration.from_create_options(options)

        if already_exists:
            state.schedule_last_modified_at = _now()
            state.refresh_execution_token()
            state.next_run_at = None
        else:
            state.status = ScheduleStatus.ACTIVE
            state.schedule_created_at = state.schedule_last_modified_at = _now()

        logger.info(f"Created schedule '{state.schedule_configuration.schedule_id}'.")
        self._save_state(state)

        # Signal to run the schedule and let run_schedule decide whether to run now or later.
        self.signal_entity(
            self._entity_id(state.schedule_configuration.schedule_id),
            RUN_SCHEDULE_OPERATION,
            state.execution_token,
        )

    def update_schedule(self, options: ScheduleUpdateOptions) -> None:
        """Update an existing schedule's configuration."""
        state = self._load_state()

        if not self._can_transition_to(state, transitions.UPDATE_SCHEDULE, state.status):
            raise ScheduleInvalidTransitionError(
                state.schedule_configuration.schedule_id if state.schedule_configuration else "",
                state.status, state.status, transitions.UPDATE_SCHEDULE)

        if state.schedule_configuration is None:
            raise ValueError("Schedule configuration is missing.")

        updated_fields = state.schedule_configuration.update(options)
        if not updated_fields:
            logger.debug("Schedule configuration is already up to date.")
            self._save_state(state)
            return

        state.schedule_last_modified_at = _now()

        if updated_fields & {"start_at", "interval", "start_immediately_if_late"}:
            state.next_run_at = None

        state.refresh_execution_token()
        logger.info(f"Updated schedule '{state.schedule_configuration.schedule_id}'.")
        self._save_state(state)

        if state.status == ScheduleStatus.ACTIVE:
            self.signal_entity(
                self._entity_id(state.schedule_configuration.schedule_id),
                RUN_SCHEDULE_OPERATION,
                state.execution_token,
            )

    def pause_schedule(self, _: Any = None) -> None:
        """Pause the schedule."""
        state = self._load_state()
        schedule_id = state.schedule_configuration.schedule_id if state.schedule_configuration else ""

        if not self._can_transition_to(state, transitions.PAUSE_SCHEDULE, ScheduleStatus.PAUSED):
            raise ScheduleInvalidTransitionError(
                schedule_id, state.status, ScheduleStatus.PAUSED, transitions.PAUSE_SCHEDULE)

        if state.schedule_configuration is None:
            raise ValueError("Schedule configuration is missing.")

        state.status = ScheduleStatus.PAUSED
        state.next_run_at = None
        state.refresh_execution_token()
        logger.info(f"Paused schedule '{schedule_id}'.")
        self._save_state(state)

    def resume_schedule(self, _: Any = None) -> None:
        """Resume a paused schedule."""
        state = self._load_state()
        schedule_id = state.schedule_configuration.schedule_id if state.schedule_configuration else ""

        if not self._can_transition_to(state, transitions.RESUME_SCHEDULE, ScheduleStatus.ACTIVE):
            raise ScheduleInvalidTransitionError(
                schedule_id, state.status, ScheduleStatus.ACTIVE, transitions.RESUME_SCHEDULE)

        if state.schedule_configuration is None:
            raise ValueError("Schedule configuration is missing.")

        state.status = ScheduleStatus.ACTIVE
        state.next_run_at = None
        logger.info(f"Resumed schedule '{schedule_id}'.")
        self._save_state(state)

        self.signal_entity(
            self._entity_id(schedule_id),
            RUN_SCHEDULE_OPERATION,
            state.execution_token,
        )

    def run_schedule(self, execution_token: str) -> None:
        """Heartbeat operation: starts the target orchestration when due and re-arms itself."""
        state = self._load_state()

        if state.status == ScheduleStatus.UNINITIALIZED:
            # This signal is no longer useful since the schedule was deleted.
            self.set_state(None)
            return

        config = state.schedule_configuration
        if config is None:
            raise ValueError("Schedule configuration is missing.")

        if execution_token != state.execution_token:
            logger.debug(f"Ignoring stale run signal for schedule '{config.schedule_id}'.")
            return

        if state.status != ScheduleStatus.ACTIVE:
            raise ValueError("Schedule must be in Active status to run.")

        end_at = _ensure_aware(config.end_at)
        if end_at is not None and _now() > end_at:
            logger.info(f"Schedule '{config.schedule_id}' has passed its end time; deleting.")
            state.next_run_at = None
            self._save_state(state)
            self.signal_entity(self._entity_id(config.schedule_id), DELETE_OPERATION)
            return

        state.next_run_at = self._determine_next_run_time(state, config)

        if state.next_run_at <= _now():
            self._start_orchestration(config, state.next_run_at)
            state.last_run_at = state.next_run_at
            state.next_run_at = None
            state.next_run_at = self._determine_next_run_time(state, config)

        self._save_state(state)

        self.signal_entity(
            self._entity_id(config.schedule_id),
            RUN_SCHEDULE_OPERATION,
            state.execution_token,
            signal_time=state.next_run_at,
        )

    def delete(self, _: Any = None) -> None:
        """Delete the schedule entity."""
        self.set_state(None)

    def _start_orchestration(self, config: ScheduleConfiguration, scheduled_run_time: datetime) -> None:
        instance_id = config.orchestration_instance_id
        if not instance_id:
            instance_id = f"{config.schedule_id}-{scheduled_run_time.isoformat()}"

        logger.info(
            f"Starting orchestration '{config.orchestration_name}' with instance ID '{instance_id}' "
            f"for schedule '{config.schedule_id}'.")
        self.schedule_new_orchestration(
            config.orchestration_name,
            config.orchestration_input,
            instance_id=instance_id,
        )

    def _determine_next_run_time(self, state: ScheduleState, config: ScheduleConfiguration) -> datetime:
        if state.next_run_at is not None:
            return _ensure_aware(state.next_run_at)  # type: ignore[return-value]

        now = _now()
        start_time = _ensure_aware(config.start_at) or _ensure_aware(state.schedule_created_at) or now
        time_since_start = now - start_time

        # Next run is in the future relative to the start time.
        if time_since_start.total_seconds() < 0:
            return start_time

        is_first_run = state.last_run_at is None
        if is_first_run and config.start_immediately_if_late:
            return now

        intervals_elapsed = int(time_since_start.total_seconds() // config.interval.total_seconds())
        return start_time + config.interval * (intervals_elapsed + 1)
