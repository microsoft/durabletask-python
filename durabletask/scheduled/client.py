# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging

from durabletask.client import (EntityQuery, OrchestrationStatus,
                                TaskHubGrpcClient)
from durabletask.entities import EntityInstanceId
from durabletask.scheduled import transitions
from durabletask.scheduled.exceptions import ScheduleNotFoundError
from durabletask.scheduled.models import (ScheduleCreationOptions,
                                          ScheduleDescription, ScheduleQuery,
                                          ScheduleState, ScheduleUpdateOptions)
from durabletask.scheduled.orchestrator import (
    ScheduleOperationRequest, execute_schedule_operation_orchestrator)
from durabletask.scheduled.schedule_entity import (DELETE_OPERATION,
                                                   ENTITY_NAME)

logger = logging.getLogger("durabletask.scheduled")


class ScheduleClient:
    """Client for managing a single schedule instance."""

    def __init__(self, client: TaskHubGrpcClient, schedule_id: str,
                 *, operation_timeout: float = 60):
        if not schedule_id:
            raise ValueError("schedule_id cannot be empty.")
        self._client = client
        self._schedule_id = schedule_id
        self._entity_id = EntityInstanceId(ENTITY_NAME, schedule_id)
        self._operation_timeout = operation_timeout

    @property
    def schedule_id(self) -> str:
        """Gets the ID of this schedule."""
        return self._schedule_id

    def _run_operation(self, operation_name: str, input: object | None = None) -> None:
        request = ScheduleOperationRequest(
            entity_id=str(self._entity_id),
            operation_name=operation_name,
            input=input,
        )
        instance_id = self._client.schedule_new_orchestration(
            execute_schedule_operation_orchestrator, input=request)
        state = self._client.wait_for_orchestration_completion(
            instance_id, timeout=self._operation_timeout)
        if state is None or state.runtime_status != OrchestrationStatus.COMPLETED:
            failure = state.failure_details if state else None
            message = failure.message if failure else "unknown error"
            raise RuntimeError(
                f"Failed to '{operation_name}' schedule '{self._schedule_id}': {message}")

    def create(self, options: ScheduleCreationOptions) -> None:
        """Create or update this schedule with the given configuration."""
        self._run_operation(transitions.CREATE_SCHEDULE, options)

    def update(self, options: ScheduleUpdateOptions) -> None:
        """Update this schedule's configuration."""
        self._run_operation(transitions.UPDATE_SCHEDULE, options)

    def pause(self) -> None:
        """Pause this schedule."""
        self._run_operation(transitions.PAUSE_SCHEDULE)

    def resume(self) -> None:
        """Resume this schedule."""
        self._run_operation(transitions.RESUME_SCHEDULE)

    def delete(self) -> None:
        """Delete this schedule."""
        self._run_operation(DELETE_OPERATION)

    def describe(self) -> ScheduleDescription:
        """Retrieve the current details of this schedule."""
        metadata = self._client.get_entity(self._entity_id, include_state=True)
        if metadata is None:
            raise ScheduleNotFoundError(self._schedule_id)
        state = metadata.get_typed_state(ScheduleState)
        if state is None:
            raise ScheduleNotFoundError(self._schedule_id)
        return state.to_description()


class ScheduledTaskClient:
    """Client for managing scheduled tasks in a Durable Task application."""

    def __init__(self, client: TaskHubGrpcClient, *, operation_timeout: float = 60):
        self._client = client
        self._operation_timeout = operation_timeout

    def get_schedule_client(self, schedule_id: str) -> ScheduleClient:
        """Get a handle to manage a specific schedule."""
        return ScheduleClient(self._client, schedule_id,
                              operation_timeout=self._operation_timeout)

    def create_schedule(self, options: ScheduleCreationOptions) -> ScheduleClient:
        """Create a new schedule and return a client for managing it."""
        schedule_client = self.get_schedule_client(options.schedule_id)
        schedule_client.create(options)
        return schedule_client

    def get_schedule(self, schedule_id: str) -> ScheduleDescription | None:
        """Get a schedule description by ID, or None if it does not exist."""
        try:
            return self.get_schedule_client(schedule_id).describe()
        except ScheduleNotFoundError:
            return None

    def list_schedules(self, schedule_query: ScheduleQuery | None = None) -> list[ScheduleDescription]:
        """List schedules matching the given filter criteria."""
        prefix = schedule_query.schedule_id_prefix if schedule_query and schedule_query.schedule_id_prefix else ""
        page_size = (schedule_query.page_size if schedule_query and schedule_query.page_size
                     else ScheduleQuery.DEFAULT_PAGE_SIZE)
        query = EntityQuery(
            instance_id_starts_with=f"@{ENTITY_NAME}@{prefix}",
            include_state=True,
            page_size=page_size,
        )
        results: list[ScheduleDescription] = []
        for metadata in self._client.get_all_entities(query):
            state = metadata.get_typed_state(ScheduleState)
            if state is None or state.schedule_configuration is None:
                continue
            if not self._matches_filter(state, schedule_query):
                continue
            results.append(state.to_description())
        return results

    @staticmethod
    def _matches_filter(state: ScheduleState, schedule_query: ScheduleQuery | None) -> bool:
        if schedule_query is None:
            return True
        if schedule_query.status is not None and state.status != schedule_query.status:
            return False
        created_at = state.schedule_created_at
        if schedule_query.created_from is not None and not (created_at and created_at > schedule_query.created_from):
            return False
        if schedule_query.created_to is not None and not (created_at and created_at < schedule_query.created_to):
            return False
        return True
