# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from datetime import datetime
from logging import Logger
from types import GeneratorType
from typing import Any, Generator, Iterable, List, TypeVar

import simplejson as json

import durabletask.protos.helpers as ph
import durabletask.protos.orchestrator_service_pb2 as pb
import durabletask.task.task as task
from durabletask.task.activities import Activity, ActivityContext
from durabletask.task.orchestration import OrchestrationContext, Orchestrator
from durabletask.task.registry import Registry, get_name
from durabletask.task.task import Task

TInput = TypeVar('TInput')
TOutput = TypeVar('TOutput')


class NonDeterminismError(Exception):
    pass


class OrchestratorNotRegisteredError(ValueError):
    pass


class ActivityNotRegisteredError(ValueError):
    pass


class OrchestrationStateError(Exception):
    pass


class RuntimeOrchestrationContext(OrchestrationContext):
    _generator: Generator[Task, Any, Any] | None
    _previous_task: Task | None

    def __init__(self, instance_id: str):
        self._generator = None
        self._is_replaying = True
        self._is_complete = False
        self._result = None
        self._pending_actions = dict[int, pb.OrchestratorAction]()
        self._pending_tasks = dict[int, task.CompletableTask]()
        self._sequence_number = 0
        self._current_utc_datetime = datetime(1000, 1, 1)
        self._instance_id = instance_id

    def run(self, generator: Generator[task.Task, Any, Any]):
        self._generator = generator
        # TODO: Do something with this task
        task = next(generator)  # this starts the generator
        # TODO: Check if the task is null?
        self._previous_task = task

    def resume(self):
        if self._generator is None:
            # This is never expected unless maybe there's an issue with the history
            raise TypeError("The orchestrator generator is not initialized! Was the orchestration history corrupted?")

        # We can resume the generator only if the previously yielded task
        # has reached a completed state. The only time this won't be the
        # case is if the user yielded on a WhenAll task and there are still
        # outstanding child tasks that need to be completed.
        if self._previous_task is not None:
            if self._previous_task.is_failed:
                # Raise the failure as an exception to the generator. The orchestrator can then either
                # handle the exception or allow it to fail the orchestration.
                self._generator.throw(self._previous_task.get_exception())
            elif self._previous_task.is_complete:
                # Resume the generator. This will either return a Task or raise StopIteration if it's done.
                next_task = self._generator.send(self._previous_task.get_result())
                # TODO: Validate the return value
                self._previous_task = next_task

    def set_complete(self, result: Any):
        self._is_complete = True
        self._result = result
        result_json: str | None = None
        if result is not None:
            result_json = json.dumps(result)
        action = ph.new_complete_orchestration_action(
            self.next_sequence_number(), pb.ORCHESTRATION_STATUS_COMPLETED, result_json)
        self._pending_actions[action.id] = action

    def set_failed(self, ex: Exception):
        self._is_complete = True
        self._pending_actions.clear()  # Cancel any pending actions
        action = ph.new_complete_orchestration_action(
            self.next_sequence_number(), pb.ORCHESTRATION_STATUS_FAILED, None, ph.new_failure_details(ex)
        )
        self._pending_actions[action.id] = action

    def get_actions(self) -> List[pb.OrchestratorAction]:
        return list(self._pending_actions.values())

    def next_sequence_number(self) -> int:
        self._sequence_number += 1
        return self._sequence_number

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def current_utc_datetime(self) -> datetime:
        return self._current_utc_datetime

    @property
    def is_replaying(self) -> bool:
        return self._is_replaying

    @current_utc_datetime.setter
    def current_utc_datetime(self, value: datetime):
        self._current_utc_datetime = value

    def create_timer(self, fire_at: datetime) -> task.Task:
        id = self.next_sequence_number()
        action = ph.new_create_timer_action(id, fire_at)
        self._pending_actions[id] = action

        timer_task = task.CompletableTask()
        self._pending_tasks[id] = timer_task
        return timer_task

    def call_activity(self, activity: Activity[TInput, TOutput], *,
                      input: TInput | None = None) -> task.Task[TOutput]:
        id = self.next_sequence_number()
        name = get_name(activity)
        action = ph.new_schedule_task_action(id, name, input)
        self._pending_actions[id] = action

        activity_task = task.CompletableTask[TOutput]()
        self._pending_tasks[id] = activity_task
        return activity_task

    def call_sub_orchestrator(self, orchestrator: Orchestrator[TInput, TOutput], *,
                              input: TInput | None = None,
                              instance_id: str | None = None) -> task.Task[TOutput]:
        id = self.next_sequence_number()
        name = get_name(orchestrator)
        if instance_id is None:
            # Create a deteministic instance ID based on the parent instance ID
            instance_id = f"{self.instance_id}:{id:04x}"
        action = ph.new_create_sub_orchestration_action(id, name, instance_id, input)
        self._pending_actions[id] = action

        sub_orch_task = task.CompletableTask[TOutput]()
        self._pending_tasks[id] = sub_orch_task
        return sub_orch_task


class OrchestrationExecutor:
    _generator: Orchestrator | None

    def __init__(self, registry: Registry, logger: Logger):
        self._registry = registry
        self._logger = logger
        self._generator = None

    def execute(self, instance_id: str, old_events: Iterable[pb.HistoryEvent], new_events: Iterable[pb.HistoryEvent]) -> List[pb.OrchestratorAction]:
        if not new_events:
            raise OrchestrationStateError("The new history event list must have at least one event in it.")

        ctx = RuntimeOrchestrationContext(instance_id)

        try:
            # Rebuild local state by replaying old history into the orchestrator function
            ctx._is_replaying = True
            for old_event in old_events:
                self.process_event(ctx, old_event)

            # Get new actions by executing newly received events into the orchestrator function
            ctx._is_replaying = False
            for new_event in new_events:
                self.process_event(ctx, new_event)
        except Exception as ex:
            # Unhandled exceptions fail the orchestration
            ctx.set_failed(ex)

        return ctx.get_actions()

    def process_event(self, ctx: RuntimeOrchestrationContext, event: pb.HistoryEvent) -> None:
        try:
            if event.HasField("orchestratorStarted"):
                ctx.current_utc_datetime = event.timestamp.ToDatetime()
            elif event.HasField("executionStarted"):
                # TODO: Check if we already started the orchestration
                fn = self._registry.get_orchestrator(event.executionStarted.name)
                if fn is None:
                    raise OrchestratorNotRegisteredError(
                        f"A '{event.executionStarted.name}' orchestrator was not registered.")

                # deserialize the input, if any
                input = None
                if event.executionStarted.input is not None and event.executionStarted.input.value != "":
                    input = json.loads(event.executionStarted.input.value)

                result = fn(ctx, input)  # this does not execute the generator, only creates it
                if isinstance(result, GeneratorType):
                    # Start the orchestrator's generator function
                    ctx.run(result)
                else:
                    # This is an orchestrator that doesn't schedule any tasks
                    ctx.set_complete(result)
            elif event.HasField("timerCreated"):
                # This history event confirms that the timer was successfully scheduled.
                # Remove the timerCreated event from the pending action list so we don't schedule it again.
                timer_id = event.eventId
                action = ctx._pending_actions.pop(timer_id, None)
                if not action:
                    raise _get_non_determinism_error(timer_id, get_name(ctx.create_timer))
                elif not action.HasField("createTimer"):
                    expected_method_name = get_name(ctx.create_timer)
                    raise _get_wrong_action_type_error(timer_id, expected_method_name, action)
            elif event.HasField("timerFired"):
                timer_id = event.timerFired.timerId
                timer_task = ctx._pending_tasks.pop(timer_id, None)
                if not timer_task:
                    # TODO: Should this be an error? When would it ever happen?
                    self._logger.warning(
                        f"Ignoring unexpected timerFired event for '{ctx.instance_id}' with ID = {timer_id}.")
                    return
                timer_task.complete(None)
                ctx.resume()
            elif event.HasField("taskScheduled"):
                # This history event confirms that the activity execution was successfully scheduled.
                # Remove the taskScheduled event from the pending action list so we don't schedule it again.
                task_id = event.eventId
                action = ctx._pending_actions.pop(task_id, None)
                if not action:
                    raise _get_non_determinism_error(task_id, get_name(ctx.call_activity))
                elif not action.HasField("scheduleTask"):
                    expected_method_name = get_name(ctx.call_activity)
                    raise _get_wrong_action_type_error(task_id, expected_method_name, action)
                elif action.scheduleTask.name != event.taskScheduled.name:
                    raise _get_wrong_action_name_error(
                        task_id,
                        method_name=get_name(ctx.call_activity),
                        expected_task_name=event.taskScheduled.name,
                        actual_task_name=action.scheduleTask.name)
            elif event.HasField("taskCompleted"):
                # This history event contains the result of a completed activity task.
                task_id = event.taskCompleted.taskScheduledId
                activity_task = ctx._pending_tasks.pop(task_id, None)
                if not activity_task:
                    # TODO: Should this be an error? When would it ever happen?
                    self._logger.warning(
                        f"Ignoring unexpected taskCompleted event for '{ctx.instance_id}' with ID = {task_id}.")
                    return
                result = None
                if not ph.is_empty(event.taskCompleted.result):
                    result = json.loads(event.taskCompleted.result.value)
                activity_task.complete(result)
                ctx.resume()
            elif event.HasField("taskFailed"):
                task_id = event.taskFailed.taskScheduledId
                activity_task = ctx._pending_tasks.pop(task_id, None)
                if not activity_task:
                    # TODO: Should this be an error? When would it ever happen?
                    self._logger.warning(
                        f"Ignoring unexpected taskFailed event for '{ctx.instance_id}' with ID = {task_id}.")
                    return
                activity_task.fail(event.taskFailed.failureDetails)
                ctx.resume()
            elif event.HasField("subOrchestrationInstanceCreated"):
                # This history event confirms that the sub-orchestration execution was successfully scheduled.
                # Remove the subOrchestrationInstanceCreated event from the pending action list so we don't schedule it again.
                task_id = event.eventId
                action = ctx._pending_actions.pop(task_id, None)
                if not action:
                    raise _get_non_determinism_error(task_id, get_name(ctx.call_sub_orchestrator))
                elif not action.HasField("createSubOrchestration"):
                    expected_method_name = get_name(ctx.call_sub_orchestrator)
                    raise _get_wrong_action_type_error(task_id, expected_method_name, action)
                elif action.createSubOrchestration.name != event.subOrchestrationInstanceCreated.name:
                    raise _get_wrong_action_name_error(
                        task_id,
                        method_name=get_name(ctx.call_sub_orchestrator),
                        expected_task_name=event.subOrchestrationInstanceCreated.name,
                        actual_task_name=action.createSubOrchestration.name)
            elif event.HasField("subOrchestrationInstanceCompleted"):
                task_id = event.subOrchestrationInstanceCompleted.taskScheduledId
                sub_orch_task = ctx._pending_tasks.pop(task_id, None)
                if not sub_orch_task:
                    # TODO: Should this be an error? When would it ever happen?
                    self._logger.warning(
                        f"Ignoring unexpected subOrchestrationInstanceCompleted event for '{ctx.instance_id}' with ID = {task_id}.")
                    return
                result = None
                if not ph.is_empty(event.subOrchestrationInstanceCompleted.result):
                    result = json.loads(event.subOrchestrationInstanceCompleted.result.value)
                sub_orch_task.complete(result)
                ctx.resume()
            elif event.HasField("subOrchestrationInstanceFailed"):
                task_id = event.subOrchestrationInstanceFailed.taskScheduledId
                sub_orch_task = ctx._pending_tasks.pop(task_id, None)
                if not sub_orch_task:
                    # TODO: Should this be an error? When would it ever happen?
                    self._logger.warning(
                        f"Ignoring unexpected subOrchestrationInstanceFailed event for '{ctx.instance_id}' with ID = {task_id}.")
                    return
                sub_orch_task.fail(event.subOrchestrationInstanceFailed.failureDetails)
                ctx.resume()
            else:
                eventType = event.WhichOneof("eventType")
                raise OrchestrationStateError(f"Don't know how to handle event of type '{eventType}'")
        except StopIteration as generatorStopped:
            # The orchestrator generator function completed
            ctx.set_complete(generatorStopped.value)


class ActivityExecutor:
    def __init__(self, registry: Registry, logger: Logger):
        self._registry = registry
        self._logger = logger

    def execute(self, orchestration_id: str, name: str, task_id: int, encoded_input: str | None) -> str | None:
        """Executes an activity function and returns the serialized result, if any."""
        fn = self._registry.get_activity(name)
        if not fn:
            raise ActivityNotRegisteredError(f"Activity function named '{name}' was not registered!")

        activity_input = json.loads(encoded_input) if encoded_input else None
        ctx = ActivityContext(orchestration_id, task_id)

        # Execute the activity function
        activity_output = fn(ctx, activity_input)

        encoded_output = json.dumps(activity_output) if activity_output else None
        return encoded_output


def _get_non_determinism_error(task_id: int, action_name: str) -> NonDeterminismError:
    return NonDeterminismError(
        f"A previous execution called {action_name} with ID={task_id}, but the current "
        f"execution doesn't have this action with this ID. This problem occurs when either "
        f"the orchestration has non-deterministic logic or if the code was changed after an "
        f"instance of this orchestration already started running.")


def _get_wrong_action_type_error(task_id: int, expected_method_name: str, action: pb.OrchestratorAction) -> NonDeterminismError:
    unexpected_method_name = _get_method_name_for_action(action)
    return NonDeterminismError(
        f"Failed to restore orchestration state due to a history mismatch: A previous execution called "
        f"{expected_method_name} with ID={task_id}, but the current execution is instead trying to call "
        f"{unexpected_method_name} as part of rebuilding it's history. This kind of mismatch can happen if an "
        f"orchestration has non-deterministic logic or if the code was changed after an instance of this "
        f"orchestration already started running.")


def _get_wrong_action_name_error(task_id: int, method_name: str, expected_task_name: str, actual_task_name: str) -> NonDeterminismError:
    return NonDeterminismError(
        f"Failed to restore orchestration state due to a history mismatch: A previous execution called "
        f"{method_name} with name='{expected_task_name}' and sequence number {task_id}, but the current "
        f"execution is instead trying to call {actual_task_name} as part of rebuilding it's history. "
        f"This kind of mismatch can happen if an orchestration has non-deterministic logic or if the code "
        f"was changed after an instance of this orchestration already started running.")


def _get_method_name_for_action(action: pb.OrchestratorAction) -> str:
    # TODO: Add the following, when available:
    # "createSubOrchestration" -> get_name(OrchestrationContext.call_sub_orchestrator)
    # "sendEvent" -> get_name(OrchestrationContext.send_event)

    action_type = action.WhichOneof('orchestratorActionType')
    if action_type == "scheduleTask":
        return get_name(OrchestrationContext.call_activity)
    elif action_type == "createTimer":
        return get_name(OrchestrationContext.create_timer)
    else:
        raise NotImplementedError(f"Action type '{action_type}' not supported!")
