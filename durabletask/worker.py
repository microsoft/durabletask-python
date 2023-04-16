# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import concurrent.futures
import logging
from datetime import datetime
from threading import Event, Thread
from types import GeneratorType
from typing import Any, Dict, Generator, List, Sequence, TypeVar

import grpc
import simplejson as json
from google.protobuf import empty_pb2

import durabletask.internal.helpers as ph
import durabletask.internal.helpers as pbh
import durabletask.internal.orchestrator_service_pb2 as pb
import durabletask.internal.orchestrator_service_pb2_grpc as stubs
import durabletask.internal.shared as shared
from durabletask import task

TInput = TypeVar('TInput')
TOutput = TypeVar('TOutput')


class _Registry:

    orchestrators: Dict[str, task.Orchestrator]
    activities: Dict[str, task.Activity]

    def __init__(self):
        self.orchestrators = dict[str, task.Orchestrator]()
        self.activities = dict[str, task.Activity]()

    def add_orchestrator(self, fn: task.Orchestrator) -> str:
        if fn is None:
            raise ValueError('An orchestrator function argument is required.')

        name = task.get_name(fn)
        self.add_named_orchestrator(name, fn)
        return name

    def add_named_orchestrator(self, name: str, fn: task.Orchestrator) -> None:
        if not name:
            raise ValueError('A non-empty orchestrator name is required.')
        if name in self.orchestrators:
            raise ValueError(f"A '{name}' orchestrator already exists.")

        self.orchestrators[name] = fn

    def get_orchestrator(self, name: str) -> task.Orchestrator | None:
        return self.orchestrators.get(name)

    def add_activity(self, fn: task.Activity) -> str:
        if fn is None:
            raise ValueError('An activity function argument is required.')

        name = task.get_name(fn)
        self.add_named_activity(name, fn)
        return name

    def add_named_activity(self, name: str, fn: task.Activity) -> None:
        if not name:
            raise ValueError('A non-empty activity name is required.')
        if name in self.activities:
            raise ValueError(f"A '{name}' activity already exists.")

        self.activities[name] = fn

    def get_activity(self, name: str) -> task.Activity | None:
        return self.activities.get(name)


class OrchestratorNotRegisteredError(ValueError):
    """Raised when attempting to start an orchestration that is not registered"""
    pass


class ActivityNotRegisteredError(ValueError):
    """Raised when attempting to call an activity that is not registered"""
    pass


class TaskHubGrpcWorker:
    _response_stream: grpc.Future | None

    def __init__(self, *,
                 host_address: str | None = None,
                 log_handler=None,
                 log_formatter: logging.Formatter | None = None):
        self._registry = _Registry()
        self._host_address = host_address if host_address else shared.get_default_host_address()
        self._logger = shared.get_logger(log_handler, log_formatter)
        self._shutdown = Event()
        self._response_stream = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def add_orchestrator(self, fn: task.Orchestrator) -> str:
        """Registers an orchestrator function with the worker."""
        return self._registry.add_orchestrator(fn)

    def add_activity(self, fn: task.Activity) -> str:
        """Registers an activity function with the worker."""
        return self._registry.add_activity(fn)

    def start(self):
        """Starts the worker on a background thread and begins listening for work items."""
        channel = shared.get_grpc_channel(self._host_address)
        stub = stubs.TaskHubSidecarServiceStub(channel)

        def run_loop():
            # TODO: Investigate whether asyncio could be used to enable greater concurrency for async activity
            #       functions. We'd need to know ahead of time whether a function is async or not.
            # TODO: Max concurrency configuration settings
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
                while not self._shutdown.is_set():
                    try:
                        # send a "Hello" message to the sidecar to ensure that it's listening
                        stub.Hello(empty_pb2.Empty())

                        # stream work items
                        self._response_stream = stub.GetWorkItems(pb.GetWorkItemsRequest())
                        self._logger.info(f'Successfully connected to {self._host_address}. Waiting for work items...')

                        # The stream blocks until either a work item is received or the stream is canceled
                        # by another thread (see the stop() method).
                        for work_item in self._response_stream:
                            request_type = work_item.WhichOneof('request')
                            self._logger.debug(f'Received "{request_type}" work item')
                            if work_item.HasField('orchestratorRequest'):
                                executor.submit(self._execute_orchestrator, work_item.orchestratorRequest, stub)
                            elif work_item.HasField('activityRequest'):
                                executor.submit(self._execute_activity, work_item.activityRequest, stub)
                            else:
                                self._logger.warning(f'Unexpected work item type: {request_type}')

                    except grpc.RpcError as rpc_error:
                        if rpc_error.code() == grpc.StatusCode.CANCELLED:  # type: ignore
                            self._logger.warning(f'Disconnected from {self._host_address}')
                        elif rpc_error.code() == grpc.StatusCode.UNAVAILABLE:  # type: ignore
                            self._logger.warning(
                                f'The sidecar at address {self._host_address} is unavailable - will continue retrying')
                        else:
                            self._logger.warning(f'Unexpected error: {rpc_error}')
                    except Exception as ex:
                        self._logger.warning(f'Unexpected error: {ex}')

                    # CONSIDER: exponential backoff
                    self._shutdown.wait(5)
                self._logger.info("No longer listening for work items")
                return

        self._logger.info(f"starting gRPC worker that connects to {self._host_address}")
        self._runLoop = Thread(target=run_loop)
        self._runLoop.start()

    def stop(self):
        """Stops the worker and waits for any pending work items to complete."""
        self._logger.info("Stopping gRPC worker...")
        self._shutdown.set()
        if self._response_stream is not None:
            self._response_stream.cancel()
        if self._runLoop is not None:
            self._runLoop.join(timeout=30)
        self._logger.info("Worker shutdown completed")

    def _execute_orchestrator(self, req: pb.OrchestratorRequest, stub: stubs.TaskHubSidecarServiceStub):
        try:
            executor = _OrchestrationExecutor(self._registry, self._logger)
            actions = executor.execute(req.instanceId, req.pastEvents, req.newEvents)
            res = pb.OrchestratorResponse(instanceId=req.instanceId, actions=actions)
        except Exception as ex:
            self._logger.exception(f"An error occurred while trying to execute instance '{req.instanceId}': {ex}")
            failure_details = pbh.new_failure_details(ex)
            actions = [pbh.new_complete_orchestration_action(-1, pb.ORCHESTRATION_STATUS_FAILED, "", failure_details)]
            res = pb.OrchestratorResponse(instanceId=req.instanceId, actions=actions)

        try:
            stub.CompleteOrchestratorTask(res)
        except Exception as ex:
            self._logger.exception(f"Failed to deliver orchestrator response for '{req.instanceId}' to sidecar: {ex}")

    def _execute_activity(self, req: pb.ActivityRequest, stub: stubs.TaskHubSidecarServiceStub):
        instance_id = req.orchestrationInstance.instanceId
        try:
            executor = _ActivityExecutor(self._registry, self._logger)
            result = executor.execute(instance_id, req.name, req.taskId, req.input.value)
            res = pb.ActivityResponse(
                instanceId=instance_id,
                taskId=req.taskId,
                result=pbh.get_string_value(result))
        except Exception as ex:
            res = pb.ActivityResponse(
                instanceId=instance_id,
                taskId=req.taskId,
                failureDetails=pbh.new_failure_details(ex))

        try:
            stub.CompleteActivityTask(res)
        except Exception as ex:
            self._logger.exception(
                f"Failed to deliver activity response for '{req.name}#{req.taskId}' of orchestration ID '{instance_id}' to sidecar: {ex}")


class _RuntimeOrchestrationContext(task.OrchestrationContext):
    _generator: Generator[task.Task, Any, Any] | None
    _previous_task: task.Task | None

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
        self._completion_status: pb.OrchestrationStatus | None = None

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

    def call_activity(self, activity: task.Activity[TInput, TOutput], *,
                      input: TInput | None = None) -> task.Task[TOutput]:
        id = self.next_sequence_number()
        name = task.get_name(activity)
        action = ph.new_schedule_task_action(id, name, input)
        self._pending_actions[id] = action

        activity_task = task.CompletableTask[TOutput]()
        self._pending_tasks[id] = activity_task
        return activity_task

    def call_sub_orchestrator(self, orchestrator: task.Orchestrator[TInput, TOutput], *,
                              input: TInput | None = None,
                              instance_id: str | None = None) -> task.Task[TOutput]:
        id = self.next_sequence_number()
        name = task.get_name(orchestrator)
        if instance_id is None:
            # Create a deteministic instance ID based on the parent instance ID
            instance_id = f"{self.instance_id}:{id:04x}"
        action = ph.new_create_sub_orchestration_action(id, name, instance_id, input)
        self._pending_actions[id] = action

        sub_orch_task = task.CompletableTask[TOutput]()
        self._pending_tasks[id] = sub_orch_task
        return sub_orch_task


class _OrchestrationExecutor:
    _generator: task.Orchestrator | None

    def __init__(self, registry: _Registry, logger: logging.Logger):
        self._registry = registry
        self._logger = logger
        self._generator = None

    def execute(self, instance_id: str, old_events: Sequence[pb.HistoryEvent], new_events: Sequence[pb.HistoryEvent]) -> List[pb.OrchestratorAction]:
        if not new_events:
            raise task.OrchestrationStateError("The new history event list must have at least one event in it.")

        ctx = _RuntimeOrchestrationContext(instance_id)
        try:
            # Rebuild local state by replaying old history into the orchestrator function
            self._logger.debug(f"{instance_id}: Rebuilding local state with {len(old_events)} history event...")
            ctx._is_replaying = True
            for old_event in old_events:
                self.process_event(ctx, old_event)

            # Get new actions by executing newly received events into the orchestrator function
            if self._logger.level <= logging.DEBUG:
                summary = _get_new_event_summary(new_events)
                self._logger.debug(f"{instance_id}: Processing {len(new_events)} new event(s): {summary}")
            ctx._is_replaying = False
            for new_event in new_events:
                self.process_event(ctx, new_event)
        except Exception as ex:
            # Unhandled exceptions fail the orchestration
            ctx.set_failed(ex)

        if ctx._completion_status:
            completion_status_str = pbh.get_orchestration_status_str(ctx._completion_status)
            self._logger.info(f"{instance_id}: Orchestration completed with status: {completion_status_str}")

        actions = ctx.get_actions()
        if self._logger.level <= logging.DEBUG:
            self._logger.debug(f"{instance_id}: Returning {len(actions)} action(s): {_get_action_summary(actions)}")
        return actions

    def process_event(self, ctx: _RuntimeOrchestrationContext, event: pb.HistoryEvent) -> None:
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
                    raise _get_non_determinism_error(timer_id, task.get_name(ctx.create_timer))
                elif not action.HasField("createTimer"):
                    expected_method_name = task.get_name(ctx.create_timer)
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
                    raise _get_non_determinism_error(task_id, task.get_name(ctx.call_activity))
                elif not action.HasField("scheduleTask"):
                    expected_method_name = task.get_name(ctx.call_activity)
                    raise _get_wrong_action_type_error(task_id, expected_method_name, action)
                elif action.scheduleTask.name != event.taskScheduled.name:
                    raise _get_wrong_action_name_error(
                        task_id,
                        method_name=task.get_name(ctx.call_activity),
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
                    raise _get_non_determinism_error(task_id, task.get_name(ctx.call_sub_orchestrator))
                elif not action.HasField("createSubOrchestration"):
                    expected_method_name = task.get_name(ctx.call_sub_orchestrator)
                    raise _get_wrong_action_type_error(task_id, expected_method_name, action)
                elif action.createSubOrchestration.name != event.subOrchestrationInstanceCreated.name:
                    raise _get_wrong_action_name_error(
                        task_id,
                        method_name=task.get_name(ctx.call_sub_orchestrator),
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
                raise task.OrchestrationStateError(f"Don't know how to handle event of type '{eventType}'")
        except StopIteration as generatorStopped:
            # The orchestrator generator function completed
            ctx.set_complete(generatorStopped.value)


class _ActivityExecutor:
    def __init__(self, registry: _Registry, logger: logging.Logger):
        self._registry = registry
        self._logger = logger

    def execute(self, orchestration_id: str, name: str, task_id: int, encoded_input: str | None) -> str | None:
        """Executes an activity function and returns the serialized result, if any."""
        self._logger.debug(f"{orchestration_id}/{task_id}: Executing activity '{name}'...")
        fn = self._registry.get_activity(name)
        if not fn:
            raise ActivityNotRegisteredError(f"Activity function named '{name}' was not registered!")

        activity_input = json.loads(encoded_input) if encoded_input else None
        ctx = task.ActivityContext(orchestration_id, task_id)

        # Execute the activity function
        activity_output = fn(ctx, activity_input)

        encoded_output = json.dumps(activity_output) if activity_output is not None else None
        chars = len(encoded_output) if encoded_output else 0
        self._logger.debug(
            f"{orchestration_id}/{task_id}: Activity '{name}' completed successfully with {chars} char(s) of encoded output.")
        return encoded_output


def _get_non_determinism_error(task_id: int, action_name: str) -> task.NonDeterminismError:
    return task.NonDeterminismError(
        f"A previous execution called {action_name} with ID={task_id}, but the current "
        f"execution doesn't have this action with this ID. This problem occurs when either "
        f"the orchestration has non-deterministic logic or if the code was changed after an "
        f"instance of this orchestration already started running.")


def _get_wrong_action_type_error(
        task_id: int,
        expected_method_name: str,
        action: pb.OrchestratorAction) -> task.NonDeterminismError:
    unexpected_method_name = _get_method_name_for_action(action)
    return task.NonDeterminismError(
        f"Failed to restore orchestration state due to a history mismatch: A previous execution called "
        f"{expected_method_name} with ID={task_id}, but the current execution is instead trying to call "
        f"{unexpected_method_name} as part of rebuilding it's history. This kind of mismatch can happen if an "
        f"orchestration has non-deterministic logic or if the code was changed after an instance of this "
        f"orchestration already started running.")


def _get_wrong_action_name_error(
        task_id: int,
        method_name: str,
        expected_task_name: str,
        actual_task_name: str) -> task.NonDeterminismError:
    return task.NonDeterminismError(
        f"Failed to restore orchestration state due to a history mismatch: A previous execution called "
        f"{method_name} with name='{expected_task_name}' and sequence number {task_id}, but the current "
        f"execution is instead trying to call {actual_task_name} as part of rebuilding it's history. "
        f"This kind of mismatch can happen if an orchestration has non-deterministic logic or if the code "
        f"was changed after an instance of this orchestration already started running.")


def _get_method_name_for_action(action: pb.OrchestratorAction) -> str:
    action_type = action.WhichOneof('orchestratorActionType')
    if action_type == "scheduleTask":
        return task.get_name(task.OrchestrationContext.call_activity)
    elif action_type == "createTimer":
        return task.get_name(task.OrchestrationContext.create_timer)
    elif action_type == "createSubOrchestration":
        return task.get_name(task.OrchestrationContext.call_sub_orchestrator)
    # elif action_type == "sendEvent":
    #    return task.get_name(task.OrchestrationContext.send_event)
    else:
        raise NotImplementedError(f"Action type '{action_type}' not supported!")


def _get_new_event_summary(new_events: Sequence[pb.HistoryEvent]) -> str:
    """Returns a summary of the new events that can be used for logging."""
    if not new_events:
        return "[]"
    elif len(new_events) == 1:
        return f"[{new_events[0].WhichOneof('eventType')}]"
    else:
        counts = dict[str, int]()
        for event in new_events:
            event_type = event.WhichOneof('eventType')
            counts[event_type] = counts.get(event_type, 0) + 1
        return f"[{', '.join(f'{name}={count}' for name, count in counts.items())}]"


def _get_action_summary(new_actions: Sequence[pb.OrchestratorAction]) -> str:
    """Returns a summary of the new actions that can be used for logging."""
    if not new_actions:
        return "[]"
    elif len(new_actions) == 1:
        return f"[{new_actions[0].WhichOneof('orchestratorActionType')}]"
    else:
        counts = dict[str, int]()
        for action in new_actions:
            action_type = action.WhichOneof('orchestratorActionType')
            counts[action_type] = counts.get(action_type, 0) + 1
        return f"[{', '.join(f'{name}={count}' for name, count in counts.items())}]"
