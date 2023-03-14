import inspect
import simplejson as json

from datetime import datetime
from types import GeneratorType
from typing import Any, Generator, List

import durabletask.protos.helpers as ph
import durabletask.protos.orchestrator_service_pb2 as pb
import durabletask.task.task as task

from durabletask.task.orchestrator import OrchestrationContext, Orchestrator
from durabletask.task.registry import Registry
from durabletask.task.task import Task


class NonDeterminismError(Exception):
    pass


class OrchestratorNotFound(ValueError):
    pass


class OrchestrationStateError(Exception):
    pass


class RuntimeOrchestrationContext(OrchestrationContext):
    _generator: Generator[Task, Any, Any] | None
    _previous_task: Task | None

    def __init__(self):
        self._generator = None
        self._is_replaying = True
        self._is_complete = False
        self._result = None
        self._pending_actions = dict[int, pb.OrchestratorAction]()
        self._pending_tasks = dict[int, task.CompletableTask]()
        self._sequence_number = 0
        self._current_utc_datetime = datetime(1000, 1, 1)

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
        if self._previous_task is not None and self._previous_task.is_complete():
            # Resume the generator. This will either return a Task or raise StopIteration if it's done.
            next_task = self._generator.send(self._previous_task.get_result())
            # TODO: Validate the return value
            self._previous_task = next_task

    def set_complete(self, result: Any):
        self._is_complete = True
        self._result = result
        result_json: str | None = None
        if result != None:
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
    def current_utc_datetime(self) -> datetime:
        return self._current_utc_datetime

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


class OrchestrationExecutor:
    generator: Orchestrator | None

    def __init__(self, registry: Registry):
        self.registry = registry
        self.generator = None

    def execute(self, instance_id: str, old_Events: List[pb.HistoryEvent], new_events: List[pb.HistoryEvent]) -> List[pb.OrchestratorAction]:
        if new_events is None or len(new_events) == 0:
            raise OrchestrationStateError(
                "The new history event list must have at least one event in it.")

        ctx = RuntimeOrchestrationContext()

        try:
            # Rebuild local state by replaying old history into the orchestrator function
            ctx._is_replaying = True
            for old_event in old_Events:
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
                fn = self.registry.get_orchestrator(event.executionStarted.name)
                if fn is None:
                    raise OrchestratorNotFound(f"A '{event.executionStarted.name}' orchestrator was not registered.")
                result = fn(ctx)  # this does not execute the generator, only creates it
                if isinstance(result, GeneratorType):
                    # Start the orchestrator's generator function
                    ctx.run(result)
                else:
                    # This is an orchestrator that doesn't schedule any tasks
                    ctx.set_complete(result)
            elif event.HasField("timerCreated"):
                id = event.eventId
                if ctx._pending_actions.pop(id, None) is None:
                    raise NonDeterminismError(inspect.cleandoc(f"""
                        A previous execution called create_timer with sequence number {id}, but the current
                        execution doesn't have this action with this sequence number. This problem occurs
                        when either the orchestration has non-deterministic logic or if the code was changed
                        after an instance of this orchestration already started running."""))
            elif event.HasField("timerFired"):
                id = event.timerFired.timerId
                timer_task = ctx._pending_tasks.pop(id, None)
                if timer_task is None:
                    # TODO: This could be a duplicate event or it could be a non-deterministic orchestration.
                    #       Duplicate events should be handled gracefully with a warning. Otherwise, the
                    #       orchestration should probably fail with an error.
                    return
                timer_task.complete(None)
                ctx.resume()
            else:
                eventType = event.WhichOneof("eventType")
                raise OrchestrationStateError(f"Don't know how to handle event of type '{eventType}'")
        except StopIteration as generatorStopped:
            # The orchestrator generator function completed
            ctx.set_complete(generatorStopped.value)
