# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# See https://peps.python.org/pep-0563/
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Callable, Generator, Generic, List, TypeVar, Union

import durabletask.internal.helpers as pbh
import durabletask.internal.orchestrator_service_pb2 as pb

T = TypeVar('T')
TInput = TypeVar('TInput')
TOutput = TypeVar('TOutput')


class OrchestrationContext(ABC):

    @property
    @abstractmethod
    def instance_id(self) -> str:
        """Get the ID of the current orchestration instance.

        The instance ID is generated and fixed when the orchestrator function
        is scheduled. It can be either auto-generated, in which case it is
        formatted as a UUID, or it can be user-specified with any format.

        Returns
        -------
        str
            The ID of the current orchestration instance.
        """
        pass

    @property
    @abstractmethod
    def current_utc_datetime(self) -> datetime:
        """Get the current date/time as UTC.

        This date/time value is derived from the orchestration history. It
        always returns the same value at specific points in the orchestrator
        function code, making it deterministic and safe for replay.

        Returns
        -------
        datetime
            The current timestamp in a way that is safe for use by orchestrator functions
        """
        pass

    @property
    @abstractmethod
    def is_replaying(self) -> bool:
        """Get the value indicating whether the orchestrator is replaying from history.

        This property is useful when there is logic that needs to run only when
        the orchestrator function is _not_ replaying. For example, certain
        types of application logging may become too noisy when duplicated as
        part of orchestrator function replay. The orchestrator code could check
        to see whether the function is being replayed and then issue the log
        statements when this value is `false`.

        Returns
        -------
        bool
            Value indicating whether the orchestrator function is currently replaying.
        """
        pass

    @abstractmethod
    def create_timer(self, fire_at: Union[datetime, timedelta]) -> Task:
        """Create a Timer Task to fire after at the specified deadline.

        Parameters
        ----------
        fire_at: datetime.datetime | datetime.timedelta
            The time for the timer to trigger or a time delta from now.

        Returns
        -------
        Task
            A Durable Timer Task that schedules the timer to wake up the orchestrator
        """
        pass

    @abstractmethod
    def call_activity(self, activity: Union[Activity[TInput, TOutput], str], *,
                      input: Union[TInput, None] = None) -> Task[TOutput]:
        """Schedule an activity for execution.

        Parameters
        ----------
        activity: Union[Activity[TInput, TOutput], str]
            A reference to the activity function to call.
        input: Union[TInput, None]
            The JSON-serializable input (or None) to pass to the activity.
        return_type: task.Task[TOutput]
            The JSON-serializable output type to expect from the activity result.

        Returns
        -------
        Task
            A Durable Task that completes when the called activity function completes or fails.
        """
        pass

    @abstractmethod
    def call_sub_orchestrator(self, orchestrator: Orchestrator[TInput, TOutput], *,
                              input: Union[TInput, None] = None,
                              instance_id: Union[str, None] = None) -> Task[TOutput]:
        """Schedule sub-orchestrator function for execution.

        Parameters
        ----------
        orchestrator: Orchestrator[TInput, TOutput]
            A reference to the orchestrator function to call.
        input: Union[TInput, None]
            The optional JSON-serializable input to pass to the orchestrator function.
        instance_id: Union[str, None]
            A unique ID to use for the sub-orchestration instance. If not specified, a
            random UUID will be used.

        Returns
        -------
        Task
            A Durable Task that completes when the called sub-orchestrator completes or fails.
        """
        pass

    # TOOD: Add a timeout parameter, which allows the task to be canceled if the event is
    # not received within the specified timeout. This requires support for task cancellation.
    @abstractmethod
    def wait_for_external_event(self, name: str) -> Task:
        """Wait asynchronously for an event to be raised with the name `name`.

        Parameters
        ----------
        name : str
            The event name of the event that the task is waiting for.

        Returns
        -------
        Task[TOutput]
            A Durable Task that completes when the event is received.
        """
        pass

    @abstractmethod
    def continue_as_new(self, new_input: Any, *, save_events: bool = False) -> None:
        """Continue the orchestration execution as a new instance.

        Parameters
        ----------
        new_input : Any
            The new input to use for the new orchestration instance.
        save_events : bool
            A flag indicating whether to add any unprocessed external events in the new orchestration history.
        """
        pass


class FailureDetails:
    def __init__(self, message: str, error_type: str, stack_trace: Union[str, None]):
        self._message = message
        self._error_type = error_type
        self._stack_trace = stack_trace

    @property
    def message(self) -> str:
        return self._message

    @property
    def error_type(self) -> str:
        return self._error_type

    @property
    def stack_trace(self) -> Union[str, None]:
        return self._stack_trace


class TaskFailedError(Exception):
    """Exception type for all orchestration task failures."""

    def __init__(self, message: str, details: pb.TaskFailureDetails):
        super().__init__(message)
        self._details = FailureDetails(
            details.errorMessage,
            details.errorType,
            details.stackTrace.value if not pbh.is_empty(details.stackTrace) else None)

    @property
    def details(self) -> FailureDetails:
        return self._details


class NonDeterminismError(Exception):
    pass


class OrchestrationStateError(Exception):
    pass


class Task(ABC, Generic[T]):
    """Abstract base class for asynchronous tasks in a durable orchestration."""
    _result: T
    _exception: Union[TaskFailedError, None]
    _parent: Union[CompositeTask[T], None]

    def __init__(self) -> None:
        super().__init__()
        self._is_complete = False
        self._exception = None
        self._parent = None

    @property
    def is_complete(self) -> bool:
        """Returns True if the task has completed, False otherwise."""
        return self._is_complete

    @property
    def is_failed(self) -> bool:
        """Returns True if the task has failed, False otherwise."""
        return self._exception is not None

    def get_result(self) -> T:
        """Returns the result of the task."""
        if not self._is_complete:
            raise ValueError('The task has not completed.')
        elif self._exception is not None:
            raise self._exception
        return self._result

    def get_exception(self) -> TaskFailedError:
        """Returns the exception that caused the task to fail."""
        if self._exception is None:
            raise ValueError('The task has not failed.')
        return self._exception


class CompositeTask(Task[T]):
    """A task that is composed of other tasks."""
    _tasks: List[Task]

    def __init__(self, tasks: List[Task]):
        super().__init__()
        self._tasks = tasks
        self._completed_tasks = 0
        self._failed_tasks = 0
        for task in tasks:
            task._parent = self
            if task.is_complete:
                self.on_child_completed(task)

    def get_tasks(self) -> List[Task]:
        return self._tasks

    @abstractmethod
    def on_child_completed(self, task: Task[T]):
        pass


class CompletableTask(Task[T]):

    def __init__(self):
        super().__init__()

    def complete(self, result: T):
        if self._is_complete:
            raise ValueError('The task has already completed.')
        self._result = result
        self._is_complete = True
        if self._parent is not None:
            self._parent.on_child_completed(self)

    def fail(self, message: str, details: pb.TaskFailureDetails):
        if self._is_complete:
            raise ValueError('The task has already completed.')
        self._exception = TaskFailedError(message, details)
        self._is_complete = True
        if self._parent is not None:
            self._parent.on_child_completed(self)


class WhenAllTask(CompositeTask[List[T]]):
    """A task that completes when all of its child tasks complete."""

    def __init__(self, tasks: List[Task[T]]):
        super().__init__(tasks)
        self._completed_tasks = 0
        self._failed_tasks = 0

    @property
    def pending_tasks(self) -> int:
        """Returns the number of tasks that have not yet completed."""
        return len(self._tasks) - self._completed_tasks

    def on_child_completed(self, task: Task[T]):
        if self.is_complete:
            raise ValueError('The task has already completed.')
        self._completed_tasks += 1
        if task.is_failed and self._exception is None:
            self._exception = task.get_exception()
            self._is_complete = True
        if self._completed_tasks == len(self._tasks):
            # The order of the result MUST match the order of the tasks provided to the constructor.
            self._result = [task.get_result() for task in self._tasks]
            self._is_complete = True

    def get_completed_tasks(self) -> int:
        return self._completed_tasks


class WhenAnyTask(CompositeTask[Task]):
    """A task that completes when any of its child tasks complete."""

    def __init__(self, tasks: List[Task]):
        super().__init__(tasks)

    def on_child_completed(self, task: Task):
        # The first task to complete is the result of the WhenAnyTask.
        if not self.is_complete:
            self._is_complete = True
            self._result = task


def when_all(tasks: List[Task[T]]) -> WhenAllTask[T]:
    """Returns a task that completes when all of the provided tasks complete or when one of the tasks fail."""
    return WhenAllTask(tasks)


def when_any(tasks: List[Task]) -> WhenAnyTask:
    """Returns a task that completes when any of the provided tasks complete or fail."""
    return WhenAnyTask(tasks)


class ActivityContext:
    def __init__(self, orchestration_id: str, task_id: int):
        self._orchestration_id = orchestration_id
        self._task_id = task_id

    @property
    def orchestration_id(self) -> str:
        """Get the ID of the orchestration instance that scheduled this activity.

        Returns
        -------
        str
            The ID of the current orchestration instance.
        """
        return self._orchestration_id

    @property
    def task_id(self) -> int:
        """Get the task ID associated with this activity invocation.

        The task ID is an auto-incrementing integer that is unique within
        the scope of the orchestration instance. It can be used to distinguish
        between multiple activity invocations that are part of the same
        orchestration instance.

        Returns
        -------
        str
            The ID of the current orchestration instance.
        """
        return self._task_id


# Orchestrators are generators that yield tasks and receive/return any type
Orchestrator = Callable[[OrchestrationContext, TInput], Union[Generator[Task, Any, Any], TOutput]]

# Activities are simple functions that can be scheduled by orchestrators
Activity = Callable[[ActivityContext, TInput], TOutput]


def get_name(fn: Callable) -> str:
    """Returns the name of the provided function"""
    name = fn.__name__
    if name == '<lambda>':
        raise ValueError('Cannot infer a name from a lambda function. Please provide a name explicitly.')

    return name
