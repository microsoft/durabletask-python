# See https://peps.python.org/pep-0563/
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, List, TypeVar

import durabletask.protos.helpers as pbh
import durabletask.protos.orchestrator_service_pb2 as pb

T = TypeVar('T')


class TaskFailedError(Exception):
    """Exception type for all orchestration task failures."""

    def __init__(self, message: str, error_type: str, stack_trace: str | None):
        super().__init__(message)
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
    def stack_trace(self) -> str | None:
        return self._stack_trace


class Task(ABC, Generic[T]):
    """Abstract base class for asynchronous tasks in a durable orchestration."""
    _result: T
    _exception: TaskFailedError | None
    _parent: CompositeTask[T] | None

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

    def fail(self, details: pb.TaskFailureDetails):
        if self._is_complete:
            raise ValueError('The task has already completed.')
        self._exception = TaskFailedError(
            details.errorMessage,
            details.errorType,
            details.stackTrace.value if not pbh.is_empty(details.stackTrace) else None)
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
        if not self.is_complete:
            self._is_complete = True
            self._result = task


def when_all(tasks: List[Task[T]]) -> WhenAllTask[T]:
    """Returns a task that completes when all of the provided tasks complete or when one of the tasks fail."""
    return WhenAllTask(tasks)


def when_any(tasks: List[Task]) -> WhenAnyTask:
    """Returns a task that completes when any of the provided tasks complete or fail."""
    return WhenAnyTask(tasks)
