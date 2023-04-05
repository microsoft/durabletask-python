from abc import ABC, abstractmethod
from typing import Type, TypeVar, Generic

import durabletask.protos.orchestrator_service_pb2 as pb
import durabletask.protos.helpers as pbh

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

    def __init__(self) -> None:
        super().__init__()
        pass

    @abstractmethod
    def is_complete(self) -> bool:
        pass

    @abstractmethod
    def is_failed(self) -> bool:
        pass

    @abstractmethod
    def get_result(self) -> T:
        pass

    @abstractmethod
    def get_exception(self) -> TaskFailedError:
        pass


class CompletableTask(Task[T]):
    _result: T | None
    _exception: TaskFailedError | None

    def __init__(self):
        super().__init__()
        self._is_complete = False
        self._result = None
        self._exception = None

    def complete(self, result: T):
        self._result = result
        self._is_complete = True

    def fail(self, details: pb.TaskFailureDetails):
        self._exception = TaskFailedError(
            details.errorMessage,
            details.errorType,
            details.stackTrace.value if not pbh.is_empty(details.stackTrace) else None)
        self._is_complete = True

    def is_complete(self) -> bool:
        return self._is_complete

    def is_failed(self) -> bool:
        return self._exception is not None

    def get_result(self) -> T | None:
        return self._result

    def get_exception(self) -> TaskFailedError | None:
        return self._exception
