from abc import ABC, abstractmethod
from typing import Type, TypeVar, Generic

T = TypeVar('T')


class Task(ABC, Generic[T]):
    """Abstract base class for asynchronous tasks in a durable orchestration."""

    def __init__(self) -> None:
        super().__init__()
        pass

    @abstractmethod
    def is_complete(self) -> bool:
        pass

    @abstractmethod
    def get_result(self) -> T:
        pass


class CompletableTask(Task[T]):
    _result: T

    def __init__(self):
        super().__init__()
        self._is_complete = False
        pass

    def complete(self, result: T):
        self._result = result
        self._is_complete = True

    def is_complete(self) -> bool:
        return self._is_complete

    def get_result(self) -> T:
        return self._result
