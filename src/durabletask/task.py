from abc import ABC, abstractmethod
from typing import TypeVar, Generic

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
