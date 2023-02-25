from abc import ABC, abstractmethod, abstractproperty
import simplejson as json

import durabletask.protos.helpers as ph
import durabletask.protos.orchestrator_service_pb2 as protos
import durabletask.task.task as task

from datetime import datetime
from typing import Any, Callable, Generator, List


class OrchestrationContext(ABC):

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

    @abstractmethod
    def create_timer(self, fire_at: datetime) -> task.Task:
        """Create a Timer Task to fire after at the specified deadline.

        Parameters
        ----------
        fire_at : datetime.datetime
            The time for the timer to trigger

        Returns
        -------
        Task
            A Durable Timer Task that schedules the timer to wake up the orchestrator
        """
        pass


# Orchestrators are generators that yield tasks and receive/return any type
Orchestrator = Callable[[OrchestrationContext], Generator[task.Task, Any, Any] | Any]
