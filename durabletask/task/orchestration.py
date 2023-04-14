# See https://peps.python.org/pep-0563/
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Generator, TypeVar
from durabletask.task.activities import Activity

import durabletask.task.task as task

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
    def create_timer(self, fire_at: datetime) -> task.Task:
        """Create a Timer Task to fire after at the specified deadline.

        Parameters
        ----------
        fire_at: datetime.datetime
            The time for the timer to trigger

        Returns
        -------
        Task
            A Durable Timer Task that schedules the timer to wake up the orchestrator
        """
        pass

    @abstractmethod
    def call_activity(self, activity: Activity[TInput, TOutput], *,
                      input: TInput | None = None) -> task.Task[TOutput]:
        """Schedule an activity for execution.

        Parameters
        ----------
        name: Activity[TInput, TOutput]
            A reference to the activity function to call.
        input: TInput | None
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
    def call_sub_orchestrator(self,  orchestrator: Orchestrator[TInput, TOutput], *,
                              input: TInput | None = None,
                              instance_id: str | None = None) -> task.Task[TOutput]:
        """Schedule sub-orchestrator function for execution.

        Parameters
        ----------
        orchestrator: Orchestrator[TInput, TOutput]
            A reference to the orchestrator function to call.
        input: TInput | None
            The optional JSON-serializable input to pass to the orchestrator function.
        instance_id: str | None
            A unique ID to use for the sub-orchestration instance. If not specified, a
            random UUID will be used.

        Returns
        -------
        Task
            A Durable Task that completes when the called sub-orchestrator completes or fails.
        """
        pass


# Orchestrators are generators that yield tasks and receive/return any type
Orchestrator = Callable[[OrchestrationContext, TInput], Generator[task.Task, Any, Any] | TOutput]
