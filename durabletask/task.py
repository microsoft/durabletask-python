# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# See https://peps.python.org/pep-0563/
from __future__ import annotations

import logging
import math
from abc import ABC, abstractmethod
from collections.abc import Callable, Generator, Sequence
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Generic, TypeAlias, TypeVar, cast, overload

from durabletask.entities import DurableEntity, EntityInstanceId, EntityLock, EntityContext
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
    def version(self) -> str | None:
        """Get the version of the orchestration instance.

        This version is set when the orchestration is scheduled and can be used
        to determine which version of the orchestrator function is being executed.

        Returns
        -------
        str | None
            The version of the orchestration instance, or None if not set.
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
    def set_custom_status(self, custom_status: Any) -> None:
        """Set the orchestration instance's custom status.

        Parameters
        ----------
        custom_status: Any
            A JSON-serializable custom status value to set.
        """
        pass

    @abstractmethod
    def create_timer(self, fire_at: datetime | timedelta) -> TimerTask:
        """Create a Timer Task to fire after at the specified deadline.

        Parameters
        ----------
        fire_at: datetime.datetime | datetime.timedelta
            The time for the timer to trigger or a time delta from now.

        Returns
        -------
        TimerTask
            A Durable Timer Task that schedules the timer to wake up the orchestrator
        """
        pass

    @overload
    def call_activity(self, activity: Activity[TInput, TOutput] | str, *,
                      input: TInput | None = ...,
                      retry_policy: RetryPolicy | None = ...,
                      tags: dict[str, str] | None = ...,
                      return_type: type[T]) -> CompletableTask[T]:
        ...

    @overload
    def call_activity(self, activity: Activity[TInput, TOutput] | str, *,
                      input: TInput | None = ...,
                      retry_policy: RetryPolicy | None = ...,
                      tags: dict[str, str] | None = ...,
                      return_type: None = ...) -> CompletableTask[TOutput]:
        ...

    @abstractmethod
    def call_activity(self, activity: Activity[TInput, TOutput] | str, *,
                      input: TInput | None = None,
                      retry_policy: RetryPolicy | None = None,
                      tags: dict[str, str] | None = None,
                      return_type: type | None = None) -> CompletableTask[Any]:
        """Schedule an activity for execution.

        Parameters
        ----------
        activity: Activity[TInput, TOutput] | str
            A reference to the activity function to call.
        input: TInput | None
            The JSON-serializable input (or None) to pass to the activity.
        retry_policy: RetryPolicy | None
            The retry policy to use for this activity call.
        tags: dict[str, str] | None
            Optional tags to associate with the activity invocation.
        return_type: type | None
            Optional type used to deserialize the activity's result. When
            provided, the result is coerced to this type (dataclasses are
            constructed from their dict payloads, types exposing a
            ``from_json()`` classmethod are reconstructed via that hook), and
            the returned task is typed as ``CompletableTask[return_type]``.
            When omitted, the return type is discovered from the activity
            function's return annotation (if a function reference is passed and
            it is annotated with a reconstructable type); otherwise the raw
            deserialized JSON is returned.

        Returns
        -------
        Task
            A Durable Task that completes when the called activity function completes or fails.
        """
        pass

    @overload
    def call_entity(self,
                    entity: EntityInstanceId,
                    operation: str,
                    input: Any = ...,
                    *,
                    return_type: type[T]) -> CompletableTask[T]:
        ...

    @overload
    def call_entity(self,
                    entity: EntityInstanceId,
                    operation: str,
                    input: Any = ...,
                    *,
                    return_type: None = ...) -> CompletableTask[Any]:
        ...

    @abstractmethod
    def call_entity(self,
                    entity: EntityInstanceId,
                    operation: str,
                    input: Any = None,
                    *,
                    return_type: type | None = None) -> CompletableTask[Any]:
        """Schedule entity function for execution.

        Parameters
        ----------
        entity: EntityInstanceId
            The ID of the entity instance to call.
        operation: str
            The name of the operation to invoke on the entity.
        input: TInput | None
            The optional JSON-serializable input to pass to the entity function.
        return_type: type | None
            Optional type used to deserialize the operation's result. When
            provided, the result is coerced to this type and the returned task
            is typed as ``CompletableTask[return_type]``; when omitted, the raw
            deserialized JSON is returned.

        Returns
        -------
        Task
            A Durable Task that completes when the called entity function completes or fails.
        """
        pass

    @abstractmethod
    def signal_entity(
            self,
            entity_id: EntityInstanceId,
            operation_name: str,
            input: Any = None,
            signal_time: datetime | None = None
    ) -> None:
        """Signal an entity function for execution.

        Parameters
        ----------
        entity_id: EntityInstanceId
            The ID of the entity instance to signal.
        operation_name: str
            The name of the operation to invoke on the entity.
        input: TInput | None
            The optional JSON-serializable input to pass to the entity function.
        signal_time: datetime | None
            The optional time at which the signal should be delivered. If None, the
            signal is delivered as soon as possible. Use this to schedule a future
            operation on the entity.
        """
        pass

    @abstractmethod
    def lock_entities(self, entities: list[EntityInstanceId]) -> CompletableTask[EntityLock]:
        """Creates a Task object that locks the specified entity instances.

        The locks will be acquired the next time the orchestrator yields.
        Best practice is to immediately yield this Task and enter the returned EntityLock.
        The lock is released when the EntityLock is exited.

        Parameters
        ----------
        entities: list[EntityInstanceId]
            The list of entity instance IDs to lock.

        Returns
        -------
        EntityLock
            A context manager object that releases the locks when exited.
        """
        pass

    @overload
    def call_sub_orchestrator(self, orchestrator: Orchestrator[TInput, TOutput] | str, *,
                              input: TInput | None = ...,
                              instance_id: str | None = ...,
                              retry_policy: RetryPolicy | None = ...,
                              version: str | None = ...,
                              return_type: type[T]) -> CompletableTask[T]:
        ...

    @overload
    def call_sub_orchestrator(self, orchestrator: Orchestrator[TInput, TOutput] | str, *,
                              input: TInput | None = ...,
                              instance_id: str | None = ...,
                              retry_policy: RetryPolicy | None = ...,
                              version: str | None = ...,
                              return_type: None = ...) -> CompletableTask[TOutput]:
        ...

    @abstractmethod
    def call_sub_orchestrator(self, orchestrator: Orchestrator[TInput, TOutput] | str, *,
                              input: TInput | None = None,
                              instance_id: str | None = None,
                              retry_policy: RetryPolicy | None = None,
                              version: str | None = None,
                              return_type: type | None = None) -> CompletableTask[Any]:
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
        retry_policy: RetryPolicy | None
            The retry policy to use for this sub-orchestrator call.
        return_type: type | None
            Optional type used to deserialize the sub-orchestrator's result. When
            provided, the result is coerced to this type and the returned task is
            typed as ``CompletableTask[return_type]``; when omitted, the raw
            deserialized JSON is returned.

        Returns
        -------
        Task
            A Durable Task that completes when the called sub-orchestrator completes or fails.
        """
        pass

    # TOOD: Add a timeout parameter, which allows the task to be cancelled if the event is
    # not received within the specified timeout. This requires support for task cancellation.
    @overload
    def wait_for_external_event(self, name: str, *,
                                data_type: type[T]) -> CancellableTask[T]:
        ...

    @overload
    def wait_for_external_event(self, name: str, *,
                                data_type: None = ...) -> CancellableTask[Any]:
        ...

    @abstractmethod
    def wait_for_external_event(self, name: str, *,
                                data_type: type | None = None) -> CancellableTask[Any]:
        """Wait asynchronously for an event to be raised with the name `name`.

        Parameters
        ----------
        name : str
            The event name of the event that the task is waiting for.
        data_type : type | None
            Optional type used to deserialize the event payload. When provided,
            the payload is coerced to this type and the returned task is typed
            as ``CancellableTask[data_type]``; when omitted, the raw
            deserialized JSON is returned.

        Returns
        -------
        CancellableTask[Any]
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

    @abstractmethod
    def new_uuid(self) -> str:
        """Create a new UUID that is safe for replay within an orchestration or operation.

        The default implementation of this method creates a name-based UUID
        using the algorithm from RFC 4122 §4.3. The name input used to generate
        this value is a combination of the orchestration instance ID, the current UTC datetime,
        and an internally managed counter.

        Returns
        -------
        str
            New UUID that is safe for replay within an orchestration or operation.
        """
        pass

    @abstractmethod
    def _exit_critical_section(self) -> None:
        pass

    def create_replay_safe_logger(self, logger: logging.Logger) -> ReplaySafeLogger:
        """Create a replay-safe logger that suppresses log messages during orchestration replay.

        The returned logger wraps the provided logger and only emits log messages when
        the orchestrator is not replaying. This prevents duplicate log messages from
        appearing as a side effect of orchestration replay.

        Parameters
        ----------
        logger : logging.Logger
            The underlying logger to wrap.

        Returns
        -------
        ReplaySafeLogger
            A logger that only emits log messages when the orchestrator is not replaying.
        """
        return ReplaySafeLogger(logger, lambda: self.is_replaying)


if TYPE_CHECKING:
    # logging.LoggerAdapter is generic in stubs but is not subscriptable
    # at runtime before Python 3.11. Use a TYPE_CHECKING alias so the
    # base class evaluates correctly at runtime.
    _LoggerAdapterBase = logging.LoggerAdapter[logging.Logger]
else:
    _LoggerAdapterBase = logging.LoggerAdapter


class ReplaySafeLogger(_LoggerAdapterBase):
    """A logger adapter that suppresses log messages during orchestration replay.

    This class extends :class:`logging.LoggerAdapter` and only emits log
    messages when the orchestrator is *not* replaying. Use this to avoid
    duplicate log entries that would otherwise appear every time the
    orchestrator replays its history.

    Obtain an instance by calling :meth:`OrchestrationContext.create_replay_safe_logger`.
    """

    def __init__(self, logger: logging.Logger, is_replaying: Callable[[], bool]) -> None:
        super().__init__(logger, {})
        self._is_replaying = is_replaying

    def isEnabledFor(self, level: int) -> bool:
        """Return whether logging is enabled for the given level.

        Returns ``False`` while the orchestrator is replaying so that callers
        can skip expensive message formatting during replay.
        """
        if self._is_replaying():
            return False
        return self.logger.isEnabledFor(level)


class FailureDetails:
    def __init__(self, message: str, error_type: str, stack_trace: str | None):
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


class TaskFailedError(Exception):
    """Exception type for all orchestration task failures."""

    def __init__(self, message: str, details: pb.TaskFailureDetails | Exception):
        super().__init__(message)
        if isinstance(details, Exception):
            details = pbh.new_failure_details(details)
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


class TaskCancelledError(Exception):
    """Exception type for cancelled orchestration tasks."""


class Task(ABC, Generic[T]):
    """Abstract base class for asynchronous tasks in a durable orchestration."""
    _result: T
    _exception: TaskFailedError | None
    _parent: CompositeTask[Any] | None

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
    _tasks: list[Task[Any]]

    def __init__(self, tasks: list[Task[Any]]):
        super().__init__()
        self._tasks = tasks
        self._completed_tasks = 0
        self._failed_tasks = 0
        for task in tasks:
            task._parent = self
            if task.is_complete:
                self.on_child_completed(task)

    def get_tasks(self) -> list[Task[Any]]:
        return self._tasks

    @abstractmethod
    def on_child_completed(self, task: Task[Any]) -> None:
        pass


class WhenAllTask(CompositeTask[list[T]]):
    """A task that completes when all of its child tasks complete."""

    def __init__(self, tasks: list[Task[T]]):
        super().__init__(cast(list[Task[Any]], tasks))
        self._completed_tasks = 0
        self._failed_tasks = 0

    @property
    def pending_tasks(self) -> int:
        """Returns the number of tasks that have not yet completed."""
        return len(self._tasks) - self._completed_tasks

    def on_child_completed(self, task: Task[Any]) -> None:
        if self.is_complete:
            raise ValueError('The task has already completed.')
        self._completed_tasks += 1
        if task.is_failed and self._exception is None:
            self._exception = task.get_exception()
            self._is_complete = True
        if self._completed_tasks == len(self._tasks):
            # The order of the result MUST match the order of the tasks provided to the constructor.
            self._result = [child.get_result() for child in self._tasks]
            self._is_complete = True

    def get_completed_tasks(self) -> int:
        return self._completed_tasks


class CompletableTask(Task[T]):

    def __init__(self, expected_type: type | None = None) -> None:
        super().__init__()
        self._retryable_parent: RetryableTask[Any] | None = None
        self._expected_type = expected_type

    def complete(self, result: T):
        if self._is_complete:
            raise ValueError('The task has already completed.')
        self._result = result
        self._is_complete = True
        if self._parent is not None:
            self._parent.on_child_completed(self)

    def fail(self, message: str, details: Exception | pb.TaskFailureDetails):
        if self._is_complete:
            raise ValueError('The task has already completed.')
        self._exception = TaskFailedError(message, details)
        self._is_complete = True
        if self._parent is not None:
            self._parent.on_child_completed(self)


class CancellableTask(CompletableTask[T]):
    """A completable task that can be cancelled before it finishes."""

    def __init__(self, expected_type: type | None = None) -> None:
        super().__init__(expected_type)
        self._is_cancelled = False
        self._cancel_handler: Callable[[], None] | None = None

    @property
    def is_cancelled(self) -> bool:
        """Returns True if the task was cancelled, False otherwise."""
        return self._is_cancelled

    def get_result(self) -> T:
        if self._is_cancelled:
            raise TaskCancelledError('The task was cancelled.')
        return super().get_result()

    def set_cancel_handler(self, cancel_handler: Callable[[], None]) -> None:
        self._cancel_handler = cancel_handler

    def cancel(self) -> bool:
        """Attempts to cancel this task.

        Returns
        -------
        bool
            True if cancellation was applied, False if the task had already completed.
        """
        if self._is_complete:
            return False

        if self._cancel_handler is not None:
            self._cancel_handler()

        self._is_cancelled = True
        self._is_complete = True
        if self._parent is not None:
            self._parent.on_child_completed(self)
        return True


class RetryableTask(CompletableTask[T]):
    """A task that can be retried according to a retry policy."""

    def __init__(self, retry_policy: RetryPolicy, action: pb.OrchestratorAction,
                 start_time: datetime, is_sub_orch: bool,
                 expected_type: type | None = None) -> None:
        super().__init__(expected_type)
        self._action = action
        self._retry_policy = retry_policy
        self._attempt_count = 1
        self._start_time = start_time
        self._is_sub_orch = is_sub_orch

    def increment_attempt_count(self) -> None:
        self._attempt_count += 1

    def compute_next_delay(self) -> timedelta | None:
        if self._attempt_count >= self._retry_policy.max_number_of_attempts:
            return None

        retry_expiration: datetime = datetime.max
        if self._retry_policy.retry_timeout is not None and self._retry_policy.retry_timeout != datetime.max:
            retry_expiration = self._start_time + self._retry_policy.retry_timeout

        if self._retry_policy.backoff_coefficient is None:
            backoff_coefficient = 1.0
        else:
            backoff_coefficient = self._retry_policy.backoff_coefficient

        if datetime.now(tz=timezone.utc).replace(tzinfo=None) < retry_expiration:
            next_delay_f = math.pow(backoff_coefficient, self._attempt_count - 1) * self._retry_policy.first_retry_interval.total_seconds()

            if self._retry_policy.max_retry_interval is not None:
                next_delay_f = min(next_delay_f, self._retry_policy.max_retry_interval.total_seconds())

            return timedelta(seconds=next_delay_f)

        return None


class TimerTask(CancellableTask[None]):
    def __init__(self, final_fire_at: datetime | None = None,
                 maximum_timer_interval: timedelta | None = None):
        super().__init__()
        self._final_fire_at = final_fire_at
        self._maximum_timer_interval = maximum_timer_interval

    def set_retryable_parent(self, retryable_task: RetryableTask[Any]) -> None:
        self._retryable_parent = retryable_task

    def _handle_timer_fired(self, current_utc_datetime: datetime) -> datetime | None:
        if (self._final_fire_at is not None
                and self._maximum_timer_interval is not None
                and current_utc_datetime < self._final_fire_at):
            return self._get_next_fire_at(current_utc_datetime)
        super().complete(None)
        return None

    def _get_next_fire_at(self, current_utc_datetime: datetime) -> datetime:
        # _handle_timer_fired guards both attributes before calling this method.
        assert self._final_fire_at is not None
        assert self._maximum_timer_interval is not None
        if current_utc_datetime + self._maximum_timer_interval < self._final_fire_at:
            return current_utc_datetime + self._maximum_timer_interval
        return self._final_fire_at


class WhenAnyTask(CompositeTask[Task[T]], Generic[T]):
    """A task that completes when any of its child tasks complete."""

    def __init__(self, tasks: list[Task[T]]):
        super().__init__(cast(list[Task[Any]], tasks))

    def on_child_completed(self, task: Task[Any]) -> None:
        # The first task to complete is the result of the WhenAnyTask.
        if not self.is_complete:
            self._is_complete = True
            self._result = cast(Task[T], task)


def when_all(tasks: list[Task[T]]) -> WhenAllTask[T]:
    """Returns a task that completes when all of the provided tasks complete or when one of the tasks fail."""
    return WhenAllTask(tasks)


def when_any(tasks: Sequence[Task[T]]) -> WhenAnyTask[T]:
    """Returns a task that completes when any of the provided tasks complete or fail."""
    return WhenAnyTask(list(tasks))


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


# Orchestrators are generators that yield tasks, receive any type, and return TOutput
Orchestrator: TypeAlias = Callable[[OrchestrationContext, TInput], Generator[Task[Any], Any, TOutput] | TOutput]

# Activities are simple functions that can be scheduled by orchestrators
Activity: TypeAlias = Callable[[ActivityContext, TInput], TOutput]

Entity: TypeAlias = Callable[[EntityContext, TInput], TOutput] | type[DurableEntity]


class RetryPolicy:
    """Represents the retry policy for an orchestration or activity function."""

    def __init__(self, *,
                 first_retry_interval: timedelta,
                 max_number_of_attempts: int,
                 backoff_coefficient: float | None = 1.0,
                 max_retry_interval: timedelta | None = None,
                 retry_timeout: timedelta | None = None):
        """Creates a new RetryPolicy instance.

        Parameters
        ----------
        first_retry_interval : timedelta
            The retry interval to use for the first retry attempt.
        max_number_of_attempts : int
            The maximum number of retry attempts.
        backoff_coefficient : float | None
            The backoff coefficient to use for calculating the next retry interval.
        max_retry_interval : timedelta | None
            The maximum retry interval to use for any retry attempt.
        retry_timeout : timedelta | None
            The maximum amount of time to spend retrying the operation.
        """
        # validate inputs
        if first_retry_interval < timedelta(seconds=0):
            raise ValueError('first_retry_interval must be >= 0')
        if max_number_of_attempts < 1:
            raise ValueError('max_number_of_attempts must be >= 1')
        if backoff_coefficient is not None and backoff_coefficient < 1:
            raise ValueError('backoff_coefficient must be >= 1')
        if max_retry_interval is not None and max_retry_interval < timedelta(seconds=0):
            raise ValueError('max_retry_interval must be >= 0')
        if retry_timeout is not None and retry_timeout < timedelta(seconds=0):
            raise ValueError('retry_timeout must be >= 0')

        self._first_retry_interval = first_retry_interval
        self._max_number_of_attempts = max_number_of_attempts
        self._backoff_coefficient = backoff_coefficient
        self._max_retry_interval = max_retry_interval
        self._retry_timeout = retry_timeout

    @property
    def first_retry_interval(self) -> timedelta:
        """The retry interval to use for the first retry attempt."""
        return self._first_retry_interval

    @property
    def max_number_of_attempts(self) -> int:
        """The maximum number of retry attempts."""
        return self._max_number_of_attempts

    @property
    def backoff_coefficient(self) -> float | None:
        """The backoff coefficient to use for calculating the next retry interval."""
        return self._backoff_coefficient

    @property
    def max_retry_interval(self) -> timedelta | None:
        """The maximum retry interval to use for any retry attempt."""
        return self._max_retry_interval

    @property
    def retry_timeout(self) -> timedelta | None:
        """The maximum amount of time to spend retrying the operation."""
        return self._retry_timeout


def get_entity_name(fn: Entity[Any, Any]) -> str:
    if hasattr(fn, "__durable_entity_name__"):
        return getattr(fn, "__durable_entity_name__")
    if isinstance(fn, type) and issubclass(fn, DurableEntity):
        return fn.__name__
    return get_name(cast(Callable[..., Any], fn))


def get_name(fn: Callable[..., Any]) -> str:
    """Returns the name of the provided function"""
    name = fn.__name__
    if name == '<lambda>':
        raise ValueError('Cannot infer a name from a lambda function. Please provide a name explicitly.')

    return name
