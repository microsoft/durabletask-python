# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# See https://peps.python.org/pep-0563/
from __future__ import annotations

import math
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Callable, Generator, Generic, Optional, TypeVar, Union
from dataclasses import dataclass

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
    def set_custom_status(self, custom_status: Any) -> None:
        """Set the orchestration instance's custom status.

        Parameters
        ----------
        custom_status: Any
            A JSON-serializable custom status value to set.
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
                      input: Optional[TInput] = None,
                      retry_policy: Optional[RetryPolicy] = None) -> Task[TOutput]:
        """Schedule an activity for execution.

        Parameters
        ----------
        activity: Union[Activity[TInput, TOutput], str]
            A reference to the activity function to call.
        input: Optional[TInput]
            The JSON-serializable input (or None) to pass to the activity.
        retry_policy: Optional[RetryPolicy]
            The retry policy to use for this activity call.

        Returns
        -------
        Task
            A Durable Task that completes when the called activity function completes or fails.
        """
        pass

    @abstractmethod
    def call_sub_orchestrator(self, orchestrator: Orchestrator[TInput, TOutput], *,
                              input: Optional[TInput] = None,
                              instance_id: Optional[str] = None,
                              retry_policy: Optional[RetryPolicy] = None) -> Task[TOutput]:
        """Schedule sub-orchestrator function for execution.

        Parameters
        ----------
        orchestrator: Orchestrator[TInput, TOutput]
            A reference to the orchestrator function to call.
        input: Optional[TInput]
            The optional JSON-serializable input to pass to the orchestrator function.
        instance_id: Optional[str]
            A unique ID to use for the sub-orchestration instance. If not specified, a
            random UUID will be used.
        retry_policy: Optional[RetryPolicy]
            The retry policy to use for this sub-orchestrator call.

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

    @abstractmethod
    def signal_entity(self, entity_id: Union[str, 'EntityInstanceId'], operation_name: str, *,
                      input: Optional[Any] = None) -> Task:
        """Signal an entity with an operation.

        Parameters
        ----------
        entity_id : Union[str, EntityInstanceId]
            The ID of the entity to signal.
        operation_name : str
            The name of the operation to perform.
        input : Optional[Any]
            The JSON-serializable input to pass to the entity operation.

        Returns
        -------
        Task
            A Durable Task that completes when the entity operation is scheduled.
        """
        pass

    @abstractmethod
    def call_entity(self, entity_id: Union[str, 'EntityInstanceId'], operation_name: str, *,
                    input: Optional[TInput] = None,
                    retry_policy: Optional[RetryPolicy] = None) -> Task[TOutput]:
        """Call an entity operation and wait for the result.

        Parameters
        ----------
        entity_id : Union[str, EntityInstanceId]
            The ID of the entity to call.
        operation_name : str
            The name of the operation to perform.
        input : Optional[TInput]
            The JSON-serializable input to pass to the entity operation.
        retry_policy : Optional[RetryPolicy]
            The retry policy to use for this entity call.

        Returns
        -------
        Task[TOutput]
            A Durable Task that completes when the entity operation completes or fails.
        """
        pass

    @abstractmethod
    def lock_entities(self, *entity_ids: Union[str, 'EntityInstanceId']) -> 'EntityLockContext':
        """Create a context manager for locking multiple entities.

        This allows orchestrations to lock entities before performing operations
        on them, preventing race conditions with other orchestrations.

        Parameters
        ----------
        *entity_ids : Union[str, EntityInstanceId]
            Variable number of entity IDs to lock

        Returns
        -------
        EntityLockContext
            A context manager that handles locking and unlocking
        """
        pass


class FailureDetails:
    def __init__(self, message: str, error_type: str, stack_trace: Optional[str]):
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
    def stack_trace(self) -> Optional[str]:
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


@dataclass
class EntityState:
    """Represents the state of a durable entity."""
    instance_id: str
    last_modified_time: datetime
    backlog_queue_size: int
    locked_by: Optional[str]
    serialized_state: Optional[str]

    @property
    def exists(self) -> bool:
        """Returns True if the entity exists (has been created), False otherwise."""
        return self.serialized_state is not None


@dataclass
class EntityQuery:
    """Represents a query for durable entities."""
    instance_id_starts_with: Optional[str] = None
    last_modified_from: Optional[datetime] = None
    last_modified_to: Optional[datetime] = None
    include_state: bool = False
    include_transient: bool = False
    page_size: Optional[int] = None
    continuation_token: Optional[str] = None


@dataclass
class EntityQueryResult:
    """Represents the result of an entity query."""
    entities: list[EntityState]
    continuation_token: Optional[str] = None


class Task(ABC, Generic[T]):
    """Abstract base class for asynchronous tasks in a durable orchestration."""
    _result: T
    _exception: Optional[TaskFailedError]
    _parent: Optional[CompositeTask[T]]

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
    _tasks: list[Task]

    def __init__(self, tasks: list[Task]):
        super().__init__()
        self._tasks = tasks
        self._completed_tasks = 0
        self._failed_tasks = 0
        for task in tasks:
            task._parent = self
            if task.is_complete:
                self.on_child_completed(task)

    def get_tasks(self) -> list[Task]:
        return self._tasks

    @abstractmethod
    def on_child_completed(self, task: Task[T]):
        pass


class WhenAllTask(CompositeTask[list[T]]):
    """A task that completes when all of its child tasks complete."""

    def __init__(self, tasks: list[Task[T]]):
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


class CompletableTask(Task[T]):

    def __init__(self):
        super().__init__()
        self._retryable_parent = None

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


class RetryableTask(CompletableTask[T]):
    """A task that can be retried according to a retry policy."""

    def __init__(self, retry_policy: RetryPolicy, action: pb.OrchestratorAction,
                 start_time: datetime, is_sub_orch: bool) -> None:
        super().__init__()
        self._action = action
        self._retry_policy = retry_policy
        self._attempt_count = 1
        self._start_time = start_time
        self._is_sub_orch = is_sub_orch

    def increment_attempt_count(self) -> None:
        self._attempt_count += 1

    def compute_next_delay(self) -> Optional[timedelta]:
        if self._attempt_count >= self._retry_policy.max_number_of_attempts:
            return None

        retry_expiration: datetime = datetime.max
        if self._retry_policy.retry_timeout is not None and self._retry_policy.retry_timeout != datetime.max:
            retry_expiration = self._start_time + self._retry_policy.retry_timeout

        if self._retry_policy.backoff_coefficient is None:
            backoff_coefficient = 1.0
        else:
            backoff_coefficient = self._retry_policy.backoff_coefficient

        if datetime.utcnow() < retry_expiration:
            next_delay_f = math.pow(backoff_coefficient, self._attempt_count - 1) * self._retry_policy.first_retry_interval.total_seconds()

            if self._retry_policy.max_retry_interval is not None:
                next_delay_f = min(next_delay_f, self._retry_policy.max_retry_interval.total_seconds())
                return timedelta(seconds=next_delay_f)

        return None


class TimerTask(CompletableTask[T]):

    def __init__(self) -> None:
        super().__init__()

    def set_retryable_parent(self, retryable_task: RetryableTask):
        self._retryable_parent = retryable_task


class WhenAnyTask(CompositeTask[Task]):
    """A task that completes when any of its child tasks complete."""

    def __init__(self, tasks: list[Task]):
        super().__init__(tasks)

    def on_child_completed(self, task: Task):
        # The first task to complete is the result of the WhenAnyTask.
        if not self.is_complete:
            self._is_complete = True
            self._result = task


def when_all(tasks: list[Task[T]]) -> WhenAllTask[T]:
    """Returns a task that completes when all of the provided tasks complete or when one of the tasks fail."""
    return WhenAllTask(tasks)


def when_any(tasks: list[Task]) -> WhenAnyTask:
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


@dataclass
class EntityInstanceId:
    """Represents the ID of a durable entity instance."""
    name: str
    key: str

    def __str__(self) -> str:
        """Return the string representation in the format: name@key"""
        return f"{self.name}@{self.key}"

    @classmethod
    def from_string(cls, instance_id: str) -> 'EntityInstanceId':
        """Parse an entity instance ID from string format (name@key)."""
        if '@' not in instance_id:
            raise ValueError(f"Invalid entity instance ID format: {instance_id}. Expected format: name@key")

        parts = instance_id.split('@', 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(f"Invalid entity instance ID format: {instance_id}. Expected format: name@key")

        return cls(name=parts[0], key=parts[1])


class EntityLockContext(ABC):
    """Abstract base class for entity locking context managers."""

    @abstractmethod
    def __enter__(self) -> 'EntityLockContext':
        """Enter the entity lock context."""
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the entity lock context."""
        pass


class EntityOperationFailedException(Exception):
    """Exception raised when an entity operation fails."""

    def __init__(self, entity_id: EntityInstanceId, operation_name: str, failure_details: FailureDetails):
        self.entity_id = entity_id
        self.operation_name = operation_name
        self.failure_details = failure_details
        super().__init__(f"Operation '{operation_name}' on entity '{entity_id}' failed: {failure_details.message}")


class EntityContext:
    """Context for entity operations, providing access to state and scheduling capabilities."""

    def __init__(self, instance_id: str, operation_name: str, is_new_entity: bool = False):
        self._instance_id = instance_id
        self._operation_name = operation_name
        self._is_new_entity = is_new_entity
        self._state: Optional[Any] = None
        self._entity_instance_id = EntityInstanceId.from_string(instance_id)

    @property
    def instance_id(self) -> str:
        """Get the ID of the entity instance.

        Returns
        -------
        str
            The ID of the current entity instance.
        """
        return self._instance_id

    @property
    def entity_id(self) -> EntityInstanceId:
        """Get the structured entity instance ID.

        Returns
        -------
        EntityInstanceId
            The structured entity instance ID.
        """
        return self._entity_instance_id

    @property
    def operation_name(self) -> str:
        """Get the name of the operation being performed on the entity.

        Returns
        -------
        str
            The name of the operation.
        """
        return self._operation_name

    @property
    def is_new_entity(self) -> bool:
        """Get a value indicating whether this is a newly created entity.

        Returns
        -------
        bool
            True if this is the first operation on this entity, False otherwise.
        """
        return self._is_new_entity

    def get_state(self, state_type: type[T] = None) -> Optional[T]:
        """Get the current state of the entity.

        Parameters
        ----------
        state_type : type[T], optional
            The type to deserialize the state to. If not provided, returns the raw state.

        Returns
        -------
        Optional[T]
            The current state of the entity, or None if the entity has no state.
        """
        return self._state

    def set_state(self, state: Any) -> None:
        """Set the current state of the entity.

        Parameters
        ----------
        state : Any
            The new state for the entity. Must be JSON-serializable.
        """
        self._state = state

    def signal_entity(self, entity_id: Union[str, EntityInstanceId], operation_name: str, *,
                      input: Optional[Any] = None) -> None:
        """Signal another entity with an operation (fire-and-forget).

        Parameters
        ----------
        entity_id : Union[str, EntityInstanceId]
            The ID of the entity to signal.
        operation_name : str
            The name of the operation to perform.
        input : Optional[Any]
            The JSON-serializable input to pass to the entity operation.
        """
        # Store the signal for later processing during entity execution
        if not hasattr(self, '_signals'):
            self._signals = []

        entity_id_str = str(entity_id) if isinstance(entity_id, EntityInstanceId) else entity_id
        self._signals.append({
            'entity_id': entity_id_str,
            'operation_name': operation_name,
            'input': input
        })

    def start_new_orchestration(self, orchestrator: Union[Orchestrator[TInput, TOutput], str], *,
                                input: Optional[TInput] = None,
                                instance_id: Optional[str] = None) -> str:
        """Start a new orchestration from within an entity operation.

        Parameters
        ----------
        orchestrator : Union[Orchestrator[TInput, TOutput], str]
            The orchestrator function or name to start.
        input : Optional[TInput]
            The JSON-serializable input to pass to the orchestration.
        instance_id : Optional[str]
            The instance ID for the new orchestration. If not provided, a random UUID will be used.

        Returns
        -------
        str
            The instance ID of the new orchestration.
        """
        # Store the orchestration start request for later processing
        if not hasattr(self, '_orchestrations'):
            self._orchestrations = []

        orchestrator_name = orchestrator if isinstance(orchestrator, str) else get_name(orchestrator)
        new_instance_id = instance_id or str(uuid.uuid4())

        self._orchestrations.append({
            'name': orchestrator_name,
            'input': input,
            'instance_id': new_instance_id
        })

        return new_instance_id


# Orchestrators are generators that yield tasks and receive/return any type
Orchestrator = Callable[[OrchestrationContext, TInput], Union[Generator[Task, Any, Any], TOutput]]

# Activities are simple functions that can be scheduled by orchestrators
Activity = Callable[[ActivityContext, TInput], TOutput]


class EntityBase:
    """Base class for entity implementations that provides method-based dispatch.

    This class allows entities to be implemented as classes with methods for each operation,
    similar to the .NET TaskEntity pattern. The entity context is automatically injected
    when methods are called.
    """

    def __init__(self):
        self._context: Optional[EntityContext] = None
        self._state: Optional[Any] = None

    @property
    def context(self) -> EntityContext:
        """Get the current entity context."""
        if self._context is None:
            raise RuntimeError("Entity context is not available outside of operation execution")
        return self._context

    def get_state(self, state_type: type[T] = None) -> Optional[T]:
        """Get the current state of the entity."""
        return self._state

    def set_state(self, state: Any) -> None:
        """Set the current state of the entity."""
        self._state = state

    def signal_entity(self, entity_id: Union[str, EntityInstanceId], operation_name: str, *,
                      input: Optional[Any] = None) -> None:
        """Signal another entity with an operation."""
        if self._context:
            self._context.signal_entity(entity_id, operation_name, input=input)

    def start_new_orchestration(self, orchestrator: Union[Orchestrator[TInput, TOutput], str], *,
                                input: Optional[TInput] = None,
                                instance_id: Optional[str] = None) -> str:
        """Start a new orchestration from within an entity operation."""
        if self._context:
            return self._context.start_new_orchestration(orchestrator, input=input, instance_id=instance_id)
        return ""


def dispatch_to_entity_method(entity_obj: Any, ctx: EntityContext, input: Any) -> Any:
    """
    Dispatch an entity operation to the appropriate method on an entity object.

    This function implements flexible method dispatch similar to the .NET implementation:
    1. Look for an exact method name match (case-insensitive)
    2. If the entity is an EntityBase subclass, inject context and state
    3. Handle method parameters automatically (context, input, or both)

    Parameters
    ----------
    entity_obj : Any
        The entity object to dispatch to
    ctx : EntityContext
        The entity context
    input : Any
        The operation input

    Returns
    -------
    Any
        The result of the operation
    """
    import inspect

    # Set up entity base if applicable
    if isinstance(entity_obj, EntityBase):
        entity_obj._context = ctx
        entity_obj._state = ctx.get_state()

    # Look for a method with the operation name (case-insensitive)
    operation_name = ctx.operation_name.lower()
    method = None

    for attr_name in dir(entity_obj):
        if attr_name.lower() == operation_name and callable(getattr(entity_obj, attr_name)):
            method = getattr(entity_obj, attr_name)
            break

    if method is None:
        raise NotImplementedError(f"Entity does not implement operation '{ctx.operation_name}'")

    # Inspect method signature to determine parameters
    sig = inspect.signature(method)
    args = []
    kwargs = {}

    # Skip 'self' parameter for bound methods
    parameters = list(sig.parameters.values())
    if parameters and parameters[0].name == 'self':
        parameters = parameters[1:]

    for param in parameters:
        param_type = param.annotation

        # Check for EntityContext parameter
        if param_type == EntityContext or param.name.lower() in ['context', 'ctx']:
            if param.kind == param.POSITIONAL_OR_KEYWORD:
                args.append(ctx)
            else:
                kwargs[param.name] = ctx
        # Check for input parameter
        elif param.name.lower() in ['input', 'data', 'arg', 'value']:
            if param.kind == param.POSITIONAL_OR_KEYWORD:
                args.append(input)
            else:
                kwargs[param.name] = input
        # Default positional parameter (assume it's input)
        elif param.kind == param.POSITIONAL_OR_KEYWORD and len(args) == 0:
            args.append(input)

    try:
        result = method(*args, **kwargs)

        # Update state if entity is EntityBase
        if isinstance(entity_obj, EntityBase):
            ctx.set_state(entity_obj._state)
            entity_obj._context = None  # Clear context after operation

        return result

    except Exception:
        # Clear context on error
        if isinstance(entity_obj, EntityBase):
            entity_obj._context = None
        raise


# Entities are stateful objects that can receive operations and maintain state
Entity = Callable[['EntityContext', TInput], TOutput]


class RetryPolicy:
    """Represents the retry policy for an orchestration or activity function."""

    def __init__(self, *,
                 first_retry_interval: timedelta,
                 max_number_of_attempts: int,
                 backoff_coefficient: Optional[float] = 1.0,
                 max_retry_interval: Optional[timedelta] = None,
                 retry_timeout: Optional[timedelta] = None):
        """Creates a new RetryPolicy instance.

        Parameters
        ----------
        first_retry_interval : timedelta
            The retry interval to use for the first retry attempt.
        max_number_of_attempts : int
            The maximum number of retry attempts.
        backoff_coefficient : Optional[float]
            The backoff coefficient to use for calculating the next retry interval.
        max_retry_interval : Optional[timedelta]
            The maximum retry interval to use for any retry attempt.
        retry_timeout : Optional[timedelta]
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
    def backoff_coefficient(self) -> Optional[float]:
        """The backoff coefficient to use for calculating the next retry interval."""
        return self._backoff_coefficient

    @property
    def max_retry_interval(self) -> Optional[timedelta]:
        """The maximum retry interval to use for any retry attempt."""
        return self._max_retry_interval

    @property
    def retry_timeout(self) -> Optional[timedelta]:
        """The maximum amount of time to spend retrying the operation."""
        return self._retry_timeout


def get_name(fn: Callable) -> str:
    """Returns the name of the provided function"""
    name = fn.__name__
    if name == '<lambda>':
        raise ValueError('Cannot infer a name from a lambda function. Please provide a name explicitly.')

    return name
