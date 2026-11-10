# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import asyncio
import inspect
import json
from collections.abc import Awaitable, Callable, Coroutine, Generator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Optional, TypeAlias, TypeVar, cast, overload
from uuid import UUID

from durabletask import task
from durabletask.entities import EntityInstanceId
from durabletask.internal.shared import is_async_callable
from durabletask.task import OrchestrationContext, RetryPolicy, Task

from ...http.models import DurableHttpResponse
from ..serialization import DEFAULT_FUNCTIONS_DATA_CONVERTER
from .function_context import FunctionContext
from .token_source import TokenSource

T = TypeVar("T")


class DurableOrchestrationContext:
    """Azure Functions-style orchestration context (v1-compatible).

    Wraps a durabletask :class:`~durabletask.task.OrchestrationContext` (and the
    orchestration input) and exposes the v1 ``DurableOrchestrationContext`` API.
    It is delivered to one-argument orchestrator functions
    (``def orchestrator(context):``); durabletask-native two-argument
    orchestrators (``def orchestrator(ctx, input):``) receive the durabletask
    context directly instead.
    """

    def __init__(self,
                 ctx: OrchestrationContext,
                 orchestration_input: Any = None,
                 input_type: Optional[type] = None):
        self._ctx = ctx
        self._input = orchestration_input
        self._input_type = input_type
        self._custom_status: Any = None
        self._will_continue_as_new = False
        # v1 exposed a FunctionContext bag of any extra trigger-payload fields.
        # The durabletask protobuf request carries no such extras, so this is
        # an empty bag -- matching the common v1 runtime case.
        self._function_context = FunctionContext()

    # -- input ---------------------------------------------------------------
    def get_input(self, expected_type: Optional[type] = None) -> Any:
        """Get the orchestration input.

        When an ``expected_type`` (or the ``input_type`` declared on the
        ``orchestration_trigger`` decorator) is available, the already-decoded
        input is coerced to that type; otherwise the raw value is returned.
        """
        resolved_type = expected_type or self._input_type
        if resolved_type is None:
            return self._input
        return DEFAULT_FUNCTIONS_DATA_CONVERTER.coerce(self._input, resolved_type)

    # -- properties ----------------------------------------------------------
    @property
    def instance_id(self) -> str:
        """Get the ID of the current orchestration instance."""
        return self._ctx.instance_id

    @property
    def is_replaying(self) -> bool:
        """Get whether the orchestrator is currently replaying."""
        return self._ctx.is_replaying

    @property
    def current_utc_datetime(self) -> datetime:
        """Get the replay-safe current UTC date/time.

        Returned as a timezone-aware (UTC) datetime for v1 compatibility;
        durabletask exposes a naive UTC datetime.
        """
        value = self._ctx.current_utc_datetime
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value

    @property
    def custom_status(self) -> Any:
        """Get the custom status set during this execution (or ``None``)."""
        return self._custom_status

    @property
    def will_continue_as_new(self) -> bool:
        """Whether :meth:`continue_as_new` has been called in this execution."""
        return self._will_continue_as_new

    @property
    def version(self) -> Optional[str]:
        """Get the version assigned to the orchestration instance (or ``None``)."""
        return self._ctx.version

    @property
    def parent_instance_id(self) -> Optional[str]:
        """Get the ID of the parent orchestration.

        Returns ``None`` for a top-level orchestration (i.e. one that was not
        started as a sub-orchestration).
        """
        return self._ctx.parent_instance_id

    @property
    def function_context(self) -> FunctionContext:
        """Get the Azure Functions-level context (extra trigger-payload fields).

        Returned as an (empty) :class:`FunctionContext` for v1 drop-in
        compatibility. The durabletask orchestration request does not carry the
        arbitrary extra fields the v1 JSON trigger payload could, so there is
        nothing to populate -- which matches the common v1 case where the base
        payload is fully consumed and the bag is empty.
        """
        return self._function_context

    @property
    def histories(self) -> Any:
        """Get the running history of scheduled tasks.

        Not available: durabletask manages orchestration history internally and
        does not expose it on the context.
        """
        raise NotImplementedError(
            "histories is not exposed by durabletask; use the client's "
            "get_orchestration_history instead.")

    # -- activities ----------------------------------------------------------
    def call_activity(self, name: Callable[..., Any] | str, input_: Any = None) -> Task[Any]:
        """Schedule an activity function for execution."""
        return self._ctx.call_activity(name, input=input_)

    def call_activity_with_retry(self,
                                 name: Callable[..., Any] | str,
                                 retry_options: RetryPolicy,
                                 input_: Any = None) -> Task[Any]:
        """Schedule an activity function for execution, with retries."""
        return self._ctx.call_activity(name, input=input_, retry_policy=retry_options)

    # -- sub-orchestrators ---------------------------------------------------
    def call_sub_orchestrator(self,
                              name: Callable[..., Any] | str,
                              input_: Any = None,
                              instance_id: Optional[str] = None) -> Task[Any]:
        """Schedule a sub-orchestrator function for execution."""
        return self._ctx.call_sub_orchestrator(name, input=input_, instance_id=instance_id)

    def call_sub_orchestrator_with_retry(self,
                                         name: Callable[..., Any] | str,
                                         retry_options: RetryPolicy,
                                         input_: Any = None,
                                         instance_id: Optional[str] = None) -> Task[Any]:
        """Schedule a sub-orchestrator function for execution, with retries."""
        return self._ctx.call_sub_orchestrator(
            name, input=input_, instance_id=instance_id, retry_policy=retry_options)

    # -- timers and events ---------------------------------------------------
    def create_timer(self, fire_at: datetime) -> Task[Any]:
        """Create a durable timer that fires at the specified time."""
        return self._ctx.create_timer(fire_at)

    def wait_for_external_event(self,
                                name: str,
                                expected_type: Optional[type] = None) -> Task[Any]:
        """Wait for an external event with the given name."""
        return self._ctx.wait_for_external_event(name, data_type=expected_type)

    # -- control -------------------------------------------------------------
    def continue_as_new(self, input_: Any) -> None:
        """Restart the orchestration with a new input."""
        self._will_continue_as_new = True
        self._ctx.continue_as_new(input_)

    def set_custom_status(self, status: Any) -> None:
        """Set the orchestration's custom status payload."""
        self._custom_status = status
        self._ctx.set_custom_status(status)

    # -- deterministic IDs ---------------------------------------------------
    def new_uuid(self) -> str:
        """Create a new replay-safe UUID string."""
        return self._ctx.new_uuid()

    def new_guid(self) -> UUID:
        """Create a new replay-safe UUID."""
        return UUID(self._ctx.new_uuid())

    # -- fan-out / fan-in ----------------------------------------------------
    def task_all(self, tasks: list[Task[Any]]) -> Task[Any]:
        """Schedule all tasks and complete when all of them complete."""
        return task.when_all(tasks)

    def task_any(self, tasks: list[Task[Any]]) -> Task[Any]:
        """Schedule all tasks and complete when the first one completes."""
        return task.when_any(tasks)

    # -- entities ------------------------------------------------------------
    def call_entity(self,
                    entityId: EntityInstanceId,
                    operationName: str,
                    operationInput: Any = None) -> Task[Any]:
        """Call an entity operation and get its result."""
        return self._ctx.call_entity(entityId, operationName, operationInput)

    def signal_entity(self,
                      entityId: EntityInstanceId,
                      operationName: str,
                      operationInput: Any = None) -> None:
        """Signal an entity operation (fire and forget)."""
        self._ctx.signal_entity(entityId, operationName, input=operationInput)

    # -- durable HTTP --------------------------------------------------------
    def call_http(self,
                  method: str,
                  uri: str,
                  content: Any = None,
                  headers: Optional[dict[str, str]] = None,
                  token_source: Optional[TokenSource] = None,
                  is_raw_str: bool = False) -> Task[Any]:
        """Schedule a durable HTTP call to the specified endpoint.

        The request is executed by a built-in activity and, if the endpoint
        responds with ``202 Accepted`` and a ``Location`` header, is polled to
        completion via a built-in sub-orchestration (honoring ``Retry-After``).
        The task's result is a
        :class:`~azure.durable_functions.http.models.DurableHttpResponse`.

        Parameters
        ----------
        method: str
            The HTTP request method.
        uri: str
            The HTTP request uri.
        content: Any
            The HTTP request content. A ``str`` is sent as JSON unless
            ``is_raw_str`` is set; other values are serialized to JSON.
        headers: Optional[dict[str, str]]
            The HTTP request headers.
        token_source: Optional[TokenSource]
            The source of the OAuth token to add to the request.
        is_raw_str: bool
            When ``True``, send ``str`` content as-is instead of JSON-encoding
            it. Requires ``content`` to be a ``str``.
        """
        # Imported lazily to avoid a module import cycle
        # (http.models -> compat.token_source).
        from ...http.builtin import BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME
        from ...http.models import DurableHttpRequest, DurableHttpResponse

        if (not isinstance(content, str)) and is_raw_str:
            raise TypeError(
                "Invalid use of 'is_raw_str' parameter: 'is_raw_str' is set to "
                "'True' but 'content' is not an instance of type 'str'. Either "
                "set 'is_raw_str' to 'False', or ensure your 'content' is of "
                "type 'str'.")

        json_content: Optional[str] = None
        if content is not None:
            if isinstance(content, str) and is_raw_str:
                json_content = content
            else:
                json_content = json.dumps(content)

        request = DurableHttpRequest(method, uri, json_content, headers, token_source)
        return self._ctx.call_sub_orchestrator(
            BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME,
            input=request.to_json(),
            return_type=DurableHttpResponse)


def _schedule_awaitables(
    operations: Sequence[Awaitable[T]],
) -> tuple[list[asyncio.Future[T]], list[asyncio.Future[T]]]:
    """Schedule each distinct awaitable once, retaining input order."""
    scheduled: dict[int, asyncio.Future[T]] = {}
    owned: list[asyncio.Future[T]] = []
    futures: list[asyncio.Future[T]] = []
    loop = asyncio.get_running_loop()
    for operation in operations:
        if not inspect.isawaitable(operation):
            raise TypeError("Orchestration operations must be awaitable.")
        if isinstance(operation, asyncio.Future) and operation.get_loop() is not loop:
            raise ValueError("Orchestration tasks must belong to the current durable event loop.")
    for operation in operations:
        key = id(operation)
        if key not in scheduled:
            future = asyncio.ensure_future(operation)
            scheduled[key] = future
            if future is not operation:
                owned.append(future)
        futures.append(scheduled[key])
    return futures, owned


async def _cancel_owned(futures: Sequence[asyncio.Future[Any]]) -> None:
    pending = [future for future in futures if not future.done()]
    for future in pending:
        future.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


class AsyncDurableOrchestrationContext:
    """Functions v1-style context supplied automatically to async orchestrators.

    Durable operations are cold, single-use coroutines. Synchronous commands
    and metadata have the same behavior and argument names as the generator
    context. Use :meth:`as_task_context` only when eager durable task handles
    are needed for interoperability.
    """

    def __init__(
        self, ctx: task.AsyncOrchestrationContext,
        orchestration_input: Any = None, input_type: type | None = None,
    ) -> None:
        self._ctx = ctx
        self._compat = DurableOrchestrationContext(ctx.as_task_context(), orchestration_input, input_type)

    @overload
    def get_input(self, expected_type: type[T]) -> T:
        ...

    @overload
    def get_input(self, expected_type: None = None) -> Any:
        ...

    def get_input(self, expected_type: type | None = None) -> Any:
        """Get the orchestration input, optionally coerced to the expected type."""
        return self._compat.get_input(expected_type)

    @property
    def instance_id(self) -> str:
        """Get the current orchestration instance ID."""
        return self._compat.instance_id

    @property
    def parent_instance_id(self) -> str | None:
        """Get the parent orchestration instance ID, if any."""
        return self._compat.parent_instance_id

    @property
    def version(self) -> str | None:
        """Get the orchestration instance version, if any."""
        return self._compat.version

    @property
    def current_utc_datetime(self) -> datetime:
        """Get the replay-safe current UTC time as a timezone-aware datetime."""
        return self._compat.current_utc_datetime

    @property
    def is_replaying(self) -> bool:
        """Get whether the orchestration is replaying its history."""
        return self._compat.is_replaying

    @property
    def custom_status(self) -> Any:
        """Get the custom status set during this execution."""
        return self._compat.custom_status

    @property
    def will_continue_as_new(self) -> bool:
        """Get whether continue-as-new was requested during this execution."""
        return self._compat.will_continue_as_new

    @property
    def function_context(self) -> FunctionContext:
        """Get the Functions context bag."""
        return self._compat.function_context

    @property
    def histories(self) -> Any:
        """History is not exposed on orchestration contexts."""
        return self._compat.histories

    def continue_as_new(self, input_: Any) -> None:
        """Restart the orchestration with a new input."""
        self._compat.continue_as_new(input_)

    def set_custom_status(self, status: Any) -> None:
        """Set the orchestration's custom status."""
        self._compat.set_custom_status(status)

    def new_uuid(self) -> str:
        """Create a replay-safe UUID string."""
        return self._compat.new_uuid()

    def new_guid(self) -> UUID:
        """Create a replay-safe UUID."""
        return self._compat.new_guid()

    def signal_entity(
        self, entityId: EntityInstanceId, operationName: str, operationInput: Any = None,
    ) -> None:
        """Signal an entity without waiting for a result."""
        self._compat.signal_entity(entityId, operationName, operationInput)

    def as_task_context(self) -> DurableOrchestrationContext:
        """Get the v1-compatible eager task-handle context."""
        return self._compat

    async def call_activity(self, name: Callable[..., Any] | str, input_: Any = None) -> Any:
        """Start and await an activity."""
        return await self._ctx.call_activity(name, input=input_)

    async def call_activity_with_retry(
        self, name: Callable[..., Any] | str, retry_options: RetryPolicy, input_: Any = None,
    ) -> Any:
        """Start and await an activity with retries."""
        return await self._ctx.call_activity(name, input=input_, retry_policy=retry_options)

    async def call_sub_orchestrator(
        self, name: Callable[..., Any] | str, input_: Any = None, instance_id: str | None = None,
    ) -> Any:
        """Start and await a sub-orchestration."""
        return await self._ctx.call_sub_orchestrator(name, input=input_, instance_id=instance_id)

    async def call_sub_orchestrator_with_retry(
        self, name: Callable[..., Any] | str, retry_options: RetryPolicy,
        input_: Any = None, instance_id: str | None = None,
    ) -> Any:
        """Start and await a sub-orchestration with retries."""
        return await self._ctx.call_sub_orchestrator(
            name, input=input_, instance_id=instance_id, retry_policy=retry_options)

    async def create_timer(self, fire_at: datetime | timedelta) -> None:
        """Wait for a durable timer, withdrawing it locally if cancelled."""
        await self._ctx.create_timer(fire_at)

    @overload
    async def wait_for_external_event(
        self, name: str, expected_type: type[T], *, timeout: timedelta | None = None,
    ) -> T:
        ...

    @overload
    async def wait_for_external_event(
        self, name: str, expected_type: None = None, *, timeout: timedelta | None = None,
    ) -> Any:
        ...

    async def wait_for_external_event(
        self, name: str, expected_type: type[Any] | None = None, *, timeout: timedelta | None = None,
    ) -> Any:
        """Wait for an event, withdrawing the local subscription on cancellation or timeout."""
        return await self._ctx.wait_for_external_event(name, data_type=expected_type, timeout=timeout)

    async def call_entity(
        self, entityId: EntityInstanceId, operationName: str, operationInput: Any = None,
    ) -> Any:
        """Start and await an entity operation."""
        return await self._ctx.call_entity(entityId, operationName, operationInput)

    async def call_http(
        self, method: str, uri: str, content: Any = None, headers: dict[str, str] | None = None,
        token_source: TokenSource | None = None, is_raw_str: bool = False,
    ) -> DurableHttpResponse:
        """Start and await a durable HTTP request."""
        return await self._compat.call_http(method, uri, content, headers, token_source, is_raw_str)

    async def task_all(self, operations: Sequence[Awaitable[T]]) -> list[T]:
        """Wait for every operation, preserving input order and duplicate references.

        The first observed failure is raised after all operations finish.
        Existing asyncio futures are borrowed; cancelling this group leaves
        them running. Coroutines started by this group are cancelled and
        cleaned up if the group itself is cancelled.
        """
        futures, owned = _schedule_awaitables(operations)
        unique = list({id(future): future for future in futures}.values())
        failures: list[BaseException] = []

        def record_failure(future: asyncio.Future[T]) -> None:
            if not failures:
                if future.cancelled():
                    failures.append(asyncio.CancelledError())
                else:
                    error = future.exception()
                    if error is not None:
                        failures.append(error)

        for future in unique:
            future.add_done_callback(record_failure)
        try:
            results = await asyncio.gather(*(asyncio.shield(future) for future in futures),
                                           return_exceptions=True)
            if failures:
                raise failures[0]
            return cast(list[T], results)
        except asyncio.CancelledError:
            await _cancel_owned(owned)
            raise
        finally:
            for future in unique:
                future.remove_done_callback(record_failure)

    async def task_any(self, operations: Sequence[Awaitable[T]]) -> asyncio.Future[T]:
        """Return the first completed operation without cancelling the losers.

        The winner is the original asyncio future if one was supplied; a raw
        coroutine is wrapped in a new task. Use tasks when identity or later
        access to losing operations is needed. Cancelling the group cancels
        only coroutines it started, not futures supplied by its caller.
        """
        if not operations:
            raise ValueError("task_any requires at least one operation.")
        futures, owned = _schedule_awaitables(operations)
        unique = list({id(future): future for future in futures}.values())
        winner: asyncio.Future[asyncio.Future[T]] = asyncio.get_running_loop().create_future()

        def completed(future: asyncio.Future[T]) -> None:
            if not winner.done():
                winner.set_result(future)

        for future in unique:
            future.add_done_callback(completed)
        try:
            return await winner
        except asyncio.CancelledError:
            await _cancel_owned(owned)
            raise
        finally:
            for future in unique:
                future.remove_done_callback(completed)


FunctionsOrchestrator: TypeAlias = (
    task.Orchestrator[Any, Any]
    | Callable[[DurableOrchestrationContext], Any]
    | Callable[[AsyncDurableOrchestrationContext], Coroutine[Any, Any, Any]]
)


def accepts_two_positional_args(fn: Callable[..., Any]) -> bool:
    """Return True if ``fn`` can be called with two positional args ``(ctx, input)``.

    Two-argument functions are treated as durabletask-native orchestrators;
    single-argument functions are treated as Azure Functions / v1-style
    orchestrators that receive a wrapped :class:`DurableOrchestrationContext`.

    This is a signature-arity heuristic, so the two contracts are expected to be
    strict: a v1-style function takes exactly one positional parameter and a
    durabletask-native function takes exactly two. Unusual shapes can be
    misclassified -- a wrapper exposing ``*args``, a ``functools.partial`` or
    callable instance with pre-applied/bound parameters, an un-introspectable
    callable (assumed native), or a v1 function with a defaulted second
    parameter -- so orchestrators should adhere to one of the two canonical
    signatures.
    """
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        # Can't introspect -> assume durabletask-native and pass through.
        return True

    positional = 0
    for param in sig.parameters.values():
        if param.kind in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD):
            positional += 1
        elif param.kind == param.VAR_POSITIONAL:
            return True
    return positional >= 2


def wrap_orchestrator(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Adapt a v1-style one-argument orchestrator to durabletask's ``(ctx, input)``.

    Two-argument (durabletask-native) orchestrators are returned unchanged. The
    returned wrapper deliberately does not set ``__wrapped__`` so durabletask
    introspects the wrapper's own ``(context, _input)`` signature rather than the
    wrapped function's signature. When the ``orchestration_trigger`` decorator
    declared an ``input_type``, it is stamped onto the wrapper's ``_input``
    annotation so durabletask's type discovery deserializes the input to that
    type (required in strict-typing mode, where the codec reconstructs a
    custom-object payload only when given the destination type).
    """
    if accepts_two_positional_args(fn):
        return fn

    input_type = getattr(fn, "_df_input_type", None)
    name = getattr(fn, "__name__", "orchestrator")

    if is_async_callable(fn):
        async def _async_wrapper(context: task.AsyncOrchestrationContext, _input: Any = None) -> Any:
            adapter = AsyncDurableOrchestrationContext(context, _input, input_type)
            coroutine = fn(adapter)
            if not inspect.iscoroutine(coroutine):
                if inspect.isgenerator(coroutine):
                    coroutine.close()
                raise TypeError("An async orchestrator must return a coroutine.")
            return await coroutine
        _async_wrapper.__name__ = name
        if input_type is not None:
            _async_wrapper.__annotations__["_input"] = input_type
        return _async_wrapper

    if inspect.isgeneratorfunction(fn):
        def _generator_wrapper(context: OrchestrationContext, _input: Any = None) -> Any:
            adapter = DurableOrchestrationContext(context, _input, input_type)
            generator = cast("Generator[Any, Any, Any]", fn(adapter))
            result: Any = yield from generator
            return result
        _generator_wrapper.__name__ = name
        if input_type is not None:
            _generator_wrapper.__annotations__["_input"] = input_type
        return _generator_wrapper

    def _wrapper(context: OrchestrationContext, _input: Any = None) -> Any:
        adapter = DurableOrchestrationContext(context, _input, input_type)
        return fn(adapter)
    _wrapper.__name__ = name
    if input_type is not None:
        _wrapper.__annotations__["_input"] = input_type
    return _wrapper
