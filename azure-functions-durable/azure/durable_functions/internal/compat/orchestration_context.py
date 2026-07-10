# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import inspect
import json
from datetime import datetime, timezone
from typing import Any, Callable, Generator, Optional, cast
from uuid import UUID

from durabletask import task
from durabletask.entities import EntityInstanceId
from durabletask.task import OrchestrationContext, RetryPolicy, Task

from ..serialization import DEFAULT_FUNCTIONS_DATA_CONVERTER
from .function_context import FunctionContext
from .token_source import TokenSource


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
            return value.replace(tzinfo=timezone.utc)
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


def accepts_two_positional_args(fn: Callable[..., Any]) -> bool:
    """Return True if ``fn`` can be called with two positional args ``(ctx, input)``.

    Two-argument functions are treated as durabletask-native orchestrators;
    single-argument functions are treated as Azure Functions / v1-style
    orchestrators that receive a wrapped :class:`DurableOrchestrationContext`.
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
    introspects the wrapper's own ``(context, _input)`` signature (and thus
    passes the raw input) rather than the wrapped function's signature.
    """
    if accepts_two_positional_args(fn):
        return fn

    input_type = getattr(fn, "_df_input_type", None)
    name = getattr(fn, "__name__", "orchestrator")

    if inspect.isgeneratorfunction(fn):
        def _generator_wrapper(context: OrchestrationContext, _input: Any = None) -> Any:
            adapter = DurableOrchestrationContext(context, _input, input_type)
            generator = cast("Generator[Any, Any, Any]", fn(adapter))
            result: Any = yield from generator
            return result
        _generator_wrapper.__name__ = name
        return _generator_wrapper

    def _wrapper(context: OrchestrationContext, _input: Any = None) -> Any:
        adapter = DurableOrchestrationContext(context, _input, input_type)
        return fn(adapter)
    _wrapper.__name__ = name
    return _wrapper
