# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import inspect
from functools import wraps
from typing import Any, Callable, Optional, Union

import azure.functions as func
from azure.functions import FunctionRegister, TriggerApi, BindingApi, AuthLevel
from azure.functions.decorators.function_app import DecoratorApi, FunctionBuilder

from durabletask import task

from .metadata import OrchestrationTrigger, ActivityTrigger, EntityTrigger, \
    DurableClient
from ..http.builtin import (
    BUILTIN_HTTP_ACTIVITY_NAME,
    BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME,
    builtin_http_activity,
    builtin_http_poll_orchestrator,
)
from ..internal.compat.activity import wrap_activity
from ..worker import DurableFunctionsWorker
from ..orchestrator import Orchestrator


class Blueprint(TriggerApi, BindingApi):
    """Durable Functions (DF) Blueprint container.

    It allows functions to be declared via trigger and binding decorators,
    but does not automatically index/register these functions.

    To register these functions, utilize the `register_functions` method from any
    :class:`FunctionRegister` subclass, such as `DFApp`.
    """

    def __init__(self,
                 http_auth_level: Union[AuthLevel, str] = AuthLevel.FUNCTION):
        """Instantiate a Durable Functions app with which to register Functions.

        Parameters
        ----------
        http_auth_level: Union[AuthLevel, str]
            Authorization level required for Function invocation.
            Defaults to AuthLevel.Function.

        Returns
        -------
        DFApp
            New instance of a Durable Functions app
        """
        # The next-in-MRO base (``DecoratorApi.__init__``) is declared with
        # untyped ``*args``/``**kwargs``, so pyright cannot see this call's type.
        super().__init__(auth_level=http_auth_level)  # pyright: ignore[reportUnknownMemberType]
        self._register_builtin_http_functions()

    def _register_builtin_http_functions(self) -> None:
        """Register the built-in durable HTTP activity and poll orchestrator.

        These back ``DurableOrchestrationContext.call_http``. They are
        registered under reserved names on every app so existing code that
        calls ``call_http`` works without any additional setup.
        """
        self.activity_trigger(
            input_name="input",
            activity=BUILTIN_HTTP_ACTIVITY_NAME)(builtin_http_activity)
        self.orchestration_trigger(
            context_name="context",
            orchestration=BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME)(
                builtin_http_poll_orchestrator)  # pyright: ignore[reportArgumentType]

    def configure_scheduled_tasks(self) -> None:
        """Opt in to durabletask scheduled tasks by registering their built-ins.

        Unlike durable HTTP (which is always available), scheduled tasks are
        opt-in: most apps don't use them, so their schedule entity and
        operation orchestrator are only registered when this method is called.
        After calling it, manage schedules from the client with
        :class:`durabletask.scheduled.ScheduledTaskClient`.

        The schedule entity is self-driving (it re-arms itself with delayed
        self-signals), so no additional worker configuration is required in the
        host-driven Functions model.
        """
        from durabletask.scheduled.orchestrator import (
            execute_schedule_operation_orchestrator,
        )
        from durabletask.scheduled.schedule_entity import ENTITY_NAME, Schedule

        self.entity_trigger(
            context_name="context", entity_name=ENTITY_NAME)(Schedule)
        self.orchestration_trigger(
            context_name="context")(execute_schedule_operation_orchestrator)

    def configure_history_export(self, writer: Any) -> None:
        """Opt in to durabletask history export by registering its built-ins.

        Like scheduled tasks, history export is opt-in: its export-job entity,
        driving orchestrator, and two activities are only registered when this
        method is called. After calling it, drive export jobs from the client
        with :class:`durabletask.extensions.history_export.ExportHistoryClient`.

        Parameters
        ----------
        writer:
            The :class:`~durabletask.extensions.history_export.writer.HistoryWriter`
            the export activities write each instance's serialized history
            through. It is not host-injectable, so it is supplied here (at app
            startup, which runs in every worker process) and reused per
            invocation.

        The activities' other dependency -- a durabletask client -- is injected
        per invocation via a ``durable_client_input`` binding, so the host
        supplies it wherever an export activity is scheduled. This works across a
        scaled-out, multi-worker deployment, unlike a client bound once in the
        request process.

        The enumeration activity uses a Functions-specific implementation
        (:mod:`azure.durable_functions.internal.history_export_compat`) that
        queries terminal instances via ``QueryInstances`` instead of the core
        ``ListInstanceIds`` call, which the Durable Functions host extension
        does not implement.
        """
        from durabletask.extensions.history_export._constants import (
            ENTITY_NAME as EXPORT_ENTITY_NAME,
        )
        from durabletask.extensions.history_export.activities import (
            EXPORT_INSTANCE_HISTORY_ACTIVITY,
            LIST_TERMINAL_INSTANCES_ACTIVITY,
        )
        from durabletask.extensions.history_export.orchestrator import (
            export_job_orchestrator,
        )
        from ..internal.history_export_compat import (
            FunctionsExportJobEntity,
            export_instance_history_client_bound,
            list_terminal_instances_client_bound,
            set_export_writer,
        )

        set_export_writer(writer)

        # Register the Functions-specific export entity, which rejects
        # ``ExportMode.CONTINUOUS`` at ``create`` (unsupported on Functions) so
        # the mode is impossible to start regardless of which client is used.
        self.entity_trigger(
            context_name="context",
            entity_name=EXPORT_ENTITY_NAME)(FunctionsExportJobEntity)
        self.orchestration_trigger(context_name="context")(export_job_orchestrator)
        # The export activities resolve their durabletask client from a
        # per-invocation ``durable_client_input`` binding (host-supplied in
        # whatever worker runs them). ``durable_client_input`` is applied as the
        # outer decorator over ``activity_trigger`` so the built function carries
        # both bindings.
        self.durable_client_input(client_name="client", sync=True)(
            self.activity_trigger(
                input_name="input",
                activity=LIST_TERMINAL_INSTANCES_ACTIVITY)(
                    list_terminal_instances_client_bound))
        self.durable_client_input(client_name="client", sync=True)(
            self.activity_trigger(
                input_name="input",
                activity=EXPORT_INSTANCE_HISTORY_ACTIVITY)(
                    export_instance_history_client_bound))

    def _configure_orchestrator_callable(
            self,
            wrap: Callable[[Callable[..., Any]], FunctionBuilder],
            input_type: Optional[type] = None
    ) -> Callable[[task.Orchestrator[Any, Any]], FunctionBuilder]:
        """Obtain decorator to construct an Orchestrator class from a user-defined Function.

        Parameters
        ----------
        wrap: Callable
            The next decorator to be applied.
        input_type: Optional[type]
            The expected type for orchestration input, forwarded from the
            ``orchestration_trigger`` decorator so a v1-style
            ``context.get_input()`` can decode the input to that type.

        Returns
        -------
        Callable
            The function to construct an Orchestrator class from the user-defined Function,
            wrapped by the next decorator in the sequence.
        """
        def decorator(orchestrator_func: task.Orchestrator[Any, Any]) -> FunctionBuilder:
            # Construct an orchestrator based on the end-user code

            if input_type is not None:
                # Stash the decorator-declared input type so the runtime can
                # feed it to a v1-style ``context.get_input()``.
                orchestrator_func._df_input_type = input_type  # type: ignore[attr-defined]  # noqa: E501

            handle = Orchestrator.create(orchestrator_func)

            # invoke next decorator, with the Orchestrator as input
            handle.__name__ = orchestrator_func.__name__
            return wrap(handle)

        return decorator

    def _configure_entity_callable(
            self,
            wrap: Callable[[Callable[..., Any]], FunctionBuilder],
            entity_name: Optional[str] = None
    ) -> Callable[[task.Entity[Any, Any]], FunctionBuilder]:
        """Obtain decorator to construct an Entity class from a user-defined Function.

        Parameters
        ----------
        wrap: Callable
            The next decorator to be applied.
        entity_name: Optional[str]
            The configured entity name from the ``entity_trigger`` binding. When
            provided it is stamped onto the user function via
            ``__durable_entity_name__`` so the worker registers the entity under
            this name (matching the ``@Name@key`` work items the host dispatches)
            rather than under the Python function's ``__name__``.

        Returns
        -------
        Callable
            The function to construct an Entity class from the user-defined Function,
            wrapped by the next decorator in the sequence.
        """
        def decorator(entity_func: task.Entity[Any, Any]) -> FunctionBuilder:
            # Preserve the configured entity name so the worker registers the
            # entity under it. Without this, ``add_entity`` falls back to the
            # Python function name and a work item for ``@<entity_name>@<key>``
            # fails to resolve the implementation.
            if entity_name is not None:
                entity_func.__durable_entity_name__ = entity_name  # type: ignore[union-attr]
            # Construct an orchestrator based on the end-user code

            # TODO: Because this handle method is the one actually exposed to the Functions SDK decorator,
            #       the parameter name will always be "context" here, even if the user specified a different name.
            #       We need to find a way to allow custom context names (like "ctx").
            # The generated handle is what the Azure Functions host registers,
            # so its ``context`` parameter must be annotated with
            # ``azure.functions.EntityContext`` for the host's entityTrigger
            # binding converter to accept it; at runtime the host passes that
            # transport context (exposing ``.body``).
            def handle(context: func.EntityContext) -> str:
                return DurableFunctionsWorker().execute_entity_batch_request(entity_func, context)

            handle.entity_function = entity_func  # pyright: ignore[reportFunctionMemberAccess]

            # invoke next decorator, with the Entity as input
            handle.__name__ = entity_func.__name__
            return wrap(handle)

        return decorator

    def _build_function(
            self,
            wrap: Callable[[FunctionBuilder], FunctionBuilder]
    ) -> Callable[[Callable[..., Any]], FunctionBuilder]:
        """Typed equivalent of the base ``_configure_function_builder``.

        The inherited method is untyped, which would otherwise propagate
        ``Unknown`` types through every decorator below. This mirrors its
        behaviour exactly using the typed protected members it relies on.
        """
        def decorator(func: Callable[..., Any]) -> FunctionBuilder:
            fb = self._validate_type(func)
            self._function_builders.append(fb)
            return wrap(fb)

        return decorator

    def orchestration_trigger(self, context_name: str,
                              orchestration: Optional[str] = None,
                              input_type: Optional[type] = None
                              ) -> Callable[[task.Orchestrator[Any, Any]], FunctionBuilder]:
        """Register an Orchestrator Function.

        Parameters
        ----------
        context_name: str
            Parameter name of the DurableOrchestrationContext object.
        orchestration: Optional[str]
            Name of Orchestrator Function.
            The value is None by default, in which case the name of the method is used.
        input_type: Optional[type]
            The expected type for the orchestration input. When set, a v1-style
            ``context.get_input()`` decodes the input payload to this type. A
            call-site ``expected_type`` argument on ``get_input`` takes
            precedence over this value.
        """
        @self._build_function
        def wrap(fb: FunctionBuilder) -> FunctionBuilder:

            def decorator() -> FunctionBuilder:
                fb.add_trigger(
                    trigger=OrchestrationTrigger(name=context_name,
                                                 orchestration=orchestration))
                return fb

            return decorator()

        return self._configure_orchestrator_callable(wrap, input_type=input_type)

    def activity_trigger(self, input_name: str,
                         activity: Optional[str] = None
                         ) -> Callable[[Callable[..., Any]], FunctionBuilder]:
        """Register an Activity Function.

        Parameters
        ----------
        input_name: str
            Parameter name of the Activity input.
        activity: Optional[str]
            Name of Activity Function.
            The value is None by default, in which case the name of the method is used.
        """
        @self._build_function
        def wrap(fb: FunctionBuilder) -> FunctionBuilder:
            fb.add_trigger(
                trigger=ActivityTrigger(name=input_name, activity=activity))
            return fb

        def decorator(user_fn: Callable[..., Any]) -> FunctionBuilder:
            # Adapt a durabletask-native two-argument activity ((ctx, input))
            # to the host's single-input convention; one-argument activities
            # pass through unchanged.
            return wrap(wrap_activity(user_fn, input_name))

        return decorator

    def entity_trigger(self,
                       context_name: str,
                       entity_name: Optional[str] = None
                       ) -> Callable[[task.Entity[Any, Any]], FunctionBuilder]:
        """Register an Entity Function.

        Parameters
        ----------
        context_name: str
            Parameter name of the Entity input.
        entity_name: Optional[str]
            Name of Entity Function.
            The value is None by default, in which case the name of the method is used.
        """
        @self._build_function
        def wrap(fb: FunctionBuilder) -> FunctionBuilder:
            def decorator() -> FunctionBuilder:
                fb.add_trigger(
                    trigger=EntityTrigger(name=context_name,
                                          entity_name=entity_name))
                return fb

            return decorator()

        return self._configure_entity_callable(wrap, entity_name)

    def durable_client_input(self,
                             client_name: str,
                             task_hub: Optional[str] = None,
                             connection_name: Optional[str] = None,
                             *,
                             sync: bool = False,
                             ) -> Callable[[Callable[..., Any]], FunctionBuilder]:
        """Register a Durable-client Function.

        Parameters
        ----------
        client_name: str
            Parameter name of durable client.
        task_hub: Optional[str]
            Used in scenarios where multiple function apps share the same storage account
            but need to be isolated from each other. If not specified, the default value
            from host.json is used.
            This value must match the value used by the target orchestrator functions.
        connection_name: Optional[str]
            The name of an app setting that contains a storage account connection string.
            The storage account represented by this connection string must be the same one
            used by the target orchestrator functions. If not specified, the default storage
            account connection string for the function app is used.
        sync: bool
            When ``True``, inject a :class:`SyncDurableFunctionsClient`. The
            default injects the asynchronous :class:`DurableFunctionsClient`.
        """

        @self._build_function
        def wrap(fb: FunctionBuilder) -> FunctionBuilder:
            def decorator() -> FunctionBuilder:
                # The converter returns the host configuration string. The
                # function wrapper below constructs and closes the requested
                # synchronous or asynchronous rich client per invocation.
                fb.add_binding(
                    binding=DurableClient(name=client_name,
                                          task_hub=task_hub,
                                          connection_name=connection_name))
                return fb

            return decorator()

        def attach_client_function(user_fn: Callable[..., Any]) -> FunctionBuilder:
            # Expose the original client function for unit testing, mirroring
            # ``.orchestrator_function`` and ``.entity_function``. The registered
            # wrapper resolves the client from the binding configuration and
            # closes it after the invocation.
            from ..client import DurableFunctionsClient, SyncDurableFunctionsClient

            function = (user_fn._function._func  # pyright: ignore[reportPrivateUsage]
                        if isinstance(user_fn, FunctionBuilder) else user_fn)
            signature = inspect.signature(function)

            def bind_client(
                    args: tuple[Any, ...],
                    kwargs: dict[str, Any],
            ) -> tuple[inspect.BoundArguments, DurableFunctionsClient | SyncDurableFunctionsClient]:
                bound = signature.bind(*args, **kwargs)
                if client_name not in bound.arguments:
                    raise TypeError(
                        f"durable client binding parameter '{client_name}' is not "
                        f"declared by function '{function.__name__}'")
                raw_client = bound.arguments[client_name]
                if not isinstance(raw_client, str):
                    raise TypeError(
                        f"durable client binding '{client_name}' did not provide its configuration")
                client = (SyncDurableFunctionsClient.get_cached(raw_client) if sync
                          else DurableFunctionsClient(raw_client))
                bound.arguments[client_name] = client
                return bound, client

            def set_client_metadata(client_bound: Callable[..., Any]) -> None:
                annotations = dict(function.__annotations__)
                supported_client_types = (
                    str,
                    bytes,
                    DurableFunctionsClient,
                    SyncDurableFunctionsClient,
                )
                if annotations.get(client_name) not in supported_client_types:
                    annotations[client_name] = str
                client_bound.__annotations__ = annotations
                setattr(client_bound, "client_function", function)

            if inspect.iscoroutinefunction(function):
                @wraps(function)
                async def async_client_bound(*args: Any, **kwargs: Any) -> Any:
                    bound, client = bind_client(args, kwargs)
                    try:
                        result = function(*bound.args, **bound.kwargs)
                        return await result
                    finally:
                        if isinstance(client, DurableFunctionsClient):
                            client.schedule_close()
                client_bound = async_client_bound
            else:
                @wraps(function)
                def sync_client_bound(*args: Any, **kwargs: Any) -> Any:
                    bound, client = bind_client(args, kwargs)
                    try:
                        return function(*bound.args, **bound.kwargs)
                    finally:
                        if isinstance(client, DurableFunctionsClient):
                            client.schedule_close()
                client_bound = sync_client_bound

            set_client_metadata(client_bound)
            if isinstance(user_fn, FunctionBuilder):
                user_fn._function._func = client_bound  # pyright: ignore[reportPrivateUsage]
                return wrap(user_fn)
            return wrap(client_bound)

        return attach_client_function

    def durable_client_input_sync(
            self,
            client_name: str,
            task_hub: Optional[str] = None,
            connection_name: Optional[str] = None,
    ) -> Callable[[Callable[..., Any]], FunctionBuilder]:
        """Register a durable-client binding that injects a synchronous client."""
        return self.durable_client_input(
            client_name, task_hub, connection_name, sync=True)


class DFApp(Blueprint, FunctionRegister):
    """Durable Functions (DF) app.

    Exports the decorators required to declare and index DF Function-types.
    """

    def register_functions(self, function_container: DecoratorApi) -> None:
        """Register the functions of a blueprint into this app.

        Every :class:`Blueprint` (and the :class:`DFApp` itself) auto-registers
        the reserved built-in durable-HTTP functions so ``call_http`` works out
        of the box. Merging a blueprint that carries its own copies would
        otherwise raise a duplicate-function-name error, breaking the standard
        Functions blueprint pattern. This app already provides the built-ins, so
        the blueprint's copies are dropped during registration. The incoming
        container is left unmodified, so the same blueprint can be registered
        into multiple apps.
        """
        reserved = {BUILTIN_HTTP_ACTIVITY_NAME, BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME}
        original = function_container._function_builders
        filtered = [fb for fb in original if _builder_function_name(fb) not in reserved]
        if len(filtered) == len(original):
            super().register_functions(function_container)
            return
        function_container._function_builders = filtered
        try:
            super().register_functions(function_container)
        finally:
            function_container._function_builders = original

    # ``register_blueprint`` is an alias of ``register_functions`` in the base
    # ``FunctionRegister`` (the same function object), so it must be re-aliased
    # here for blueprint registration to get the same built-in de-duplication.
    register_blueprint = register_functions


def _builder_function_name(function_builder: FunctionBuilder) -> Optional[str]:
    """Return a function builder's registered name, or ``None`` if unavailable."""
    try:
        return function_builder._function.get_function_name()  # pyright: ignore[reportPrivateUsage]
    except Exception:  # pragma: no cover - defensive; name is always present in practice
        return None
