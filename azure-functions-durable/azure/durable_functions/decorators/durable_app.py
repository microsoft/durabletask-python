#  Copyright (c) Microsoft Corporation. All rights reserved.
#  Licensed under the MIT License.

from functools import wraps
from typing import Any, Callable, Optional, Union

from azure.functions import FunctionRegister, TriggerApi, BindingApi, AuthLevel
from azure.functions.decorators.function_app import FunctionBuilder

from durabletask import task

from .metadata import OrchestrationTrigger, ActivityTrigger, EntityTrigger, \
    DurableClient
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

    def _configure_orchestrator_callable(
            self,
            wrap: Callable[[Callable[..., Any]], FunctionBuilder]
    ) -> Callable[[task.Orchestrator[Any, Any]], FunctionBuilder]:
        """Obtain decorator to construct an Orchestrator class from a user-defined Function.

        Parameters
        ----------
        wrap: Callable
            The next decorator to be applied.

        Returns
        -------
        Callable
            The function to construct an Orchestrator class from the user-defined Function,
            wrapped by the next decorator in the sequence.
        """
        def decorator(orchestrator_func: task.Orchestrator[Any, Any]) -> FunctionBuilder:
            # Construct an orchestrator based on the end-user code

            handle = Orchestrator.create(orchestrator_func)

            # invoke next decorator, with the Orchestrator as input
            handle.__name__ = orchestrator_func.__name__
            return wrap(handle)

        return decorator

    def _configure_entity_callable(
            self,
            wrap: Callable[[Callable[..., Any]], FunctionBuilder]
    ) -> Callable[[task.Entity[Any, Any]], FunctionBuilder]:
        """Obtain decorator to construct an Entity class from a user-defined Function.

        Parameters
        ----------
        wrap: Callable
            The next decorator to be applied.

        Returns
        -------
        Callable
            The function to construct an Entity class from the user-defined Function,
            wrapped by the next decorator in the sequence.
        """
        def decorator(entity_func: task.Entity[Any, Any]) -> FunctionBuilder:
            # Construct an orchestrator based on the end-user code

            # TODO: Because this handle method is the one actually exposed to the Functions SDK decorator,
            #       the parameter name will always be "context" here, even if the user specified a different name.
            #       We need to find a way to allow custom context names (like "ctx").
            def handle(context: Any) -> str:
                return DurableFunctionsWorker().execute_entity_batch_request(entity_func, context)

            handle.entity_function = entity_func  # pyright: ignore[reportFunctionMemberAccess]

            # invoke next decorator, with the Entity as input
            handle.__name__ = entity_func.__name__
            return wrap(handle)

        return decorator

    def _add_rich_client(
            self,
            fb: FunctionBuilder,
            parameter_name: str,
            client_constructor: Callable[[Any], Any]
    ) -> None:
        # Obtain user-code and force type annotation on the client-binding parameter to be `str`.
        # This ensures a passing type-check of that specific parameter,
        # circumventing a limitation of the worker in type-checking rich DF Client objects.
        # TODO: Once rich-binding type checking is possible, remove the annotation change.
        # ``FunctionBuilder._function`` and ``Function._func`` are private to
        # azure-functions with no public accessor for mutating the wrapped
        # user function. Holding it as ``Any`` keeps the single private-access
        # waiver here rather than spreading it across each ``._func`` use.
        function_obj: Any = fb._function  # pyright: ignore[reportPrivateUsage]
        user_code = function_obj._func
        user_code.__annotations__[parameter_name] = str

        # `wraps` This ensures we re-export the same method-signature as the decorated method
        @wraps(user_code)
        async def df_client_middleware(*args: Any, **kwargs: Any) -> Any:

            # Obtain JSON-string currently passed as DF Client,
            # construct rich object from it,
            # and assign parameter to that rich object
            starter = kwargs[parameter_name]
            client = client_constructor(starter)
            kwargs[parameter_name] = client

            # Invoke user code with rich DF Client binding
            return await user_code(*args, **kwargs)

        # TODO: Is there a better way to support retrieving the unwrapped user code?
        df_client_middleware.client_function = function_obj._func  # pyright: ignore[reportAttributeAccessIssue]

        function_obj._func = df_client_middleware

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
                              orchestration: Optional[str] = None
                              ) -> Callable[[task.Orchestrator[Any, Any]], FunctionBuilder]:
        """Register an Orchestrator Function.

        Parameters
        ----------
        context_name: str
            Parameter name of the DurableOrchestrationContext object.
        orchestration: Optional[str]
            Name of Orchestrator Function.
            The value is None by default, in which case the name of the method is used.
        """
        @self._configure_orchestrator_callable
        @self._build_function
        def wrap(fb: FunctionBuilder) -> FunctionBuilder:

            def decorator() -> FunctionBuilder:
                fb.add_trigger(
                    trigger=OrchestrationTrigger(name=context_name,
                                                 orchestration=orchestration))
                return fb

            return decorator()

        return wrap

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
            def decorator() -> FunctionBuilder:
                fb.add_trigger(
                    trigger=ActivityTrigger(name=input_name,
                                            activity=activity))
                return fb

            return decorator()

        return wrap

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
        @self._configure_entity_callable
        @self._build_function
        def wrap(fb: FunctionBuilder) -> FunctionBuilder:
            def decorator() -> FunctionBuilder:
                fb.add_trigger(
                    trigger=EntityTrigger(name=context_name,
                                          entity_name=entity_name))
                return fb

            return decorator()

        return wrap

    def durable_client_input(self,
                             client_name: str,
                             task_hub: Optional[str] = None,
                             connection_name: Optional[str] = None
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
        """

        @self._build_function
        def wrap(fb: FunctionBuilder) -> FunctionBuilder:
            def decorator() -> FunctionBuilder:
                fb.add_binding(
                    binding=DurableClient(name=client_name,
                                          task_hub=task_hub,
                                          connection_name=connection_name))
                return fb

            return decorator()

        return wrap


class DFApp(Blueprint, FunctionRegister):
    """Durable Functions (DF) app.

    Exports the decorators required to declare and index DF Function-types.
    """

    pass
