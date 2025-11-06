#  Copyright (c) Microsoft Corporation. All rights reserved.
#  Licensed under the MIT License.
from .metadata import OrchestrationTrigger, ActivityTrigger, EntityTrigger, \
    DurableClient
from typing import Callable, Optional
from typing import Union
from azure.functions import FunctionRegister, TriggerApi, BindingApi, AuthLevel, OrchestrationContext


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
        super().__init__(auth_level=http_auth_level)

    def _configure_orchestrator_callable(self, wrap) -> Callable:
        """Obtain decorator to construct an Orchestrator class from a user-defined Function.

        In the old programming model, this decorator's logic was unavoidable boilerplate
        in user-code. Now, this is handled internally by the framework.

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
        def decorator(orchestrator_func):
            # Construct an orchestrator based on the end-user code

            # TODO: Extract this logic (?)
            def handle(context: OrchestrationContext) -> str:
                context_body = getattr(context, "body", None)
                if context_body is None:
                    context_body = context
                orchestration_context = context_body
                # TODO: Run the orchestration using the context
                return ""

            handle.orchestrator_function = orchestrator_func

            # invoke next decorator, with the Orchestrator as input
            handle.__name__ = orchestrator_func.__name__
            return wrap(handle)

        return decorator

    def orchestration_trigger(self, context_name: str,
                              orchestration: Optional[str] = None):
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
        @self._configure_function_builder
        def wrap(fb):

            def decorator():
                fb.add_trigger(
                    trigger=OrchestrationTrigger(name=context_name,
                                                 orchestration=orchestration))
                return fb

            return decorator()

        return wrap

    def activity_trigger(self, input_name: str,
                         activity: Optional[str] = None):
        """Register an Activity Function.

        Parameters
        ----------
        input_name: str
            Parameter name of the Activity input.
        activity: Optional[str]
            Name of Activity Function.
            The value is None by default, in which case the name of the method is used.
        """
        @self._configure_function_builder
        def wrap(fb):
            def decorator():
                fb.add_trigger(
                    trigger=ActivityTrigger(name=input_name,
                                            activity=activity))
                return fb

            return decorator()

        return wrap

    def entity_trigger(self, context_name: str,
                       entity_name: Optional[str] = None):
        """Register an Entity Function.

        Parameters
        ----------
        context_name: str
            Parameter name of the Entity input.
        entity_name: Optional[str]
            Name of Entity Function.
            The value is None by default, in which case the name of the method is used.
        """
        @self._configure_function_builder
        def wrap(fb):
            def decorator():
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
                             ):
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

        @self._configure_function_builder
        def wrap(fb):
            def decorator():
                # self._add_rich_client(fb, client_name, DurableOrchestrationClient)

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
