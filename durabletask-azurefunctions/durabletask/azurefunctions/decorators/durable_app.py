#  Copyright (c) Microsoft Corporation. All rights reserved.
#  Licensed under the MIT License.
import base64
from functools import wraps

from durabletask.internal.orchestrator_service_pb2 import OrchestratorRequest, OrchestratorResponse
from .metadata import OrchestrationTrigger, ActivityTrigger, EntityTrigger, \
    DurableClient
from typing import Callable, Optional
from typing import Union
from azure.functions import FunctionRegister, TriggerApi, BindingApi, AuthLevel

# TODO: Use __init__.py to optimize imports
from durabletask.azurefunctions.client import DurableFunctionsClient
from durabletask.azurefunctions.worker import DurableFunctionsWorker
from durabletask.azurefunctions.internal.azurefunctions_null_stub import AzureFunctionsNullStub


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

            # TODO: Move this logic somewhere better
            def handle(context) -> str:
                context_body = getattr(context, "body", None)
                if context_body is None:
                    context_body = context
                orchestration_context = context_body
                request = OrchestratorRequest()
                request.ParseFromString(base64.b64decode(orchestration_context))
                stub = AzureFunctionsNullStub()
                worker = DurableFunctionsWorker()
                response: Optional[OrchestratorResponse] = None

                def stub_complete(stub_response):
                    nonlocal response
                    response = stub_response
                stub.CompleteOrchestratorTask = stub_complete
                execution_started_events = [e for e in [e1 for e1 in request.newEvents] + [e2 for e2 in request.pastEvents] if e.HasField("executionStarted")]
                function_name = execution_started_events[-1].executionStarted.name
                worker.add_named_orchestrator(function_name, orchestrator_func)
                worker._execute_orchestrator(request, stub, None)

                if response is None:
                    raise Exception("Orchestrator execution did not produce a response.")
                # The Python worker returns the input as type "json", so double-encoding is necessary
                return '"' + base64.b64encode(response.SerializeToString()).decode('utf-8') + '"'

            handle.orchestrator_function = orchestrator_func

            # invoke next decorator, with the Orchestrator as input
            handle.__name__ = orchestrator_func.__name__
            return wrap(handle)

        return decorator

    def _configure_entity_callable(self, wrap) -> Callable:
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
        def decorator(entity_func):
            # TODO: Implement entity support - similar to orchestrators (?)
            raise NotImplementedError()

        return decorator

    def _add_rich_client(self, fb, parameter_name,
                         client_constructor):
        # Obtain user-code and force type annotation on the client-binding parameter to be `str`.
        # This ensures a passing type-check of that specific parameter,
        # circumventing a limitation of the worker in type-checking rich DF Client objects.
        # TODO: Once rich-binding type checking is possible, remove the annotation change.
        user_code = fb._function._func
        user_code.__annotations__[parameter_name] = str

        # `wraps` This ensures we re-export the same method-signature as the decorated method
        @wraps(user_code)
        async def df_client_middleware(*args, **kwargs):

            # Obtain JSON-string currently passed as DF Client,
            # construct rich object from it,
            # and assign parameter to that rich object
            starter = kwargs[parameter_name]
            client = client_constructor(starter)
            kwargs[parameter_name] = client

            # Invoke user code with rich DF Client binding
            return await user_code(*args, **kwargs)

        # TODO: Is there a better way to support retrieving the unwrapped user code?
        df_client_middleware.client_function = fb._function._func  # type: ignore

        user_code_with_rich_client = df_client_middleware
        fb._function._func = user_code_with_rich_client

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
        @self._configure_entity_callable
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
                self._add_rich_client(fb, client_name, DurableFunctionsClient)

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
