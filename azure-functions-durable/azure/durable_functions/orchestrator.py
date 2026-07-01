"""Durable Orchestrator.

Responsible for orchestrating the execution of the user defined generator
function.
"""
from typing import Any, Callable, Generator

import azure.functions as func

from durabletask.task import OrchestrationContext

from .worker import DurableFunctionsWorker


class Orchestrator:
    """Durable Orchestration Class.

    Responsible for orchestrating the execution of the user defined generator
    function.
    """

    def __init__(self,
                 activity_func: Callable[[OrchestrationContext, Any], Generator[Any, Any, Any]]):
        """Create a new orchestrator for the user defined generator.

        Responsible for orchestrating the execution of the user defined
        generator function.
        :param activity_func: Generator function to orchestrate.
        """
        self.fn: Callable[[OrchestrationContext, Any], Generator[Any, Any, Any]] = activity_func

    def handle(self, context: func.OrchestrationContext) -> str:
        """Handle the orchestration of the user defined generator function.

        Parameters
        ----------
        context : azure.functions.OrchestrationContext
            The Durable Functions orchestration trigger context. This is the
            transport wrapper supplied by the host (it exposes ``.body``); the
            user's orchestrator function receives a durabletask
            ``OrchestrationContext`` during execution inside the worker.

        Returns
        -------
        str
            The JSON-formatted string representing the user's orchestration
            state after this invocation
        """
        self.durable_context = context
        return DurableFunctionsWorker().execute_orchestration_request(self.fn, context)

    @classmethod
    def create(cls, fn: Callable[[OrchestrationContext, Any], Generator[Any, Any, Any]]) \
            -> Callable[[Any], str]:
        """Create an instance of the orchestration class.

        Parameters
        ----------
        fn: Callable[[DurableOrchestrationContext], Iterator[Any]]
            Generator function that needs orchestration

        Returns
        -------
        Callable[[Any], str]
            Handle function of the newly created orchestration client
        """

        # The generated handle is the function registered with the Azure
        # Functions host. Its ``context`` parameter must be annotated with
        # ``azure.functions.OrchestrationContext`` so the host's
        # orchestrationTrigger binding converter accepts it; at runtime the
        # host passes that transport context (exposing ``.body``).
        def handle(context: func.OrchestrationContext) -> str:
            return Orchestrator(fn).handle(context)

        handle.orchestrator_function = fn  # pyright: ignore[reportFunctionMemberAccess]

        return handle
