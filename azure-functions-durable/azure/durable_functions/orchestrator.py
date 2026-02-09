"""Durable Orchestrator.

Responsible for orchestrating the execution of the user defined generator
function.
"""
from typing import Callable, Any, Generator

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

    def handle(self, context: OrchestrationContext) -> str:
        """Handle the orchestration of the user defined generator function.

        Parameters
        ----------
        context : DurableOrchestrationContext
            The DF orchestration context

        Returns
        -------
        str
            The JSON-formatted string representing the user's orchestration
            state after this invocation
        """
        self.durable_context = context
        return DurableFunctionsWorker()._execute_orchestrator(self.fn, context)

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

        def handle(context) -> str:
            return Orchestrator(fn).handle(context)

        handle.orchestrator_function = fn  # type: ignore

        return handle
