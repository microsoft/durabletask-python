# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Durable Orchestrator.

Responsible for orchestrating the execution of a user-defined orchestrator
function.
"""
from typing import Any, Callable

import azure.functions as func

from .worker import DurableFunctionsWorker


class Orchestrator:
    """Durable Orchestration Class.

    Responsible for orchestrating the execution of the user defined generator
    function.
    """

    def __init__(self, orchestrator_func: Callable[..., Any]):
        """Create a new orchestrator wrapper for a user orchestrator function.

        The wrapped function may be a durabletask-native two-argument
        orchestrator or a v1-style single-argument orchestrator, and may or may
        not be a generator; ``DurableFunctionsWorker`` adapts it as needed.

        :param orchestrator_func: The user's orchestrator function to run.
        """
        self.fn: Callable[..., Any] = orchestrator_func

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
    def create(cls, fn: Callable[..., Any]) -> Callable[[Any], str]:
        """Create the Functions host handle for a user orchestrator function.

        Parameters
        ----------
        fn: Callable[..., Any]
            The user's orchestrator function (durabletask-native two-argument
            or v1-style single-argument; generator or not).

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
