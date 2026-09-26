# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Async host invocation context and offloading of synchronous durable code."""

import asyncio
import inspect
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar, copy_context
from functools import lru_cache, wraps
from types import FunctionType
from typing import Any, Callable, ParamSpec, TypeVar

import azure.functions as func

_invocation_context: ContextVar[func.Context | None] = ContextVar("durable_invocation_context", default=None)
_Parameters = ParamSpec("_Parameters")
_Result = TypeVar("_Result")


def preserve_function_source(wrapper: Callable[..., Any], original: Callable[..., Any]) -> None:
    """Preserve the filename used by the Functions worker to index the app directory."""
    if isinstance(wrapper, FunctionType) and isinstance(original, FunctionType):
        wrapper.__code__ = wrapper.__code__.replace(co_filename=original.__code__.co_filename)


@lru_cache(maxsize=1)
def _fallback_executor() -> ThreadPoolExecutor:
    setting = os.environ.get("PYTHON_THREADPOOL_THREAD_COUNT")
    max_workers = None
    if setting is not None:
        try:
            max_workers = int(setting)
            if not 1 <= max_workers <= sys.maxsize:
                raise ValueError("out of range")
        except ValueError:
            logging.getLogger(__name__).warning("Invalid PYTHON_THREADPOOL_THREAD_COUNT; using the default thread count")
            max_workers = None
    return ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="durable-functions")


def _executor() -> ThreadPoolExecutor:
    runtime = sys.modules.get("azure_functions_runtime")
    get_executor = getattr(runtime, "get_threadpool_executor", None)
    if callable(get_executor):
        executor = get_executor()
        if isinstance(executor, ThreadPoolExecutor):
            return executor
    return _fallback_executor()


async def run_sync(function: Callable[_Parameters, _Result], *args: _Parameters.args,
                   **kwargs: _Parameters.kwargs) -> _Result:
    context = _invocation_context.get()
    copied_context = copy_context()

    def invoke() -> _Result:
        storage = context.thread_local_storage if context is not None else None
        previous_id = getattr(storage, "invocation_id", None)
        runtime = sys.modules.get("azure_functions_runtime")
        invocation_id: ContextVar[str | None] | None = getattr(runtime, "invocation_id_cv", None)
        invocation_token = None
        if context is not None:
            setattr(storage, "invocation_id", context.invocation_id)
            if isinstance(invocation_id, ContextVar):
                invocation_token = invocation_id.set(context.invocation_id)
        try:
            return function(*args, **kwargs)
        finally:
            if invocation_id is not None and invocation_token is not None:
                invocation_id.reset(invocation_token)
            if storage is not None:
                setattr(storage, "invocation_id", previous_id)

    return await asyncio.get_running_loop().run_in_executor(_executor(), copied_context.run, invoke)


def wrap_invocation(function: Callable[..., Any], trigger_name: str,
                    *, registered_trigger_name: str | None = None) -> Callable[..., Any]:
    signature = inspect.signature(function)
    registered_name = registered_trigger_name or trigger_name
    if registered_name == "context":
        registered_name = "_durable_input"
        while registered_name in signature.parameters:
            registered_name = "_" + registered_name
    parameters = [
        parameter.replace(name=registered_name) if parameter.name == trigger_name else parameter
        for parameter in signature.parameters.values()
    ]
    inject_context = "context" not in signature.parameters or trigger_name == "context"
    if inject_context:
        context_parameter = inspect.Parameter(
            "context", inspect.Parameter.KEYWORD_ONLY, default=None, annotation=func.Context)
        insertion = next((index for index, parameter in enumerate(parameters)
                          if parameter.kind == inspect.Parameter.VAR_KEYWORD), len(parameters))
        parameters.insert(insertion, context_parameter)
    host_signature = signature.replace(parameters=parameters)

    @wraps(function)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        bound = host_signature.bind(*args, **kwargs)
        context = bound.arguments.get("context")
        if inject_context:
            bound.arguments.pop("context", None)
        if registered_name != trigger_name:
            bound.arguments[trigger_name] = bound.arguments.pop(registered_name)
        token = _invocation_context.set(context)
        try:
            original = inspect.BoundArguments(signature, bound.arguments)
            return await function(*original.args, **original.kwargs)
        finally:
            _invocation_context.reset(token)

    annotations = dict(function.__annotations__)
    if registered_name != trigger_name and trigger_name in annotations:
        annotations[registered_name] = annotations.pop(trigger_name)
    if inject_context:
        annotations["context"] = func.Context
    wrapper.__annotations__ = annotations
    setattr(wrapper, "__signature__", host_signature)
    setattr(wrapper, "_df_trigger_name", registered_name)
    return wrapper
