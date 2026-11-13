# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Durable HTTP calls from durabletask orchestration contexts."""

from __future__ import annotations

import json
from collections.abc import Coroutine
from typing import Any, overload

from durabletask import task

from ..internal.compat.token_source import TokenSource
from .builtin import BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME
from .models import DurableHttpRequest, DurableHttpResponse, HttpRetryOptions


def _build_request(
        method: str,
        uri: str,
        content: Any,
        headers: dict[str, str] | None,
        token_source: TokenSource | None,
        is_raw_str: bool,
        retry_options: HttpRetryOptions | None) -> dict[str, Any]:
    if is_raw_str and not isinstance(content, str):
        raise TypeError(
            "Invalid use of 'is_raw_str' parameter: 'is_raw_str' is set to "
            "'True' but 'content' is not an instance of type 'str'. Either "
            "set 'is_raw_str' to 'False', or ensure your 'content' is of "
            "type 'str'.")

    json_content: str | None = None
    if content is not None:
        json_content = content if is_raw_str else json.dumps(content)

    return DurableHttpRequest(
        method, uri, json_content, headers, token_source, retry_options).to_json()


def schedule_http_request(
        ctx: task.OrchestrationContext,
        method: str,
        uri: str,
        content: Any = None,
        headers: dict[str, str] | None = None,
        token_source: TokenSource | None = None,
        is_raw_str: bool = False,
        retry_options: HttpRetryOptions | None = None) -> task.Task[DurableHttpResponse]:
    """Schedule the built-in durable HTTP sub-orchestration on an eager context."""
    request = _build_request(method, uri, content, headers, token_source, is_raw_str, retry_options)
    return ctx.call_sub_orchestrator(
        BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME, input=request, return_type=DurableHttpResponse)


async def _call_http_async(
        ctx: task.AsyncOrchestrationContext,
        method: str,
        uri: str,
        content: Any,
        headers: dict[str, str] | None,
        token_source: TokenSource | None,
        is_raw_str: bool,
        retry_options: HttpRetryOptions | None) -> DurableHttpResponse:
    request = _build_request(method, uri, content, headers, token_source, is_raw_str, retry_options)
    return await ctx.call_sub_orchestrator(
        BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME, input=request, return_type=DurableHttpResponse)


@overload
def call_http(
        ctx: task.AsyncOrchestrationContext,
        method: str,
        uri: str,
        *,
        content: Any = None,
        headers: dict[str, str] | None = None,
        token_source: TokenSource | None = None,
        is_raw_str: bool = False,
        retry_options: HttpRetryOptions | None = None,
) -> Coroutine[Any, Any, DurableHttpResponse]:
    ...


@overload
def call_http(
        ctx: task.OrchestrationContext,
        method: str,
        uri: str,
        *,
        content: Any = None,
        headers: dict[str, str] | None = None,
        token_source: TokenSource | None = None,
        is_raw_str: bool = False,
        retry_options: HttpRetryOptions | None = None,
) -> task.Task[DurableHttpResponse]:
    ...


def call_http(
        ctx: task.AsyncOrchestrationContext | task.OrchestrationContext,
        method: str,
        uri: str,
        *,
        content: Any = None,
        headers: dict[str, str] | None = None,
        token_source: TokenSource | None = None,
        is_raw_str: bool = False,
        retry_options: HttpRetryOptions | None = None,
) -> Coroutine[Any, Any, DurableHttpResponse] | task.Task[DurableHttpResponse]:
    """Make a durable HTTP request from an orchestrator.

    The request is executed by a built-in activity. If the endpoint responds
    with ``202 Accepted`` and a ``Location`` header, a built-in
    sub-orchestration polls that location until the operation completes,
    honoring ``Retry-After``. The result is a :class:`DurableHttpResponse`;
    unselected non-2xx responses are returned rather than raised.

    With an :class:`~durabletask.task.AsyncOrchestrationContext`, this returns
    a single-use coroutine for ``await``. Like other async context operations,
    it is cold: validation, content serialization, and scheduling happen when
    the coroutine starts. With an
    :class:`~durabletask.task.OrchestrationContext`, the request is scheduled
    immediately and a durable task is returned for ``yield``.

    Parameters
    ----------
    ctx: AsyncOrchestrationContext | OrchestrationContext
        The orchestration context supplied to the orchestrator.
    method: str
        The HTTP request method.
    uri: str
        The HTTP request URI.
    content: Any
        The request content. A ``str`` is sent as JSON unless ``is_raw_str``
        is set; other values are serialized to JSON.
    headers: dict[str, str] | None
        The request headers.
    token_source: TokenSource | None
        The source of an OAuth token to add to the request.
    is_raw_str: bool
        When ``True``, send ``str`` content as-is instead of JSON-encoding
        it. Requires ``content`` to be a ``str``.
    retry_options: HttpRetryOptions | None
        Opt-in durable failure retries for the initial request only.
        Selected HTTP statuses and activity failures fail the call after
        exhaustion. Location polls do not inherit the policy. Only enable
        retries when repeating the request's side effects is safe.

    Raises
    ------
    TypeError
        If ``ctx`` is not a durabletask orchestration context, or if
        ``is_raw_str`` is set and ``content`` is not a ``str``.
    """
    if isinstance(ctx, task.AsyncOrchestrationContext):
        return _call_http_async(
            ctx, method, uri, content, headers, token_source, is_raw_str, retry_options)
    # Annotations are not enforced at runtime; reject other contexts clearly.
    if not isinstance(ctx, task.OrchestrationContext):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise TypeError(
            "call_http requires a durabletask AsyncOrchestrationContext or OrchestrationContext. "
            "Orchestrators that receive a DurableOrchestrationContext should use context.call_http().")
    return schedule_http_request(
        ctx, method, uri, content, headers, token_source, is_raw_str, retry_options)
