# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Built-in durable HTTP support for the Azure Functions compatibility layer.

The v1 ``context.call_http`` API relied on the Durable Functions host extension
to execute the HTTP request (including automatic ``202 Accepted`` polling and
Managed Identity token acquisition). The durabletask protocol this SDK is built
on has no native HTTP action, so the feature is reconstructed here from core
primitives:

* a built-in **activity** (:func:`builtin_http_activity`) performs a single HTTP
  request -- acquiring a bearer token via ``azure-identity`` when a token source
  is supplied -- and returns the response, and
* a built-in **poll orchestrator** (:func:`builtin_http_poll_orchestrator`)
  issues the request and, while the endpoint returns ``202`` with a ``Location``
  header, waits on a durable timer (honoring ``Retry-After``) and re-polls until
  the operation completes.

``DurableOrchestrationContext.call_http`` schedules the poll orchestrator as a
sub-orchestration, preserving the single-``yield`` v1 ergonomics while keeping
the 202 polling loop durable (checkpointed across restarts).

Both functions are auto-registered on every ``Blueprint``/``DFApp`` under
reserved names so existing apps that call ``call_http`` work with no changes.
"""

from __future__ import annotations

import urllib.error
import urllib.request
from datetime import timedelta
from email.utils import parsedate_to_datetime
from typing import Any, Generator, Optional

from .models import DurableHttpResponse

# Reserved built-in function names. The v1 host used ``BuiltIn::HttpActivity``;
# ``::`` is not a valid Azure Functions function name, so ``__`` is used here.
# The reserved names are unlikely to collide with user-defined functions.
BUILTIN_HTTP_ACTIVITY_NAME = "BuiltIn__HttpActivity"
BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME = "BuiltIn__HttpPollOrchestrator"

# Fallback interval (seconds) between polls when the 202 response carries no
# usable ``Retry-After`` header.
_DEFAULT_POLL_INTERVAL_SECONDS = 1


def _acquire_bearer_token(resource: str) -> str:
    """Acquire an AAD bearer token for ``resource`` via ``azure-identity``.

    Imported lazily so the dependency is only touched when a token source is
    actually used.
    """
    from azure.identity import DefaultAzureCredential

    credential = DefaultAzureCredential()
    scope = resource.rstrip("/") + "/.default"
    return credential.get_token(scope).token


def builtin_http_activity(input: dict[str, Any]) -> dict[str, Any]:
    """Execute a single HTTP request and return the response payload.

    ``input`` is the JSON form of a
    :class:`~azure.durable_functions.http.models.DurableHttpRequest`
    (``method``, ``uri``, ``content``, ``headers``, ``tokenSource``). The
    return value is the JSON form of a
    :class:`~azure.durable_functions.http.models.DurableHttpResponse`.

    The parameter and return are declared as ``dict[str, Any]`` for static type
    checking, but the *runtime* annotations are reset to the bare ``dict`` type
    just below the function. The Azure Functions Python worker inspects trigger
    annotations during indexing and requires a real ``type`` -- it rejects
    parameterized generics such as ``dict[str, Any]`` with
    ``FunctionLoadError: ... invalid non-type annotation`` and, for a
    ``typing.Union`` origin like ``Optional[...]``, raises
    ``TypeError: issubclass() arg 1 must be a class``.
    """
    request = input or {}
    method = str(request.get("method", "GET")).upper()
    uri = request.get("uri")
    if not uri:
        raise ValueError("A non-empty 'uri' is required for a durable HTTP call.")
    content = request.get("content")
    headers: dict[str, str] = dict(request.get("headers") or {})

    token_source = request.get("tokenSource")
    if token_source:
        resource = token_source.get("resource")
        if resource:
            token = _acquire_bearer_token(resource)
            headers.setdefault("Authorization", f"Bearer {token}")

    # ``content`` has already been serialized to a string by ``call_http`` (JSON
    # unless ``is_raw_str`` was set), so it is sent as-is.
    data = content.encode("utf-8") if isinstance(content, str) else None
    req = urllib.request.Request(url=uri, data=data, method=method, headers=headers)

    try:
        with urllib.request.urlopen(req) as resp:  # noqa: S310 - user-supplied URL is the feature
            status = int(resp.status)
            resp_headers = {k: v for k, v in resp.headers.items()}
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        # Non-2xx responses (including 202) surface here; capture rather than raise
        # so the orchestrator can inspect the status code and headers.
        status = int(e.code)
        resp_headers = {k: v for k, v in (e.headers or {}).items()}
        body = e.read().decode("utf-8", errors="replace")

    return DurableHttpResponse(
        status_code=status, headers=resp_headers, content=body).to_json()


# The Azure Functions worker reads a trigger function's runtime ``__annotations__``
# during indexing and rejects parameterized generics (it requires a bare
# ``dict``). The signature above is typed ``dict[str, Any]`` for static analysis;
# reset the runtime annotations to the bare ``dict`` the worker accepts.
builtin_http_activity.__annotations__ = {"input": dict, "return": dict}


def _get_header(headers: dict[str, str], name: str) -> Optional[str]:
    """Case-insensitively look up ``name`` in ``headers``."""
    lowered = name.lower()
    for key, value in headers.items():
        if key.lower() == lowered:
            return value
    return None


def _retry_after_seconds(headers: dict[str, str]) -> int:
    """Parse the ``Retry-After`` header into a delay in seconds.

    Supports both the delta-seconds and HTTP-date forms; falls back to
    :data:`_DEFAULT_POLL_INTERVAL_SECONDS` when absent or unparseable.
    """
    raw = _get_header(headers, "Retry-After")
    if raw is None:
        return _DEFAULT_POLL_INTERVAL_SECONDS
    raw = raw.strip()
    if raw.isdigit():
        return max(int(raw), 0)
    try:
        parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return _DEFAULT_POLL_INTERVAL_SECONDS
    return _DEFAULT_POLL_INTERVAL_SECONDS


def builtin_http_poll_orchestrator(context: Any) -> Generator[Any, Any, dict[str, Any]]:
    """Issue a durable HTTP request and poll while it returns ``202``.

    Receives the request payload as its input, calls the built-in HTTP activity,
    and while the response is ``202 Accepted`` with a ``Location`` header waits
    on a durable timer (honoring ``Retry-After``) before re-polling the
    ``Location`` URL. Returns the final response payload.
    """
    request: dict[str, Any] = context.get_input() or {}
    response: dict[str, Any] = yield context.call_activity(
        BUILTIN_HTTP_ACTIVITY_NAME, request)

    while response.get("status_code") == 202:
        headers: dict[str, str] = response.get("headers") or {}
        location = _get_header(headers, "Location")
        if not location:
            # Cannot poll without a Location; return the 202 as-is.
            break

        delay = _retry_after_seconds(headers)
        fire_at = context.current_utc_datetime + timedelta(seconds=delay)
        yield context.create_timer(fire_at)

        poll_request: dict[str, Any] = {"method": "GET", "uri": location}
        # Preserve auth for the polling requests.
        if request.get("headers") is not None:
            poll_request["headers"] = request["headers"]
        if request.get("tokenSource") is not None:
            poll_request["tokenSource"] = request["tokenSource"]

        response = yield context.call_activity(BUILTIN_HTTP_ACTIVITY_NAME, poll_request)

    return response


# The durable dispatch name is the registered function name (its ``__name__``).
# Assign the reserved names so ``call_activity`` / ``call_sub_orchestrator`` in
# ``call_http`` resolve to these built-ins.
builtin_http_activity.__name__ = BUILTIN_HTTP_ACTIVITY_NAME
builtin_http_poll_orchestrator.__name__ = BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME
