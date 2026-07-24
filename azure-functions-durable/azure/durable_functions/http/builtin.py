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
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Generator, Optional
from urllib.parse import urljoin, urlparse

from .models import DurableHttpResponse

# Reserved built-in function names. The v1 host used ``BuiltIn::HttpActivity``;
# ``::`` is not a valid Azure Functions function name, so ``__`` is used here.
# The reserved names are unlikely to collide with user-defined functions.
BUILTIN_HTTP_ACTIVITY_NAME = "BuiltIn__HttpActivity"
BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME = "BuiltIn__HttpPollOrchestrator"

# Fallback interval (seconds) between polls when the 202 response carries no
# usable ``Retry-After`` header.
_DEFAULT_POLL_INTERVAL_SECONDS = 1

# Process-wide credential cache. ``DefaultAzureCredential`` is safe to reuse and
# caches tokens internally, so a single worker-local instance avoids repeating
# credential-chain discovery and token acquisition on every durable HTTP
# activity. This is worker-local state: each worker process holds its own
# instance, which is naturally rebuilt when the process recycles.
_cached_credential: Optional[Any] = None


def _get_credential() -> Any:
    """Return a lazily-created, process-wide ``DefaultAzureCredential``.

    ``azure-identity`` is imported lazily so the dependency is only touched when
    a token source is actually used.
    """
    global _cached_credential
    if _cached_credential is None:
        from azure.identity import DefaultAzureCredential

        _cached_credential = DefaultAzureCredential()
    return _cached_credential


def _acquire_bearer_token(resource: str) -> str:
    """Acquire an AAD bearer token for ``resource`` via ``azure-identity``.

    Uses a process-wide :class:`DefaultAzureCredential` (see
    :func:`_get_credential`) so a long-running ``202`` managed-identity poll does
    not repeat credential-chain discovery and token acquisition on every poll.
    """
    scope = resource.rstrip("/") + "/.default"
    return _get_credential().get_token(scope).token


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
    # Durable HTTP only ever means http(s); reject other schemes (file://,
    # ftp://, ...) that urlopen would otherwise honor, closing off local-file
    # reads / SSRF to non-HTTP endpoints from orchestration-supplied URLs.
    if urlparse(str(uri)).scheme.lower() not in ("http", "https"):
        raise ValueError(
            "call_http only supports http/https URLs; "
            f"got {uri!r}.")
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


def _retry_after_seconds(headers: dict[str, str], now: datetime) -> int:
    """Parse the ``Retry-After`` header into a delay in seconds.

    Supports both the delta-seconds and HTTP-date forms; falls back to
    :data:`_DEFAULT_POLL_INTERVAL_SECONDS` when absent or unparseable. For the
    HTTP-date form the delay is computed against ``now`` -- which the caller
    supplies as the orchestration's replay-safe ``current_utc_datetime`` -- so
    the resulting timer fire time is deterministic across replays.
    """
    raw = _get_header(headers, "Retry-After")
    if raw is None:
        return _DEFAULT_POLL_INTERVAL_SECONDS
    raw = raw.strip()
    if raw.isdigit():
        return max(int(raw), 0)
    try:
        retry_at = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return _DEFAULT_POLL_INTERVAL_SECONDS
    # ``parsedate_to_datetime`` may return a naive datetime (no zone in the
    # header); treat it as UTC. Normalize ``now`` the same way so the
    # subtraction is well-defined regardless of the caller's tz-awareness.
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return max(int((retry_at - now).total_seconds()), 0)


def builtin_http_poll_orchestrator(context: Any) -> Generator[Any, Any, DurableHttpResponse]:
    """Issue a durable HTTP request and poll while it returns ``202``.

    Receives the request payload as its input, calls the built-in HTTP activity,
    and while the response is ``202 Accepted`` with a ``Location`` header waits
    on a durable timer (honoring ``Retry-After``) before re-polling the
    ``Location`` URL. Returns the final response as a
    :class:`~azure.durable_functions.http.models.DurableHttpResponse`.

    The built-in activity returns the response as a plain JSON ``dict`` (the
    ``DurableHttpResponse`` wire form), which the polling loop inspects directly.
    The orchestrator returns a ``DurableHttpResponse`` object so the
    sub-orchestration result crosses the wire as a type-matched custom-object
    envelope -- ``call_http`` declares ``return_type=DurableHttpResponse``, so it
    is reconstructed type-safely (required under strict typing, which will not
    build a custom type from a bare JSON object).
    """
    request: dict[str, Any] = context.get_input() or {}
    response: dict[str, Any] = yield context.call_activity(
        BUILTIN_HTTP_ACTIVITY_NAME, request)

    # Track the URI of the most recent request so a relative ``Location`` can be
    # resolved against it.
    current_uri: str = str(request.get("uri") or "")

    while response.get("status_code") == 202:
        headers: dict[str, str] = response.get("headers") or {}
        location = _get_header(headers, "Location")
        if not location:
            # Cannot poll without a Location; return the 202 as-is.
            break

        # A ``Location`` may be relative (e.g. ``/operations/42``); resolve it
        # against the current request URI so the next poll targets an absolute
        # http(s) URL (the built-in activity rejects non-absolute URIs).
        location = urljoin(current_uri, location)

        now = context.current_utc_datetime
        delay = _retry_after_seconds(headers, now)
        fire_at = now + timedelta(seconds=delay)
        yield context.create_timer(fire_at)

        poll_request: dict[str, Any] = {"method": "GET", "uri": location}
        # Preserve auth for the polling requests.
        if request.get("headers") is not None:
            poll_request["headers"] = request["headers"]
        if request.get("tokenSource") is not None:
            poll_request["tokenSource"] = request["tokenSource"]

        current_uri = location
        response = yield context.call_activity(BUILTIN_HTTP_ACTIVITY_NAME, poll_request)

    return DurableHttpResponse.from_json(response)


# The durable dispatch name is the registered function name (its ``__name__``).
# Assign the reserved names so ``call_activity`` / ``call_sub_orchestrator`` in
# ``call_http`` resolve to these built-ins.
builtin_http_activity.__name__ = BUILTIN_HTTP_ACTIVITY_NAME
builtin_http_poll_orchestrator.__name__ = BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME
