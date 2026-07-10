# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from azure.durable_functions.http.builtin import (
    BUILTIN_HTTP_ACTIVITY_NAME,
    BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME,
    builtin_http_activity,
    builtin_http_poll_orchestrator,
)
from azure.durable_functions.http.models import (
    DurableHttpRequest,
    DurableHttpResponse,
)
from azure.durable_functions.internal.compat.token_source import (
    ManagedIdentityTokenSource,
)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_request_property_getters():
    token = ManagedIdentityTokenSource("https://management.core.windows.net/")
    req = DurableHttpRequest(
        "POST", "http://example.com", content="body",
        headers={"h": "v"}, token_source=token)
    assert req.method == "POST"
    assert req.uri == "http://example.com"
    assert req.content == "body"
    assert req.headers == {"h": "v"}
    assert req.token_source is token


def test_request_optional_getters_default_to_none():
    req = DurableHttpRequest("GET", "http://example.com")
    assert req.content is None
    assert req.headers is None
    assert req.token_source is None


def test_request_to_json_minimal():
    req = DurableHttpRequest("GET", "http://example.com")
    assert req.to_json() == {"method": "GET", "uri": "http://example.com"}


def test_request_to_json_full():
    token = ManagedIdentityTokenSource("https://management.core.windows.net/")
    req = DurableHttpRequest(
        "POST", "http://example.com", content='{"a": 1}',
        headers={"h": "v"}, token_source=token)
    assert req.to_json() == {
        "method": "POST",
        "uri": "http://example.com",
        "content": '{"a": 1}',
        "headers": {"h": "v"},
        "tokenSource": {"resource": "https://management.core.windows.net/",
                        "kind": "AzureManagedIdentity"},
    }


def test_response_round_trip():
    resp = DurableHttpResponse(200, {"h": "v"}, "body")
    restored = DurableHttpResponse.from_json(resp.to_json())
    assert restored.status_code == 200
    assert restored.headers == {"h": "v"}
    assert restored.content == "body"


def test_response_from_json_accepts_camel_case():
    restored = DurableHttpResponse.from_json({"statusCode": 404})
    assert restored.status_code == 404
    assert restored.headers == {}
    assert restored.content is None


# ---------------------------------------------------------------------------
# Built-in activity
# ---------------------------------------------------------------------------

def _fake_urlopen_response(status, headers, body):
    resp = MagicMock()
    resp.status = status
    resp.headers.items.return_value = list(headers.items())
    resp.read.return_value = body.encode("utf-8")
    resp.__enter__.return_value = resp
    resp.__exit__.return_value = False
    return resp


def test_activity_executes_request():
    fake_resp = _fake_urlopen_response(200, {"Content-Type": "application/json"}, "ok")
    with patch("azure.durable_functions.http.builtin.urllib.request.urlopen",
               return_value=fake_resp):
        result = builtin_http_activity({"method": "GET", "uri": "http://example.com"})
    assert result["status_code"] == 200
    assert result["headers"] == {"Content-Type": "application/json"}
    assert result["content"] == "ok"


def test_activity_requires_uri():
    with pytest.raises(ValueError):
        builtin_http_activity({"method": "GET"})


def test_activity_adds_bearer_token_for_token_source():
    fake_resp = _fake_urlopen_response(200, {}, "ok")
    captured = {}

    def _capture(req):
        captured["headers"] = dict(req.headers)
        return fake_resp

    fake_credential = MagicMock()
    fake_credential.get_token.return_value = SimpleNamespace(token="THE_TOKEN")
    with patch("azure.durable_functions.http.builtin.urllib.request.urlopen",
               side_effect=_capture), \
            patch("azure.identity.DefaultAzureCredential",
                  return_value=fake_credential):
        builtin_http_activity({
            "method": "GET",
            "uri": "http://example.com",
            "tokenSource": {"resource": "https://management.core.windows.net/"},
        })

    # urllib normalizes header keys to title-case.
    assert captured["headers"]["Authorization"] == "Bearer THE_TOKEN"
    fake_credential.get_token.assert_called_once_with(
        "https://management.core.windows.net/.default")


# ---------------------------------------------------------------------------
# Built-in poll orchestrator
# ---------------------------------------------------------------------------

def _fake_orchestration_context(request):
    activity_calls = []

    def call_activity(name, inp):
        activity_calls.append((name, inp))
        return ("activity_task", len(activity_calls))

    def create_timer(fire_at):
        return ("timer", fire_at)

    ctx = SimpleNamespace(
        get_input=lambda: request,
        call_activity=call_activity,
        create_timer=create_timer,
        current_utc_datetime=datetime(2020, 1, 1, tzinfo=timezone.utc),
        _activity_calls=activity_calls,
    )
    return ctx


def test_poll_orchestrator_returns_non_202_immediately():
    ctx = _fake_orchestration_context({"method": "GET", "uri": "http://x"})
    gen = builtin_http_poll_orchestrator(ctx)
    assert next(gen) == ("activity_task", 1)
    with pytest.raises(StopIteration) as stop:
        gen.send({"status_code": 200, "headers": {}, "content": "done"})
    assert stop.value.value["status_code"] == 200
    assert len(ctx._activity_calls) == 1


def test_poll_orchestrator_polls_until_complete():
    request = {"method": "GET", "uri": "http://x",
               "headers": {"h": "v"}, "tokenSource": {"resource": "r"}}
    ctx = _fake_orchestration_context(request)
    gen = builtin_http_poll_orchestrator(ctx)

    # First request yields the initial activity task.
    assert next(gen) == ("activity_task", 1)

    # A 202 with a Location + Retry-After schedules a durable timer.
    timer = gen.send({
        "status_code": 202,
        "headers": {"Location": "http://poll", "Retry-After": "5"},
        "content": None,
    })
    assert timer[0] == "timer"
    assert timer[1] == ctx.current_utc_datetime + timedelta(seconds=5)

    # After the timer, the Location URL is polled via the activity.
    assert gen.send(None) == ("activity_task", 2)
    poll_name, poll_input = ctx._activity_calls[1]
    assert poll_name == BUILTIN_HTTP_ACTIVITY_NAME
    assert poll_input == {
        "method": "GET",
        "uri": "http://poll",
        "headers": {"h": "v"},
        "tokenSource": {"resource": "r"},
    }

    # A final 200 completes the orchestration.
    with pytest.raises(StopIteration) as stop:
        gen.send({"status_code": 200, "headers": {}, "content": "done"})
    assert stop.value.value["content"] == "done"


def test_poll_orchestrator_stops_when_202_has_no_location():
    ctx = _fake_orchestration_context({"method": "GET", "uri": "http://x"})
    gen = builtin_http_poll_orchestrator(ctx)
    next(gen)
    with pytest.raises(StopIteration) as stop:
        gen.send({"status_code": 202, "headers": {}, "content": None})
    assert stop.value.value["status_code"] == 202
    assert len(ctx._activity_calls) == 1


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def test_builtins_registered_under_reserved_names():
    import azure.durable_functions as df

    app = df.DFApp()
    names = {f.get_function_name() for f in app.get_functions()}
    assert BUILTIN_HTTP_ACTIVITY_NAME in names
    assert BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME in names
