# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import base64
import io
import json
import urllib.error
from datetime import datetime, timedelta, tzinfo
from unittest.mock import MagicMock, patch

import pytest

import azure.durable_functions as df
from azure.durable_functions.http.builtin import (
    BUILTIN_HTTP_ACTIVITY_NAME,
    BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME,
    builtin_http_activity,
    builtin_http_poll_orchestrator,
)
from azure.durable_functions.internal.serialization import DEFAULT_FUNCTIONS_DATA_CONVERTER
from azure.durable_functions.worker import DurableFunctionsWorker
from durabletask import task
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb


def _options(**kwargs):
    return df.HttpRetryOptions(
        first_retry_interval=timedelta(seconds=2),
        max_number_of_attempts=3,
        **kwargs)


def test_http_retry_defaults_and_round_trip():
    policy = _options()
    assert isinstance(policy, df.RetryPolicy)
    assert policy.backoff_coefficient == 1
    assert policy.max_retry_interval == timedelta(days=6)
    assert policy.retry_timeout is None
    assert policy.status_codes_to_retry is None
    assert policy.to_json() == {
        "firstRetryIntervalInMilliseconds": 2000,
        "maxNumberOfAttempts": 3,
        "backoffCoefficient": 1,
        "maxRetryIntervalInMilliseconds": 518400000,
        "retryTimeoutInMilliseconds": None,
        "statusCodesToRetry": None,
    }
    assert df.HttpRetryOptions.from_json(json.loads(json.dumps(policy.to_json()))).to_json() == policy.to_json()


def test_http_retry_from_json_with_missing_optional_fields():
    # Only the required fields provided (as when serialized by minimal JSON or external SDKs)
    wire = {
        "firstRetryIntervalInMilliseconds": 1500,
        "maxNumberOfAttempts": 4,
    }
    policy = df.HttpRetryOptions.from_json(wire)
    assert policy.first_retry_interval == timedelta(milliseconds=1500)
    assert policy.max_number_of_attempts == 4
    assert policy.backoff_coefficient == 1.0
    assert policy.max_retry_interval == timedelta(days=6)
    assert policy.retry_timeout is None
    assert policy.status_codes_to_retry is None

    # Wire payload explicitly setting maxRetryInterval to null (unbounded)
    unbounded_wire = {
        "firstRetryIntervalInMilliseconds": 2000,
        "maxNumberOfAttempts": 2,
        "maxRetryIntervalInMilliseconds": None,
    }
    unbounded_policy = df.HttpRetryOptions.from_json(unbounded_wire)
    assert unbounded_policy.max_retry_interval is None
    assert unbounded_policy.retry_timeout is None


def test_http_retry_from_json_with_snake_case_keys():
    snake_wire = {
        "first_retry_interval_in_milliseconds": 2500,
        "max_number_of_attempts": 5,
        "backoff_coefficient": 1.5,
        "max_retry_interval_in_milliseconds": 60000,
        "retry_timeout_in_milliseconds": 120000,
        "status_codes_to_retry": [429, 503],
    }
    policy = df.HttpRetryOptions.from_json(snake_wire)
    assert policy.first_retry_interval == timedelta(milliseconds=2500)
    assert policy.max_number_of_attempts == 5
    assert policy.backoff_coefficient == 1.5
    assert policy.max_retry_interval == timedelta(seconds=60)
    assert policy.retry_timeout == timedelta(seconds=120)
    assert policy.status_codes_to_retry == [429, 503]


def test_request_snapshots_policy_and_status_list():
    codes = [429, 503]
    policy = _options(
        backoff_coefficient=2,
        max_retry_interval=timedelta(seconds=30),
        retry_timeout=timedelta(minutes=1),
        status_codes_to_retry=codes)
    request = df.DurableHttpRequest("GET", "https://example.com", retry_options=policy)
    expected = policy.to_json()
    codes.append(500)
    # Even changes to the caller's policy after constructing the request must
    # not alter the wire snapshot.
    policy._max_number_of_attempts = 100
    policy._status_codes_to_retry.append(404)
    payload = request.to_json()
    assert payload["retryOptions"] == expected
    payload["retryOptions"]["statusCodesToRetry"].append(400)
    assert request.to_json()["retryOptions"] == expected
    assert request.retry_options.to_json() == expected
    assert df.DurableHttpRequest("GET", "https://example.com").retry_options is None


@pytest.mark.parametrize("kwargs", [
    {"first_retry_interval": timedelta(seconds=-1), "max_number_of_attempts": 2},
    {"first_retry_interval": timedelta(seconds=1), "max_number_of_attempts": 0},
    {"first_retry_interval": timedelta(seconds=1), "max_number_of_attempts": 2, "backoff_coefficient": 0.5},
    {"first_retry_interval": timedelta(seconds=1), "max_number_of_attempts": 2, "max_retry_interval": timedelta(seconds=-1)},
    {"first_retry_interval": timedelta(seconds=1), "max_number_of_attempts": 2, "retry_timeout": timedelta(seconds=-1)},
])
def test_http_retry_policy_preserves_core_validation(kwargs):
    with pytest.raises(ValueError):
        df.HttpRetryOptions(**kwargs)


@pytest.mark.parametrize("codes", [None, [], [202, 429]])
@pytest.mark.parametrize("status", [199, 200, 202, 204, 302, 400, 429, 503])
@pytest.mark.parametrize("http_error", [False, True])
def test_activity_classifies_before_reading_and_closes_response(codes, status, http_error):
    selected = status in codes if codes else not 200 <= status < 300
    if http_error:
        body = io.BytesIO(b"response")
        response = urllib.error.HTTPError(
            "https://example.com", status, "response", {"X-Test": "value"}, body)
        read = MagicMock(wraps=response.read)
        response.read = read
        open_options = {"side_effect": response}
    else:
        response = MagicMock()
        response.status = status
        response.headers = {"X-Test": "value"}
        response.__enter__.return_value = response
        read = response.read
        read.return_value = b"response"
        open_options = {"return_value": response}
    if selected:
        read.side_effect = AssertionError("Selected error bodies must never be read")

    with patch("azure.durable_functions.http.builtin._open_http_request", **open_options):
        request = df.DurableHttpRequest(
            "GET", "https://example.com",
            retry_options=_options(status_codes_to_retry=codes)).to_json()
        if selected:
            with pytest.raises(RuntimeError, match=f"status code {status}"):
                builtin_http_activity(request)
            read.assert_not_called()
        else:
            assert builtin_http_activity(request) == {
                "status_code": status, "headers": {"X-Test": "value"}, "content": "response"}
            read.assert_called_once_with()
    if http_error:
        assert body.closed
    else:
        response.__exit__.assert_called_once()


def test_no_options_preserves_http_error_response():
    body = io.BytesIO(b"error detail")
    response = urllib.error.HTTPError(
        "https://example.com", 503, "unavailable", {"Retry-After": "9"}, body)
    with patch("azure.durable_functions.http.builtin._open_http_request", side_effect=response):
        assert builtin_http_activity({"method": "GET", "uri": "https://example.com"}) == {
            "status_code": 503, "headers": {"Retry-After": "9"}, "content": "error detail"}
    assert body.closed


class _History:
    """Dispatch through the Functions worker, recreating it for every replay."""

    def __init__(self, func, input=None, parent=None):
        self.func = func
        self.now = datetime(2020, 1, 1)
        self.events = []
        started = helpers.new_execution_started_event(
            func.__name__, "instance", json.dumps(input))
        if parent is not None:
            started.executionStarted.parentInstance.orchestrationInstance.instanceId = parent
        self.actions = self.dispatch([started])

    def execute(self, past, new):
        request = pb.OrchestratorRequest(
            instanceId="instance", pastEvents=past, newEvents=new)
        encoded = base64.b64encode(request.SerializeToString()).decode()
        output = DurableFunctionsWorker().execute_orchestration_request(self.func, encoded)
        response = pb.OrchestratorResponse()
        response.ParseFromString(base64.b64decode(output))
        return list(response.actions)

    def dispatch(self, events):
        new = [helpers.new_orchestrator_started_event(self.now), *events]
        actions = self.execute(self.events, new)
        self.events.extend(new)
        self.actions = actions
        return actions

    def only_action(self, kind):
        assert len(self.actions) == 1
        action = self.actions[0]
        assert action.HasField(kind), action
        return action

    def run_activity(self, *, status=200, headers=None, error=None):
        action = self.only_action("scheduleTask")
        assert action.scheduleTask.name == BUILTIN_HTTP_ACTIVITY_NAME
        request = json.loads(action.scheduleTask.input.value)
        self.events.append(helpers.new_task_scheduled_event(
            action.id, BUILTIN_HTTP_ACTIVITY_NAME, encoded_input=action.scheduleTask.input.value))
        response = MagicMock()
        response.status = status
        response.headers = headers or {}
        response.read.return_value = b"body"
        response.__enter__.return_value = response
        with patch("azure.durable_functions.http.builtin._open_http_request",
                   **({"side_effect": error} if error else {"return_value": response})):
            try:
                result = builtin_http_activity(request)
            except Exception as exc:
                completion = helpers.new_task_failed_event(action.id, exc)
            else:
                completion = helpers.new_task_completed_event(action.id, json.dumps(result))
        self.dispatch([completion])
        return request

    def fire_timer(self, expected_delay):
        action = self.only_action("createTimer")
        fire_at = action.createTimer.fireAt.ToDatetime()
        assert fire_at == self.now + expected_delay
        self.events.append(helpers.new_timer_created_event(action.id, fire_at))
        self.now = fire_at
        self.dispatch([helpers.new_timer_fired_event(action.id, fire_at)])

    def assert_replay(self):
        # Replay old history with no new completions: the emitted action must
        # match even though the real wall clock is years ahead of this history.
        replayed = self.execute(self.events, [helpers.new_orchestrator_started_event(self.now)])
        expected = [pb.OrchestratorAction.FromString(action.SerializeToString()) for action in self.actions]
        for action in replayed + expected:
            if action.HasField("completeOrchestration"):
                # The outer traceback includes a different executor line when
                # the failure is processed as an old event rather than new.
                action.completeOrchestration.failureDetails.ClearField("stackTrace")
        assert replayed == expected

    def result(self):
        completion = self.only_action("completeOrchestration").completeOrchestration
        assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED, completion
        return DEFAULT_FUNCTIONS_DATA_CONVERTER.deserialize(
            completion.result.value, df.DurableHttpResponse)


def _poll_history(options):
    request = df.DurableHttpRequest(
        "POST", "https://example.com/start",
        content="request", headers={"X-Test": "value"}, retry_options=options)
    return _History(builtin_http_poll_orchestrator, request.to_json(), parent="caller")


@pytest.mark.parametrize("transport_error", [False, True])
def test_durable_retry_then_success(transport_error):
    history = _poll_history(_options(status_codes_to_retry=[503]))
    request = history.run_activity(
        status=503, error=urllib.error.URLError("disconnected") if transport_error else None)
    history.assert_replay()
    history.fire_timer(timedelta(seconds=2))
    assert history.run_activity() == request
    assert history.result().status_code == 200
    assert history.result().content == "body"


def test_default_backoff_is_constant_and_timeout_is_unbounded():
    history = _poll_history(_options())
    history.now += timedelta(days=365)
    for _ in range(2):
        history.run_activity(status=503)
        history.fire_timer(timedelta(seconds=2))
    history.run_activity()
    assert history.result().status_code == 200


@pytest.mark.parametrize("remove_cap", [False, True])
def test_default_six_day_cap_and_explicit_unbounded_interval(remove_cap):
    kwargs = {"max_retry_interval": None} if remove_cap else {}
    history = _poll_history(df.HttpRetryOptions(
        first_retry_interval=timedelta(days=8), max_number_of_attempts=2, **kwargs))
    history.run_activity(status=503)
    # Core splits long durable timers into three-day chunks.
    history.fire_timer(timedelta(days=3))
    history.fire_timer(timedelta(days=3))
    if remove_cap:
        history.fire_timer(timedelta(days=2))
    history.run_activity()
    assert history.result().status_code == 200


def test_durable_retries_backoff_cap_and_exhaustion():
    history = _poll_history(df.HttpRetryOptions(
        first_retry_interval=timedelta(seconds=2),
        max_number_of_attempts=4, backoff_coefficient=3,
        max_retry_interval=timedelta(seconds=5)))
    for delay in (2, 5, 5):
        history.run_activity(status=503)
        history.assert_replay()
        history.fire_timer(timedelta(seconds=delay))
    history.run_activity(status=503)
    history.assert_replay()
    completion = history.only_action("completeOrchestration").completeOrchestration
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert "status code 503" in str(completion.failureDetails)


@pytest.mark.parametrize("wall_clock_year", [2019, 2030])
@pytest.mark.parametrize("elapsed", [9, 10, 11])
def test_http_timeout_uses_replayed_orchestration_clock(monkeypatch, wall_clock_year, elapsed):
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            instant = datetime(wall_clock_year, 1, 1)
            return instant.replace(tzinfo=tz)

    monkeypatch.setattr(task, "datetime", FrozenDateTime)
    history = _poll_history(_options(retry_timeout=timedelta(seconds=10)))
    history.now += timedelta(seconds=elapsed)
    history.run_activity(status=503)
    history.assert_replay()
    if elapsed >= 10:
        completion = history.only_action("completeOrchestration").completeOrchestration
        assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    else:
        # Core checks elapsed time when a failure arrives, not whether the next
        # delay will cross the timeout. The retry at t=11 is still scheduled.
        history.fire_timer(timedelta(seconds=2))
        history.run_activity()
        assert history.result().status_code == 200


@pytest.mark.parametrize("codes", [[429], []])
def test_initial_202_polls_without_retrying_later_errors(codes):
    history = _poll_history(_options(status_codes_to_retry=codes))
    history.run_activity(status=202, headers={"Location": "/status", "Retry-After": "3"})
    history.fire_timer(timedelta(seconds=3))
    poll = history.run_activity(status=503)
    assert poll == {"method": "GET", "uri": "https://example.com/status", "headers": {"X-Test": "value"}}
    assert history.result().status_code == 503


def test_selected_202_retries_instead_of_polling():
    history = _poll_history(_options(status_codes_to_retry=[202]))
    original = history.run_activity(
        status=202, headers={"Location": "/status", "Retry-After": "9"})
    history.fire_timer(timedelta(seconds=2))
    assert history.run_activity() == original
    assert history.result().status_code == 200


def test_retry_then_multiple_location_polls_do_not_inherit_policy():
    history = _poll_history(_options(status_codes_to_retry=[503]))
    original = history.run_activity(status=503)
    history.fire_timer(timedelta(seconds=2))
    assert history.run_activity(status=202, headers={"Location": "/first"}) == original
    for location in ("/second", "/third"):
        history.fire_timer(timedelta(seconds=1))
        poll = history.run_activity(status=202, headers={"Location": location})
        assert "retryOptions" not in poll
    history.fire_timer(timedelta(seconds=1))
    assert "retryOptions" not in history.run_activity(status=503)
    assert history.result().status_code == 503


def test_location_poll_transport_failure_is_not_retried():
    history = _poll_history(_options())
    history.run_activity(status=202, headers={"Location": "/status"})
    history.fire_timer(timedelta(seconds=1))
    history.run_activity(error=urllib.error.URLError("poll disconnected"))
    completion = history.only_action("completeOrchestration").completeOrchestration
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED


def test_transport_failure_without_options_is_not_retried():
    history = _poll_history(None)
    history.run_activity(error=urllib.error.URLError("disconnected"))
    completion = history.only_action("completeOrchestration").completeOrchestration
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED


@pytest.mark.parametrize("with_options", [False, True])
def test_excluded_error_and_no_options_return_full_response(with_options):
    history = _poll_history(_options(status_codes_to_retry=[429]) if with_options else None)
    history.run_activity(status=503, headers={"X-Error": "detail"})
    response = history.result()
    assert response.to_json() == {"status_code": 503, "headers": {"X-Error": "detail"}, "content": "body"}


@pytest.mark.parametrize("succeed", [False, True])
def test_call_http_schedules_snapshot_and_observes_child_result(succeed):
    codes = [503]
    policy = df.HttpRetryOptions(
        first_retry_interval=timedelta(seconds=2),
        max_number_of_attempts=1, status_codes_to_retry=codes)

    def caller(context):
        pending = context.call_http("GET", "https://example.com", retry_options=policy)
        codes.append(404)
        return (yield pending)

    parent = _History(caller)
    action = parent.only_action("createSubOrchestration")
    assert action.createSubOrchestration.name == BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME
    payload = json.loads(action.createSubOrchestration.input.value)
    assert payload["retryOptions"]["statusCodesToRetry"] == [503]
    parent.events.append(helpers.new_sub_orchestration_created_event(
        action.id, BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME, "child",
        encoded_input=action.createSubOrchestration.input.value))
    child = _History(builtin_http_poll_orchestrator, payload, parent="instance")
    child.run_activity(status=200 if succeed else 503)
    completion = child.only_action("completeOrchestration").completeOrchestration
    if succeed:
        parent.dispatch([helpers.new_sub_orchestration_completed_event(action.id, completion.result.value)])
        assert parent.result().status_code == 200
    else:
        failure = helpers.new_sub_orchestration_failed_event(action.id, RuntimeError("placeholder"))
        failure.subOrchestrationInstanceFailed.failureDetails.CopyFrom(completion.failureDetails)
        parent.dispatch([failure])
        completion = parent.only_action("completeOrchestration").completeOrchestration
        assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
        assert "status code 503" in str(completion.failureDetails)
