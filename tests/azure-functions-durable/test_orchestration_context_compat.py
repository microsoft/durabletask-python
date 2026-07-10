# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from azure.durable_functions.internal.compat.orchestration_context import (
    DurableOrchestrationContext,
    accepts_two_positional_args,
    wrap_orchestrator,
)


# ---------------------------------------------------------------------------
# Adapter delegation
# ---------------------------------------------------------------------------

def _adapter(orchestration_input=None):
    fake_ctx = MagicMock()
    fake_ctx.instance_id = "iid"
    fake_ctx.is_replaying = True
    return DurableOrchestrationContext(fake_ctx, orchestration_input), fake_ctx


def test_get_input_returns_stored_input():
    adapter, _ = _adapter({"x": 1})
    assert adapter.get_input() == {"x": 1}


def test_property_delegation():
    adapter, fake = _adapter()
    assert adapter.instance_id == "iid"
    assert adapter.is_replaying is True
    assert adapter.current_utc_datetime is fake.current_utc_datetime


def test_call_activity_delegates():
    adapter, fake = _adapter()
    adapter.call_activity("A", input_=3)
    fake.call_activity.assert_called_once_with("A", input=3)


def test_call_activity_with_retry_delegates():
    adapter, fake = _adapter()
    retry = object()
    adapter.call_activity_with_retry("A", retry, input_=4)
    fake.call_activity.assert_called_once_with("A", input=4, retry_policy=retry)


def test_call_sub_orchestrator_delegates():
    adapter, fake = _adapter()
    adapter.call_sub_orchestrator("Sub", input_=1, instance_id="sid")
    fake.call_sub_orchestrator.assert_called_once_with("Sub", input=1, instance_id="sid")


def test_call_sub_orchestrator_with_retry_delegates():
    adapter, fake = _adapter()
    retry = object()
    adapter.call_sub_orchestrator_with_retry("Sub", retry, input_=1, instance_id="sid")
    fake.call_sub_orchestrator.assert_called_once_with(
        "Sub", input=1, instance_id="sid", retry_policy=retry)


def test_wait_for_external_event_maps_expected_type():
    adapter, fake = _adapter()
    adapter.wait_for_external_event("evt", expected_type=str)
    fake.wait_for_external_event.assert_called_once_with("evt", data_type=str)


def test_create_timer_delegates():
    adapter, fake = _adapter()
    adapter.create_timer("fire_at")
    fake.create_timer.assert_called_once_with("fire_at")


def test_continue_as_new_and_set_custom_status_delegate():
    adapter, fake = _adapter()
    adapter.continue_as_new({"n": 1})
    fake.continue_as_new.assert_called_once_with({"n": 1})
    adapter.set_custom_status("status")
    fake.set_custom_status.assert_called_once_with("status")


def test_entity_operations_delegate():
    adapter, fake = _adapter()
    adapter.call_entity("@e@k", "op", 1)
    fake.call_entity.assert_called_once_with("@e@k", "op", 1)
    adapter.signal_entity("@e@k", "op", 2)
    fake.signal_entity.assert_called_once_with("@e@k", "op", input=2)


def test_new_uuid_and_new_guid():
    adapter, fake = _adapter()
    fake.new_uuid.return_value = "12345678-1234-5678-1234-567812345678"
    assert adapter.new_uuid() == "12345678-1234-5678-1234-567812345678"
    guid = adapter.new_guid()
    assert isinstance(guid, UUID)
    assert str(guid) == "12345678-1234-5678-1234-567812345678"


def test_task_all_and_task_any_use_when_helpers():
    adapter, _ = _adapter()
    with patch("durabletask.task.when_all", return_value="ALL") as when_all, \
            patch("durabletask.task.when_any", return_value="ANY") as when_any:
        assert adapter.task_all(["t1", "t2"]) == "ALL"
        assert adapter.task_any(["t1", "t2"]) == "ANY"
    when_all.assert_called_once_with(["t1", "t2"])
    when_any.assert_called_once_with(["t1", "t2"])


def test_call_http_schedules_poll_sub_orchestrator():
    from azure.durable_functions.http.builtin import (
        BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME,
    )
    from azure.durable_functions.http.models import DurableHttpResponse

    adapter, fake = _adapter()
    adapter.call_http("GET", "http://example.com")

    fake.call_sub_orchestrator.assert_called_once()
    args, kwargs = fake.call_sub_orchestrator.call_args
    assert args[0] == BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME
    assert kwargs["input"] == {"method": "GET", "uri": "http://example.com"}
    assert kwargs["return_type"] is DurableHttpResponse


def test_call_http_serializes_content_and_token_source():
    from azure.durable_functions.internal.compat.token_source import (
        ManagedIdentityTokenSource,
    )

    adapter, fake = _adapter()
    token = ManagedIdentityTokenSource("https://management.core.windows.net/")
    adapter.call_http(
        "POST", "http://example.com",
        content={"a": 1}, headers={"h": "v"}, token_source=token)

    payload = fake.call_sub_orchestrator.call_args.kwargs["input"]
    assert payload["method"] == "POST"
    assert payload["content"] == '{"a": 1}'
    assert payload["headers"] == {"h": "v"}
    assert payload["tokenSource"]["resource"] == "https://management.core.windows.net/"


def test_call_http_raw_str_content_is_not_json_encoded():
    adapter, fake = _adapter()
    adapter.call_http("POST", "http://example.com", content="raw", is_raw_str=True)
    payload = fake.call_sub_orchestrator.call_args.kwargs["input"]
    assert payload["content"] == "raw"


def test_call_http_is_raw_str_requires_str_content():
    adapter, _ = _adapter()
    with pytest.raises(TypeError):
        adapter.call_http("POST", "http://example.com", content={"a": 1}, is_raw_str=True)


# ---------------------------------------------------------------------------
# Additional context members
# ---------------------------------------------------------------------------

def test_custom_status_tracks_set_custom_status():
    adapter, fake = _adapter()
    assert adapter.custom_status is None
    adapter.set_custom_status({"progress": 50})
    assert adapter.custom_status == {"progress": 50}
    fake.set_custom_status.assert_called_once_with({"progress": 50})


def test_will_continue_as_new_tracks_continue_as_new():
    adapter, fake = _adapter()
    assert adapter.will_continue_as_new is False
    adapter.continue_as_new({"next": 1})
    assert adapter.will_continue_as_new is True
    fake.continue_as_new.assert_called_once_with({"next": 1})


def test_parent_instance_id_delegates():
    adapter, fake = _adapter()
    fake.parent_instance_id = "parent-123"
    assert adapter.parent_instance_id == "parent-123"


def test_function_context_returns_empty_bag():
    from azure.durable_functions.internal.compat.function_context import FunctionContext
    adapter, _ = _adapter()
    fc = adapter.function_context
    assert isinstance(fc, FunctionContext)
    # Empty by default: no extra attributes, matching the common v1 case.
    assert [a for a in vars(fc)] == []


def test_histories_raises_not_implemented():
    adapter, _ = _adapter()
    with pytest.raises(NotImplementedError):
        _ = adapter.histories


# ---------------------------------------------------------------------------
# Arity detection and wrapping
# ---------------------------------------------------------------------------

def test_accepts_two_positional_args():
    assert accepts_two_positional_args(lambda ctx, inp: None) is True
    assert accepts_two_positional_args(lambda ctx: None) is False
    assert accepts_two_positional_args(lambda *args: None) is True


def test_wrap_passes_through_two_arg_orchestrator():
    def orch(ctx, inp):
        return None
    assert wrap_orchestrator(orch) is orch


def test_wrap_adapts_one_arg_non_generator():
    seen = {}

    def orch(context):
        seen["input"] = context.get_input()
        return "done"

    wrapped = wrap_orchestrator(orch)
    assert wrapped is not orch
    fake_ctx = MagicMock()
    result = wrapped(fake_ctx, 42)
    assert result == "done"
    assert seen["input"] == 42


def test_wrap_adapts_one_arg_generator_end_to_end():
    seen = {}

    def orch(context):
        seen["input"] = context.get_input()
        activity_result = yield context.call_activity("A", input_=5)
        seen["activity_result"] = activity_result
        return activity_result * 2

    wrapped = wrap_orchestrator(orch)
    fake_ctx = MagicMock()
    fake_ctx.call_activity.return_value = "SCHEDULED_TASK"

    gen = wrapped(fake_ctx, 7)
    # First advance schedules the activity and yields the durabletask task.
    yielded = next(gen)
    assert yielded == "SCHEDULED_TASK"
    fake_ctx.call_activity.assert_called_once_with("A", input=5)
    assert seen["input"] == 7

    # Feeding the activity result resumes the orchestrator to completion.
    with pytest.raises(StopIteration) as stop:
        gen.send(10)
    assert stop.value.value == 20
    assert seen["activity_result"] == 10


def test_wrap_preserves_orchestrator_name():
    def my_orchestrator(context):
        return None
    assert wrap_orchestrator(my_orchestrator).__name__ == "my_orchestrator"
