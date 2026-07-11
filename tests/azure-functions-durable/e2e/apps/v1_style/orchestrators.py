# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Orchestrator functions for the V1-style sample app (blueprint).

Every orchestrator uses the classic v1 single-argument generator style
(``def orch(context):``) and the ``DurableOrchestrationContext`` API. Together
these cover the full v1 orchestration surface -- activity chaining,
fan-out/fan-in, sub-orchestrations, external events, timers, entities, custom
status, continue-as-new, retries, deterministic IDs, context properties -- plus
the documented failure paths (activity/sub-orchestration failures, exhausted
retries, and the ``histories`` NotImplementedError).
"""

from datetime import timedelta

import azure.durable_functions as df

bp = df.Blueprint()


# ---------------------------------------------------------------------------
# Core patterns
# ---------------------------------------------------------------------------

@bp.orchestration_trigger(context_name="context")
def activity_chain(context: df.DurableOrchestrationContext):
    first = yield context.call_activity("say_hello", "Tokyo")
    second = yield context.call_activity("say_hello", "Seattle")
    third = yield context.call_activity("say_hello", "London")
    return [first, second, third]


@bp.orchestration_trigger(context_name="context")
def fan_out_fan_in(context: df.DurableOrchestrationContext):
    count = context.get_input() or 5
    tasks = [context.call_activity("square", i) for i in range(1, count + 1)]
    results = yield context.task_all(tasks)
    return sum(results)


@bp.orchestration_trigger(context_name="context")
def sub_orchestration_parent(context: df.DurableOrchestrationContext):
    child_result = yield context.call_sub_orchestrator("activity_chain")
    return {"from_child": child_result}


@bp.orchestration_trigger(context_name="context")
def wait_for_approval(context: df.DurableOrchestrationContext):
    context.set_custom_status("waiting")
    approved = yield context.wait_for_external_event("approval")
    context.set_custom_status("received")
    return {"approved": approved}


@bp.orchestration_trigger(context_name="context")
def counter_orchestration(context: df.DurableOrchestrationContext):
    entity_id = df.EntityId("counter", context.instance_id)
    yield context.call_entity(entity_id, "add", 5)
    yield context.call_entity(entity_id, "add", 3)
    total = yield context.call_entity(entity_id, "get")
    return total


@bp.orchestration_trigger(context_name="context")
def continue_as_new_counter(context: df.DurableOrchestrationContext):
    # Count up to 5 across continue-as-new generations, then stop.
    value = context.get_input() or 0
    value += 1
    if value < 5:
        context.continue_as_new(value)
        return value
    return value


# ---------------------------------------------------------------------------
# Timers and task_any (select)
# ---------------------------------------------------------------------------

@bp.orchestration_trigger(context_name="context")
def timer_wait(context: df.DurableOrchestrationContext):
    deadline = context.current_utc_datetime + timedelta(seconds=2)
    yield context.create_timer(deadline)
    return "fired"


@bp.orchestration_trigger(context_name="context")
def event_or_timeout(context: df.DurableOrchestrationContext):
    event_task = context.wait_for_external_event("go")
    timeout_task = context.create_timer(
        context.current_utc_datetime + timedelta(seconds=30))
    winner = yield context.task_any([event_task, timeout_task])
    if winner is event_task:
        return {"result": "event", "data": event_task.get_result()}
    return {"result": "timeout"}


# ---------------------------------------------------------------------------
# Deterministic IDs, context properties, parent/child
# ---------------------------------------------------------------------------

@bp.orchestration_trigger(context_name="context")
def deterministic_ids(context: df.DurableOrchestrationContext):
    return {"uuid": context.new_uuid(), "guid": str(context.new_guid())}


@bp.orchestration_trigger(context_name="context")
def context_properties(context: df.DurableOrchestrationContext):
    # Yield once so the orchestrator replays at least once.
    yield context.call_activity("say_hello", "probe")
    return {
        "instance_id": context.instance_id,
        "is_replaying": context.is_replaying,
        "version": context.version,
        "parent_instance_id": context.parent_instance_id,
        "has_current_utc_datetime": context.current_utc_datetime is not None,
        "will_continue_as_new": context.will_continue_as_new,
        "has_function_context": context.function_context is not None,
    }


@bp.orchestration_trigger(context_name="context")
def child_reports_parent(context: df.DurableOrchestrationContext):
    return {"parent": context.parent_instance_id, "instance": context.instance_id}


@bp.orchestration_trigger(context_name="context")
def parent_with_child(context: df.DurableOrchestrationContext):
    child = yield context.call_sub_orchestrator("child_reports_parent")
    return {"parent_seen_by_child": child["parent"], "my_instance": context.instance_id}


# ---------------------------------------------------------------------------
# Entity interactions from an orchestrator
# ---------------------------------------------------------------------------

@bp.orchestration_trigger(context_name="context")
def signal_counter(context: df.DurableOrchestrationContext):
    key = context.get_input()
    entity_id = df.EntityId("counter", key)
    # Fire-and-forget signal from within an orchestration.
    context.signal_entity(entity_id, "add", 10)
    return key


@bp.orchestration_trigger(context_name="context")
def describe_entity(context: df.DurableOrchestrationContext):
    key = context.get_input()
    entity_id = df.EntityId("probe", key)
    description = yield context.call_entity(entity_id, "describe")
    return description


# ---------------------------------------------------------------------------
# Retries
# ---------------------------------------------------------------------------

@bp.orchestration_trigger(context_name="context")
def retry_then_succeed(context: df.DurableOrchestrationContext):
    key = context.new_uuid()
    options = df.RetryOptions(
        first_retry_interval_in_milliseconds=100, max_number_of_attempts=5)
    result = yield context.call_activity_with_retry(
        "flaky", options, {"key": key, "threshold": 3})
    return result


@bp.orchestration_trigger(context_name="context")
def retry_exhausted(context: df.DurableOrchestrationContext):
    options = df.RetryOptions(
        first_retry_interval_in_milliseconds=100, max_number_of_attempts=2)
    result = yield context.call_activity_with_retry("always_fail", options, "still failing")
    return result


@bp.orchestration_trigger(context_name="context")
def flaky_suborch(context: df.DurableOrchestrationContext):
    payload = context.get_input()
    result = yield context.call_activity("flaky", payload)
    return result


@bp.orchestration_trigger(context_name="context")
def suborch_retry_then_succeed(context: df.DurableOrchestrationContext):
    key = context.new_uuid()
    options = df.RetryOptions(
        first_retry_interval_in_milliseconds=100, max_number_of_attempts=5)
    result = yield context.call_sub_orchestrator_with_retry(
        "flaky_suborch", options, {"key": key, "threshold": 2})
    return result


# ---------------------------------------------------------------------------
# Failure paths
# ---------------------------------------------------------------------------

@bp.orchestration_trigger(context_name="context")
def activity_fails(context: df.DurableOrchestrationContext):
    result = yield context.call_activity("always_fail", "boom")
    return result


@bp.orchestration_trigger(context_name="context")
def sub_orch_fails(context: df.DurableOrchestrationContext):
    result = yield context.call_sub_orchestrator("activity_fails")
    return result


@bp.orchestration_trigger(context_name="context")
def rewind_target(context: df.DurableOrchestrationContext):
    # Calls an activity that fails on its first attempt. The orchestration
    # fails; a client rewind replays the failed activity, which then succeeds.
    result = yield context.call_activity("fail_once", context.instance_id)
    return result


@bp.orchestration_trigger(context_name="context")
def access_histories(context: df.DurableOrchestrationContext):
    # histories is intentionally unsupported and raises NotImplementedError,
    # which surfaces as a failed orchestration.
    _ = context.histories
    return "unreachable"
