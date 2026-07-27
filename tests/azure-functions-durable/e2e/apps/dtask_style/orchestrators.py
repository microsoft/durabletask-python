# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Orchestrator functions for the durabletask-native-style sample app (blueprint).

Every orchestrator uses the modern durabletask two-argument style
(``def orch(ctx, input):``) and the durabletask ``OrchestrationContext`` API
directly. These cover the inherited durabletask surface -- activity chaining,
fan-out/fan-in (``task.when_all``), sub-orchestrations, external events, timers,
entities (``ctx.call_entity`` / ``ctx.signal_entity``), custom status,
continue-as-new, retries (``RetryPolicy``), deterministic IDs, ``task.when_any``,
and context properties -- plus the failure paths.
"""

from datetime import timedelta
from typing import Any

import azure.durable_functions as df
from durabletask import entities, task

bp = df.Blueprint()


# ---------------------------------------------------------------------------
# Core patterns
# ---------------------------------------------------------------------------

@bp.orchestration_trigger(context_name="context")
def activity_chain(ctx: task.OrchestrationContext, _: Any):
    first = yield ctx.call_activity("say_hello", input="Tokyo")
    second = yield ctx.call_activity("say_hello", input="Seattle")
    third = yield ctx.call_activity("say_hello", input="London")
    return [first, second, third]


@bp.orchestration_trigger(context_name="context")
def fan_out_fan_in(ctx: task.OrchestrationContext, count: Any):
    count = count or 5
    tasks = [ctx.call_activity("square", input=i) for i in range(1, count + 1)]
    results = yield task.when_all(tasks)
    return sum(results)


@bp.orchestration_trigger(context_name="context")
def sub_orchestration_parent(ctx: task.OrchestrationContext, _: Any):
    child_result = yield ctx.call_sub_orchestrator("activity_chain")
    return {"from_child": child_result}


@bp.orchestration_trigger(context_name="context")
def wait_for_approval(ctx: task.OrchestrationContext, _: Any):
    ctx.set_custom_status("waiting")
    approved = yield ctx.wait_for_external_event("approval")
    ctx.set_custom_status("received")
    return {"approved": approved}


@bp.orchestration_trigger(context_name="context")
def counter_orchestration(ctx: task.OrchestrationContext, _: Any):
    entity_id = entities.EntityInstanceId("counter", ctx.instance_id)
    yield ctx.call_entity(entity_id, "add", 5)
    yield ctx.call_entity(entity_id, "add", 3)
    total = yield ctx.call_entity(entity_id, "get")
    return total


@bp.orchestration_trigger(context_name="context")
def reset_counter(ctx: task.OrchestrationContext, _: Any):
    # Exercises an entity operation that returns None (``reset``): the None
    # result must round-trip through ``call_entity`` as None.
    entity_id = entities.EntityInstanceId("counter", ctx.instance_id)
    yield ctx.call_entity(entity_id, "add", 5)
    reset_result = yield ctx.call_entity(entity_id, "reset")
    total = yield ctx.call_entity(entity_id, "get")
    return {"reset_result": reset_result, "reset_result_is_none": reset_result is None, "total": total}


@bp.orchestration_trigger(context_name="context")
def continue_as_new_counter(ctx: task.OrchestrationContext, value: Any):
    value = (value or 0) + 1
    if value < 5:
        ctx.continue_as_new(value)
        return value
    return value


# ---------------------------------------------------------------------------
# Timers and when_any (select)
# ---------------------------------------------------------------------------

@bp.orchestration_trigger(context_name="context")
def timer_wait(ctx: task.OrchestrationContext, _: Any):
    yield ctx.create_timer(ctx.current_utc_datetime + timedelta(seconds=2))
    return "fired"


@bp.orchestration_trigger(context_name="context")
def event_or_timeout(ctx: task.OrchestrationContext, _: Any):
    event_task = ctx.wait_for_external_event("go")
    timeout_task = ctx.create_timer(ctx.current_utc_datetime + timedelta(seconds=30))
    winner = yield task.when_any([event_task, timeout_task])
    if winner is event_task:
        return {"result": "event", "data": event_task.get_result()}
    return {"result": "timeout"}


# ---------------------------------------------------------------------------
# Deterministic IDs, context properties, parent/child
# ---------------------------------------------------------------------------

@bp.orchestration_trigger(context_name="context")
def deterministic_ids(ctx: task.OrchestrationContext, _: Any):
    return {"uuid1": ctx.new_uuid(), "uuid2": ctx.new_uuid()}


@bp.orchestration_trigger(context_name="context")
def context_properties(ctx: task.OrchestrationContext, _: Any):
    yield ctx.call_activity("say_hello", input="probe")
    return {
        "instance_id": ctx.instance_id,
        "is_replaying": ctx.is_replaying,
        "version": ctx.version,
        "parent_instance_id": ctx.parent_instance_id,
        "has_current_utc_datetime": ctx.current_utc_datetime is not None,
    }


@bp.orchestration_trigger(context_name="context")
def child_reports_parent(ctx: task.OrchestrationContext, _: Any):
    return {"parent": ctx.parent_instance_id, "instance": ctx.instance_id}


@bp.orchestration_trigger(context_name="context")
def parent_with_child(ctx: task.OrchestrationContext, _: Any):
    child = yield ctx.call_sub_orchestrator("child_reports_parent")
    return {"parent_seen_by_child": child["parent"], "my_instance": ctx.instance_id}


# ---------------------------------------------------------------------------
# Entity interactions from an orchestrator
# ---------------------------------------------------------------------------

@bp.orchestration_trigger(context_name="context")
def signal_counter(ctx: task.OrchestrationContext, key: Any):
    entity_id = entities.EntityInstanceId("counter", key)
    ctx.signal_entity(entity_id, "add", 10)
    return key


@bp.orchestration_trigger(context_name="context")
def describe_entity(ctx: task.OrchestrationContext, key: Any):
    entity_id = entities.EntityInstanceId("probe", key)
    description = yield ctx.call_entity(entity_id, "describe")
    return description


# ---------------------------------------------------------------------------
# Advanced entity patterns: locks, entity-to-entity signal, entity-starts-
# orchestration, and entity-operation failure propagation
# ---------------------------------------------------------------------------

@bp.orchestration_trigger(context_name="context")
def lock_and_increment(ctx: task.OrchestrationContext, key: Any):
    # Critical section around an entity via ctx.lock_entities.
    entity_id = entities.EntityInstanceId("counter", key)
    with (yield ctx.lock_entities([entity_id])):
        yield ctx.call_entity(entity_id, "add", 5)
        total = yield ctx.call_entity(entity_id, "get")
    return total


@bp.orchestration_trigger(context_name="context")
def lock_then_throw(ctx: task.OrchestrationContext, key: Any):
    # Acquire a lock, mutate the entity, then raise: the lock must be released
    # so a later orchestration can lock the same entity again.
    entity_id = entities.EntityInstanceId("counter", key)
    with (yield ctx.lock_entities([entity_id])):
        yield ctx.call_entity(entity_id, "add", 1)
        raise RuntimeError("boom after locking")


@bp.orchestration_trigger(context_name="context")
def relay_signal(ctx: task.OrchestrationContext, key: Any):
    # Orchestrator calls a Relay entity operation that in turn signals the
    # counter entity (entity-to-entity signalling).
    relay_id = entities.EntityInstanceId("relay", key)
    yield ctx.call_entity(relay_id, "signal_counter", {"key": key, "amount": 7})
    return key


@bp.orchestration_trigger(context_name="context")
def signal_counter_delayed(ctx: task.OrchestrationContext, key: Any):
    # Schedules a delayed (future) signal to the counter entity, then completes
    # immediately. The signal should only be delivered after the delay elapses.
    entity_id = entities.EntityInstanceId("counter", key)
    signal_at = ctx.current_utc_datetime + timedelta(seconds=3)
    ctx.signal_entity(entity_id, "add", 4, signal_time=signal_at)
    return key


@bp.orchestration_trigger(context_name="context")
def scheduled_tick(ctx: task.OrchestrationContext, key: Any):
    # Target orchestration for a scheduled task: each run signals the counter
    # entity so tests can observe that the schedule fired repeatedly.
    entity_id = entities.EntityInstanceId("counter", key)
    ctx.signal_entity(entity_id, "add", 1)
    return key


@bp.orchestration_trigger(context_name="context")
def relay_start_orch(ctx: task.OrchestrationContext, key: Any):
    # Orchestrator calls a Relay entity operation that schedules a new
    # orchestration (signal_counter), which then signals the counter entity.
    relay_id = entities.EntityInstanceId("relay", key)
    yield ctx.call_entity(
        relay_id, "start_orchestration", {"name": "signal_counter", "input": key})
    return key


@bp.orchestration_trigger(context_name="context")
def call_failing_entity(ctx: task.OrchestrationContext, key: Any):
    # Unhandled entity-operation failure must fail the orchestration.
    relay_id = entities.EntityInstanceId("relay", key)
    result = yield ctx.call_entity(relay_id, "boom")
    return result


@bp.orchestration_trigger(context_name="context")
def call_failing_entity_handled(ctx: task.OrchestrationContext, key: Any):
    # A caught entity-operation failure lets the orchestration complete.
    relay_id = entities.EntityInstanceId("relay", key)
    try:
        yield ctx.call_entity(relay_id, "boom")
        return {"caught": False}
    except Exception as exc:  # noqa: BLE001 - test intentionally catches broadly
        return {"caught": True, "error": str(exc)}


# ---------------------------------------------------------------------------
# Retries
# ---------------------------------------------------------------------------

@bp.orchestration_trigger(context_name="context")
def retry_then_succeed(ctx: task.OrchestrationContext, _: Any):
    key = ctx.new_uuid()
    policy = task.RetryPolicy(
        first_retry_interval=timedelta(milliseconds=100), max_number_of_attempts=5)
    result = yield ctx.call_activity(
        "flaky", input={"key": key, "threshold": 3}, retry_policy=policy)
    return result


@bp.orchestration_trigger(context_name="context")
def retry_exhausted(ctx: task.OrchestrationContext, _: Any):
    policy = task.RetryPolicy(
        first_retry_interval=timedelta(milliseconds=100), max_number_of_attempts=2)
    result = yield ctx.call_activity("always_fail", input="still failing", retry_policy=policy)
    return result


@bp.orchestration_trigger(context_name="context")
def flaky_suborch(ctx: task.OrchestrationContext, payload: Any):
    result = yield ctx.call_activity("flaky", input=payload)
    return result


@bp.orchestration_trigger(context_name="context")
def suborch_retry_then_succeed(ctx: task.OrchestrationContext, _: Any):
    key = ctx.new_uuid()
    policy = task.RetryPolicy(
        first_retry_interval=timedelta(milliseconds=100), max_number_of_attempts=5)
    result = yield ctx.call_sub_orchestrator(
        "flaky_suborch", input={"key": key, "threshold": 2}, retry_policy=policy)
    return result


# ---------------------------------------------------------------------------
# Failure paths
# ---------------------------------------------------------------------------

@bp.orchestration_trigger(context_name="context")
def activity_fails(ctx: task.OrchestrationContext, _: Any):
    result = yield ctx.call_activity("always_fail", input="boom")
    return result


@bp.orchestration_trigger(context_name="context")
def sub_orch_fails(ctx: task.OrchestrationContext, _: Any):
    result = yield ctx.call_sub_orchestrator("activity_fails")
    return result
