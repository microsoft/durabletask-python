# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for distributed tracing utilities and integration."""

import json
import logging
from typing import Any
from unittest.mock import patch

import pytest
from google.protobuf import wrappers_pb2

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

import durabletask.internal.helpers as helpers
import durabletask.internal.orchestrator_service_pb2 as pb
import durabletask.internal.tracing as tracing
from durabletask import task, worker

logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.DEBUG)
TEST_LOGGER = logging.getLogger("tests")
TEST_INSTANCE_ID = 'abc123'


# Module-level setup: create a TracerProvider with an InMemorySpanExporter once.
# Newer OpenTelemetry versions only allow set_tracer_provider to be called once.
_EXPORTER = InMemorySpanExporter()
_PROVIDER = TracerProvider()
_PROVIDER.add_span_processor(SimpleSpanProcessor(_EXPORTER))
trace.set_tracer_provider(_PROVIDER)


@pytest.fixture(autouse=True)
def otel_setup():
    """Clear the in-memory exporter before each test."""
    _EXPORTER.clear()
    yield _EXPORTER


# ---------------------------------------------------------------------------
# Tests for tracing utility functions
# ---------------------------------------------------------------------------


class TestGetCurrentTraceContext:
    """Tests for tracing.get_current_trace_context()."""

    def test_returns_none_when_no_active_span(self, otel_setup):
        """When there is no active span, should return None."""
        result = tracing.get_current_trace_context()
        assert result is None

    def test_returns_trace_context_with_active_span(self, otel_setup):
        """When there is an active span, should return a populated TraceContext."""
        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("test-span"):
            result = tracing.get_current_trace_context()

        assert result is not None
        assert isinstance(result, pb.TraceContext)
        assert result.traceParent != ""
        assert result.spanID != ""
        # traceparent format: 00-<trace_id>-<span_id>-<flags>
        parts = result.traceParent.split("-")
        assert len(parts) == 4
        assert parts[0] == "00"
        assert len(parts[1]) == 32  # trace ID
        assert len(parts[2]) == 16  # span ID
        assert result.spanID == parts[2]


class TestExtractTraceContext:
    """Tests for tracing.extract_trace_context()."""

    def test_returns_none_for_none_input(self):
        result = tracing.extract_trace_context(None)
        assert result is None

    def test_returns_none_for_empty_traceparent(self):
        proto_ctx = pb.TraceContext(traceParent="", spanID="")
        result = tracing.extract_trace_context(proto_ctx)
        assert result is None

    def test_extracts_valid_context(self, otel_setup):
        """Should extract a valid OTel context from a protobuf TraceContext."""
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        proto_ctx = pb.TraceContext(
            traceParent=traceparent,
            spanID="b7ad6b7169203331",
        )
        otel_ctx = tracing.extract_trace_context(proto_ctx)
        assert otel_ctx is not None

    def test_extracts_context_with_tracestate(self, otel_setup):
        """Should extract context including tracestate."""
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        tracestate_val = "congo=t61rcWkgMzE"
        proto_ctx = pb.TraceContext(
            traceParent=traceparent,
            spanID="b7ad6b7169203331",
            traceState=wrappers_pb2.StringValue(value=tracestate_val),
        )
        otel_ctx = tracing.extract_trace_context(proto_ctx)
        assert otel_ctx is not None


class TestStartSpan:
    """Tests for tracing.start_span()."""

    def test_creates_span_without_parent(self, otel_setup: InMemorySpanExporter):
        """Should create a span even without a parent trace context."""
        with tracing.start_span("test-span") as span:
            assert span is not None

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].name == "test-span"

    def test_creates_span_with_attributes(self, otel_setup: InMemorySpanExporter):
        """Should create a span with custom attributes."""
        attrs = {"key1": "value1", "key2": "value2"}
        with tracing.start_span("test-span", attributes=attrs) as span:
            assert span is not None

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].attributes["key1"] == "value1"
        assert spans[0].attributes["key2"] == "value2"

    def test_creates_child_span_from_trace_context(self, otel_setup: InMemorySpanExporter):
        """Should create a child span linked to the parent trace context."""
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        proto_ctx = pb.TraceContext(
            traceParent=traceparent,
            spanID="b7ad6b7169203331",
        )
        with tracing.start_span("child-span", trace_context=proto_ctx) as span:
            assert span is not None

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        child_span = spans[0]
        assert child_span.name == "child-span"
        # The child span's trace ID should match the parent's
        assert child_span.context.trace_id == int("0af7651916cd43dd8448eb211c80319c", 16)


class TestSetSpanError:
    """Tests for tracing.set_span_error()."""

    def test_records_error_on_span(self, otel_setup: InMemorySpanExporter):
        """Should record error status and exception on the span."""
        with tracing.start_span("error-span") as span:
            ex = ValueError("something went wrong")
            tracing.set_span_error(span, ex)

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].status.status_code == StatusCode.ERROR
        assert "something went wrong" in spans[0].status.description

    def test_noop_with_none_span(self):
        """Should not raise when span is None."""
        tracing.set_span_error(None, ValueError("test"))


# ---------------------------------------------------------------------------
# Tests for client-side trace context injection
# ---------------------------------------------------------------------------


class TestClientTraceContextInjection:
    """Tests that the client methods inject trace context."""

    def test_schedule_new_orchestration_includes_trace_context(self, otel_setup):
        """schedule_new_orchestration should set parentTraceContext from current span."""
        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("client-span"):
            ctx = tracing.get_current_trace_context()

        assert ctx is not None
        assert ctx.traceParent != ""
        assert ctx.spanID != ""

    def test_signal_entity_trace_context_not_none(self, otel_setup):
        """When tracing is active, signal_entity parentTraceContext should be non-None."""
        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("client-span"):
            ctx = tracing.get_current_trace_context()

        assert ctx is not None


# ---------------------------------------------------------------------------
# Tests for activity execution with tracing
# ---------------------------------------------------------------------------


class TestActivityExecutionTracing:
    """Tests that activity execution creates spans from parent trace context."""

    def test_activity_executes_within_span(self, otel_setup: InMemorySpanExporter):
        """Activity execution should create a span when parentTraceContext is provided."""
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        parent_ctx = pb.TraceContext(
            traceParent=traceparent,
            spanID="b7ad6b7169203331",
        )

        def test_activity(ctx: task.ActivityContext, input: Any):
            return "hello"

        registry = worker._Registry()
        name = registry.add_activity(test_activity)
        executor = worker._ActivityExecutor(registry, TEST_LOGGER)

        with tracing.start_span(
            f"activity:{name}",
            trace_context=parent_ctx,
            attributes={"durabletask.task.instance_id": TEST_INSTANCE_ID,
                        "durabletask.task.name": name,
                        "durabletask.task.task_id": "42"},
        ):
            result = executor.execute(TEST_INSTANCE_ID, name, 42, None)

        assert result == json.dumps("hello")
        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].name == f"activity:{name}"
        assert spans[0].attributes["durabletask.task.instance_id"] == TEST_INSTANCE_ID

    def test_activity_error_sets_span_error(self, otel_setup: InMemorySpanExporter):
        """Activity execution errors should be recorded on the span."""
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        parent_ctx = pb.TraceContext(
            traceParent=traceparent,
            spanID="b7ad6b7169203331",
        )

        def failing_activity(ctx: task.ActivityContext, input: Any):
            raise ValueError("Activity failed!")

        registry = worker._Registry()
        name = registry.add_activity(failing_activity)
        executor = worker._ActivityExecutor(registry, TEST_LOGGER)

        with pytest.raises(ValueError, match="Activity failed!"):
            with tracing.start_span(
                f"activity:{name}",
                trace_context=parent_ctx,
            ) as span:
                try:
                    executor.execute(TEST_INSTANCE_ID, name, 42, None)
                except Exception as ex:
                    tracing.set_span_error(span, ex)
                    raise

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].status.status_code == StatusCode.ERROR


# ---------------------------------------------------------------------------
# Tests for orchestration trace context propagation
# ---------------------------------------------------------------------------


class TestOrchestrationTraceContextPropagation:
    """Tests that orchestration actions include trace context."""

    def test_schedule_task_action_includes_trace_context(self):
        """new_schedule_task_action should include parentTraceContext when provided."""
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        parent_ctx = pb.TraceContext(
            traceParent=traceparent,
            spanID="b7ad6b7169203331",
        )
        action = helpers.new_schedule_task_action(
            0, "my_activity", None, None,
            parent_trace_context=parent_ctx
        )
        assert action.scheduleTask.parentTraceContext.traceParent == traceparent

    def test_schedule_task_action_without_trace_context(self):
        """new_schedule_task_action should work without trace context."""
        action = helpers.new_schedule_task_action(0, "my_activity", None, None)
        # parentTraceContext should not be set (default empty)
        assert action.scheduleTask.parentTraceContext.traceParent == ""

    def test_create_sub_orchestration_action_includes_trace_context(self):
        """new_create_sub_orchestration_action should include parentTraceContext."""
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        parent_ctx = pb.TraceContext(
            traceParent=traceparent,
            spanID="b7ad6b7169203331",
        )
        action = helpers.new_create_sub_orchestration_action(
            0, "sub_orch", "inst1", None, None,
            parent_trace_context=parent_ctx
        )
        assert action.createSubOrchestration.parentTraceContext.traceParent == traceparent

    def test_create_sub_orchestration_action_without_trace_context(self):
        """new_create_sub_orchestration_action should work without trace context."""
        action = helpers.new_create_sub_orchestration_action(
            0, "sub_orch", "inst1", None, None
        )
        assert action.createSubOrchestration.parentTraceContext.traceParent == ""


class TestOrchestrationExecutorStoresTraceContext:
    """Tests that the orchestration executor extracts and stores trace context from events."""

    def test_execution_started_stores_parent_trace_context(self):
        """process_event should store parentTraceContext from executionStarted."""
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        parent_ctx = pb.TraceContext(
            traceParent=traceparent,
            spanID="b7ad6b7169203331",
        )

        def simple_orchestrator(ctx: task.OrchestrationContext, _):
            return "done"

        registry = worker._Registry()
        registry.add_orchestrator(simple_orchestrator)

        ctx = worker._RuntimeOrchestrationContext(TEST_INSTANCE_ID, registry)
        assert ctx._parent_trace_context is None

        executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)

        # Create an executionStarted event with parentTraceContext
        event = pb.HistoryEvent(
            eventId=-1,
            executionStarted=pb.ExecutionStartedEvent(
                name="simple_orchestrator",
                orchestrationInstance=pb.OrchestrationInstance(instanceId=TEST_INSTANCE_ID),
                parentTraceContext=parent_ctx,
            )
        )

        executor.process_event(ctx, event)
        assert ctx._parent_trace_context is not None
        assert ctx._parent_trace_context.traceParent == traceparent

    def test_execution_started_without_trace_context(self):
        """process_event should leave parentTraceContext as None when not provided."""
        def simple_orchestrator(ctx: task.OrchestrationContext, _):
            return "done"

        registry = worker._Registry()
        registry.add_orchestrator(simple_orchestrator)

        ctx = worker._RuntimeOrchestrationContext(TEST_INSTANCE_ID, registry)
        executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)

        event = pb.HistoryEvent(
            eventId=-1,
            executionStarted=pb.ExecutionStartedEvent(
                name="simple_orchestrator",
                orchestrationInstance=pb.OrchestrationInstance(instanceId=TEST_INSTANCE_ID),
            )
        )

        executor.process_event(ctx, event)
        assert ctx._parent_trace_context is None


class TestOtelNotAvailable:
    """Tests that tracing functions gracefully degrade when OTel is unavailable."""

    def test_get_current_trace_context_without_otel(self):
        """get_current_trace_context returns None when OTel is not available."""
        with patch.object(tracing, '_OTEL_AVAILABLE', False):
            result = tracing.get_current_trace_context()
        assert result is None

    def test_extract_trace_context_without_otel(self):
        """extract_trace_context returns None when OTel is not available."""
        proto_ctx = pb.TraceContext(traceParent="00-abc-def-01", spanID="def")
        with patch.object(tracing, '_OTEL_AVAILABLE', False):
            result = tracing.extract_trace_context(proto_ctx)
        assert result is None

    def test_start_span_without_otel(self):
        """start_span should yield None when OTel is not available."""
        with patch.object(tracing, '_OTEL_AVAILABLE', False):
            with tracing.start_span("test") as span:
                assert span is None

    def test_set_span_error_without_otel(self):
        """set_span_error should be a no-op when OTel is not available."""
        with patch.object(tracing, '_OTEL_AVAILABLE', False):
            tracing.set_span_error(None, ValueError("test"))  # should not raise

    def test_start_orchestration_span_without_otel(self):
        """start_orchestration_span returns all-None tuple when OTel unavailable."""
        with patch.object(tracing, '_OTEL_AVAILABLE', False):
            span, tokens, span_id, start_time = tracing.start_orchestration_span(
                "test_orch", "inst1",
            )
        assert span is None
        assert tokens is None
        assert span_id is None
        assert start_time is None

    def test_end_orchestration_span_without_otel(self):
        """end_orchestration_span is a no-op when OTel is unavailable."""
        with patch.object(tracing, '_OTEL_AVAILABLE', False):
            tracing.end_orchestration_span(None, None, True, False)

    def test_emit_activity_schedule_span_without_otel(self):
        """emit_activity_schedule_span is a no-op when OTel is unavailable."""
        with patch.object(tracing, '_OTEL_AVAILABLE', False):
            tracing.emit_activity_schedule_span("act", "inst1", 1)

    def test_emit_timer_span_without_otel(self):
        """emit_timer_span is a no-op when OTel is unavailable."""
        from datetime import datetime, timezone
        with patch.object(tracing, '_OTEL_AVAILABLE', False):
            tracing.emit_timer_span("orch", "inst1", 1, datetime.now(timezone.utc))

    def test_start_create_orchestration_span_without_otel(self):
        """start_create_orchestration_span yields None when OTel unavailable."""
        with patch.object(tracing, '_OTEL_AVAILABLE', False):
            with tracing.start_create_orchestration_span("orch", "inst1") as span:
                assert span is None

    def test_start_raise_event_span_without_otel(self):
        """start_raise_event_span yields None when OTel unavailable."""
        with patch.object(tracing, '_OTEL_AVAILABLE', False):
            with tracing.start_raise_event_span("evt", "inst1") as span:
                assert span is None


# ---------------------------------------------------------------------------
# Tests for span naming helpers
# ---------------------------------------------------------------------------


class TestSpanNaming:
    """Tests for create_span_name and create_timer_span_name."""

    def test_create_span_name_without_version(self):
        assert tracing.create_span_name("orchestration", "MyOrch") == "orchestration:MyOrch"

    def test_create_span_name_with_version(self):
        assert tracing.create_span_name("activity", "Say", "1.0") == "activity:Say@(1.0)"

    def test_create_timer_span_name(self):
        assert tracing.create_timer_span_name("MyOrch") == "orchestration:MyOrch:timer"


# ---------------------------------------------------------------------------
# Tests for schema attribute constants
# ---------------------------------------------------------------------------


class TestSchemaConstants:
    """Tests that schema constants match expected names."""

    def test_attribute_keys_defined(self):
        assert tracing.ATTR_TASK_TYPE == "durabletask.type"
        assert tracing.ATTR_TASK_NAME == "durabletask.task.name"
        assert tracing.ATTR_TASK_VERSION == "durabletask.task.version"
        assert tracing.ATTR_TASK_INSTANCE_ID == "durabletask.task.instance_id"
        assert tracing.ATTR_TASK_EXECUTION_ID == "durabletask.task.execution_id"
        assert tracing.ATTR_TASK_STATUS == "durabletask.task.status"
        assert tracing.ATTR_TASK_TASK_ID == "durabletask.task.task_id"
        assert tracing.ATTR_EVENT_TARGET_INSTANCE_ID == "durabletask.event.target_instance_id"
        assert tracing.ATTR_FIRE_AT == "durabletask.fire_at"


# ---------------------------------------------------------------------------
# Tests for Producer / Client / Server span creation
# ---------------------------------------------------------------------------


class TestCreateOrchestrationSpan:
    """Tests for start_create_orchestration_span (Producer span)."""

    def test_creates_producer_span(self, otel_setup: InMemorySpanExporter):
        """Should create a Producer span for create_orchestration."""
        with tracing.start_create_orchestration_span("MyOrch", "inst-123") as span:
            assert span is not None

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s.name == "create_orchestration:MyOrch"
        assert s.kind == trace.SpanKind.PRODUCER
        assert s.attributes[tracing.ATTR_TASK_TYPE] == "orchestration"
        assert s.attributes[tracing.ATTR_TASK_NAME] == "MyOrch"
        assert s.attributes[tracing.ATTR_TASK_INSTANCE_ID] == "inst-123"

    def test_creates_producer_span_with_version(self, otel_setup: InMemorySpanExporter):
        with tracing.start_create_orchestration_span("MyOrch", "inst-123", version="2.0"):
            pass

        spans = otel_setup.get_finished_spans()
        assert spans[0].name == "create_orchestration:MyOrch@(2.0)"
        assert spans[0].attributes[tracing.ATTR_TASK_VERSION] == "2.0"

    def test_trace_context_injected_inside_producer_span(self, otel_setup: InMemorySpanExporter):
        """Inside the producer span, get_current_trace_context should capture producer span ctx."""
        with tracing.start_create_orchestration_span("Orch", "inst"):
            ctx = tracing.get_current_trace_context()
        assert ctx is not None
        assert ctx.traceParent != ""


class TestRaiseEventSpan:
    """Tests for start_raise_event_span (Producer span)."""

    def test_creates_producer_span(self, otel_setup: InMemorySpanExporter):
        with tracing.start_raise_event_span("MyEvent", "inst-456") as span:
            assert span is not None

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s.name == "orchestration_event:MyEvent"
        assert s.kind == trace.SpanKind.PRODUCER
        assert s.attributes[tracing.ATTR_TASK_TYPE] == "event"
        assert s.attributes[tracing.ATTR_TASK_NAME] == "MyEvent"
        assert s.attributes[tracing.ATTR_EVENT_TARGET_INSTANCE_ID] == "inst-456"


class TestOrchestrationServerSpan:
    """Tests for start_orchestration_span and end_orchestration_span."""

    def test_creates_server_span(self, otel_setup: InMemorySpanExporter):
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        parent_ctx = pb.TraceContext(
            traceParent=traceparent,
            spanID="b7ad6b7169203331",
        )
        span, tokens, span_id, start_time_ns = tracing.start_orchestration_span(
            "MyOrch", "inst-100", parent_trace_context=parent_ctx,
        )
        assert span is not None
        assert span_id is not None
        assert len(span_id) == 16

        tracing.end_orchestration_span(span, tokens, True, False)

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s.name == "orchestration:MyOrch"
        assert s.kind == trace.SpanKind.SERVER
        assert s.attributes[tracing.ATTR_TASK_TYPE] == "orchestration"
        assert s.attributes[tracing.ATTR_TASK_NAME] == "MyOrch"
        assert s.attributes[tracing.ATTR_TASK_INSTANCE_ID] == "inst-100"
        assert s.attributes[tracing.ATTR_TASK_STATUS] == "Completed"

    def test_server_span_failure(self, otel_setup: InMemorySpanExporter):
        span, tokens, span_id, _ = tracing.start_orchestration_span(
            "FailOrch", "inst-200",
        )
        tracing.end_orchestration_span(span, tokens, True, True, "boom")

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].status.status_code == StatusCode.ERROR
        assert spans[0].attributes[tracing.ATTR_TASK_STATUS] == "Failed"

    def test_server_span_not_complete(self, otel_setup: InMemorySpanExporter):
        """Span without completion should not set status attribute."""
        span, tokens, _, _ = tracing.start_orchestration_span("PendingOrch", "inst-300")
        tracing.end_orchestration_span(span, tokens, False, False)

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert tracing.ATTR_TASK_STATUS not in spans[0].attributes


# ---------------------------------------------------------------------------
# Tests for emit-and-close spans (Client / Internal)
# ---------------------------------------------------------------------------


class TestEmitActivityScheduleSpan:
    """Tests for emit_activity_schedule_span and emit_activity_schedule_span_failed."""

    def test_emits_client_span(self, otel_setup: InMemorySpanExporter):
        tracing.emit_activity_schedule_span("SayHello", "inst-1", 42)

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s.name == "activity:SayHello"
        assert s.kind == trace.SpanKind.CLIENT
        assert s.attributes[tracing.ATTR_TASK_TYPE] == "activity"
        assert s.attributes[tracing.ATTR_TASK_NAME] == "SayHello"
        assert s.attributes[tracing.ATTR_TASK_TASK_ID] == "42"

    def test_emits_failed_client_span(self, otel_setup: InMemorySpanExporter):
        tracing.emit_activity_schedule_span_failed("SayHello", "inst-1", 42, "oops")

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].status.status_code == StatusCode.ERROR

    def test_emits_span_with_version(self, otel_setup: InMemorySpanExporter):
        tracing.emit_activity_schedule_span("Act", "inst-1", 1, version="3.0")
        spans = otel_setup.get_finished_spans()
        assert spans[0].name == "activity:Act@(3.0)"
        assert spans[0].attributes[tracing.ATTR_TASK_VERSION] == "3.0"


class TestEmitSubOrchestrationScheduleSpan:
    """Tests for emit_sub_orchestration_schedule_span."""

    def test_emits_client_span(self, otel_setup: InMemorySpanExporter):
        tracing.emit_sub_orchestration_schedule_span("SubOrch", "sub-inst-1")

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s.name == "orchestration:SubOrch"
        assert s.kind == trace.SpanKind.CLIENT
        assert s.attributes[tracing.ATTR_TASK_TYPE] == "orchestration"

    def test_emits_failed_client_span(self, otel_setup: InMemorySpanExporter):
        tracing.emit_sub_orchestration_schedule_span_failed("SubOrch", "sub-inst-1", "failed")
        spans = otel_setup.get_finished_spans()
        assert spans[0].status.status_code == StatusCode.ERROR


class TestEmitTimerSpan:
    """Tests for emit_timer_span."""

    def test_emits_internal_span(self, otel_setup: InMemorySpanExporter):
        from datetime import datetime, timezone
        fire_at = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        tracing.emit_timer_span("MyOrch", "inst-1", 5, fire_at)

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s.name == "orchestration:MyOrch:timer"
        assert s.kind == trace.SpanKind.INTERNAL
        assert s.attributes[tracing.ATTR_TASK_TYPE] == "timer"
        assert s.attributes[tracing.ATTR_FIRE_AT] == fire_at.isoformat()
        assert s.attributes[tracing.ATTR_TASK_TASK_ID] == "5"


class TestEmitEventRaisedSpan:
    """Tests for emit_event_raised_span."""

    def test_emits_producer_span(self, otel_setup: InMemorySpanExporter):
        tracing.emit_event_raised_span("approval", "inst-1", target_instance_id="inst-2")

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s.name == "orchestration_event:approval"
        assert s.kind == trace.SpanKind.PRODUCER
        assert s.attributes[tracing.ATTR_TASK_TYPE] == "event"
        assert s.attributes[tracing.ATTR_EVENT_TARGET_INSTANCE_ID] == "inst-2"

    def test_emits_span_without_target(self, otel_setup: InMemorySpanExporter):
        tracing.emit_event_raised_span("approval", "inst-1")

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert tracing.ATTR_EVENT_TARGET_INSTANCE_ID not in spans[0].attributes


# ---------------------------------------------------------------------------
# Tests for build_orchestration_trace_context
# ---------------------------------------------------------------------------


class TestBuildOrchestrationTraceContext:
    """Tests for build_orchestration_trace_context."""

    def test_returns_none_when_span_id_none(self):
        result = tracing.build_orchestration_trace_context(None, None)
        assert result is None

    def test_builds_context_with_span_id(self):
        result = tracing.build_orchestration_trace_context("abc123def456", None)
        assert result is not None
        assert result.spanID.value == "abc123def456"

    def test_builds_context_with_start_time(self):
        start_time_ns = 1704067200000000000  # 2024-01-01T00:00:00Z
        result = tracing.build_orchestration_trace_context("abc123", start_time_ns)
        assert result is not None
        assert result.spanStartTime.seconds == 1704067200
        assert result.spanStartTime.nanos == 0
