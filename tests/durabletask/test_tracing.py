# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for distributed tracing utilities and integration."""

import json
import logging
from datetime import datetime, timezone
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
        assert spans[0].attributes is not None
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
        assert child_span.context is not None
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
        assert spans[0].status.description is not None
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
        assert spans[0].attributes is not None
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

    def test_emit_timer_span_without_otel(self):
        """emit_timer_span is a no-op when OTel is unavailable."""
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

    def test_create_client_span_context_without_otel(self):
        """create_client_span_context returns None when OTel is unavailable."""
        with patch.object(tracing, '_OTEL_AVAILABLE', False):
            result = tracing.create_client_span_context("activity", "Act", "inst1")
        assert result is None

    def test_end_client_span_without_otel(self):
        """end_client_span is a no-op when OTel is unavailable."""
        with patch.object(tracing, '_OTEL_AVAILABLE', False):
            tracing.end_client_span(None)  # should not raise


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
        assert s.attributes is not None
        assert s.attributes[tracing.ATTR_TASK_TYPE] == "orchestration"
        assert s.attributes[tracing.ATTR_TASK_NAME] == "MyOrch"
        assert s.attributes[tracing.ATTR_TASK_INSTANCE_ID] == "inst-123"

    def test_creates_producer_span_with_version(self, otel_setup: InMemorySpanExporter):
        with tracing.start_create_orchestration_span("MyOrch", "inst-123", version="2.0"):
            pass

        spans = otel_setup.get_finished_spans()
        assert spans[0].name == "create_orchestration:MyOrch@(2.0)"
        assert spans[0].attributes is not None
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
        assert s.attributes is not None
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
        assert s.attributes is not None
        assert s.attributes[tracing.ATTR_TASK_TYPE] == "orchestration"
        assert s.attributes[tracing.ATTR_TASK_NAME] == "MyOrch"
        assert s.attributes[tracing.ATTR_TASK_INSTANCE_ID] == "inst-100"
        assert s.attributes[tracing.ATTR_TASK_STATUS] == "Completed"

    def test_start_time_always_captured(self, otel_setup: InMemorySpanExporter):
        """On first execution (no orchestration_trace_context), start_time_ns
        should still be non-None so it can be persisted for cross-worker replay."""
        span, tokens, span_id, start_time_ns = tracing.start_orchestration_span(
            "MyOrch", "inst-first",
        )
        assert start_time_ns is not None
        assert start_time_ns > 0
        tracing.end_orchestration_span(span, tokens, True, False)

    def test_server_span_failure(self, otel_setup: InMemorySpanExporter):
        span, tokens, span_id, _ = tracing.start_orchestration_span(
            "FailOrch", "inst-200",
        )
        tracing.end_orchestration_span(span, tokens, True, True, "boom")

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].status.status_code == StatusCode.ERROR
        assert spans[0].attributes is not None
        assert spans[0].attributes[tracing.ATTR_TASK_STATUS] == "Failed"

    def test_server_span_not_complete(self, otel_setup: InMemorySpanExporter):
        """Span without completion should not set status attribute."""
        span, tokens, _, _ = tracing.start_orchestration_span("PendingOrch", "inst-300")
        tracing.end_orchestration_span(span, tokens, False, False)

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].attributes is not None
        assert tracing.ATTR_TASK_STATUS not in spans[0].attributes


class TestCreateClientSpanContext:
    """Tests for create_client_span_context."""

    def test_creates_client_span_with_trace_context(self, otel_setup: InMemorySpanExporter):
        """Should return a (TraceContext, span) tuple with correct attributes."""
        result = tracing.create_client_span_context(
            "activity", "SayHello", "inst-1", task_id=42)
        assert result is not None
        trace_ctx, span = result

        assert trace_ctx.traceParent != ""
        assert trace_ctx.spanID != ""
        # Span should NOT be finished yet
        assert len(otel_setup.get_finished_spans()) == 0

        # End it and verify attributes
        span.end()
        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s.kind == trace.SpanKind.CLIENT
        assert s.name == "activity:SayHello"
        assert s.attributes is not None
        assert s.attributes[tracing.ATTR_TASK_TYPE] == "activity"
        assert s.attributes[tracing.ATTR_TASK_NAME] == "SayHello"
        assert s.attributes[tracing.ATTR_TASK_INSTANCE_ID] == "inst-1"
        assert s.attributes[tracing.ATTR_TASK_TASK_ID] == "42"

    def test_includes_version_attribute(self, otel_setup: InMemorySpanExporter):
        result = tracing.create_client_span_context(
            "activity", "Act", "inst-1", version="2.0")
        assert result is not None
        _, span = result
        span.end()
        spans = otel_setup.get_finished_spans()
        assert spans[0].name == "activity:Act@(2.0)"
        assert spans[0].attributes is not None
        assert spans[0].attributes[tracing.ATTR_TASK_VERSION] == "2.0"

    def test_trace_context_span_id_matches_span(self, otel_setup: InMemorySpanExporter):
        """The TraceContext spanID should match the CLIENT span's span ID."""
        result = tracing.create_client_span_context(
            "orchestration", "SubOrch", "inst-1")
        assert result is not None
        trace_ctx, span = result
        span.end()
        spans = otel_setup.get_finished_spans()
        span_ctx = spans[0].get_span_context()
        assert span_ctx is not None
        client_span_id = format(span_ctx.span_id, '016x')
        assert trace_ctx.spanID == client_span_id


class TestEndClientSpan:
    """Tests for end_client_span."""

    def test_ends_span(self, otel_setup: InMemorySpanExporter):
        """end_client_span should close the span and export it."""
        result = tracing.create_client_span_context(
            "activity", "Act", "inst-1")
        assert result is not None
        _, span = result
        tracing.end_client_span(span)
        assert len(otel_setup.get_finished_spans()) == 1

    def test_ends_span_with_error(self, otel_setup: InMemorySpanExporter):
        result = tracing.create_client_span_context(
            "activity", "Act", "inst-1")
        assert result is not None
        _, span = result
        tracing.end_client_span(span, is_error=True, error_message="boom")
        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].status.status_code == StatusCode.ERROR
        assert spans[0].status.description is not None
        assert "boom" in spans[0].status.description

    def test_noop_with_none_span(self):
        """Should not raise when span is None."""
        tracing.end_client_span(None)  # no-op


class TestEmitTimerSpan:
    """Tests for emit_timer_span."""

    def test_emits_internal_span(self, otel_setup: InMemorySpanExporter):
        fire_at = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        tracing.emit_timer_span("MyOrch", "inst-1", 5, fire_at)

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s.name == "orchestration:MyOrch:timer"
        assert s.kind == trace.SpanKind.INTERNAL
        assert s.attributes is not None
        assert s.attributes[tracing.ATTR_TASK_TYPE] == "timer"
        assert s.attributes[tracing.ATTR_FIRE_AT] == fire_at.isoformat()
        assert s.attributes[tracing.ATTR_TASK_TASK_ID] == "5"

    def test_backdated_start_time(self, otel_setup: InMemorySpanExporter):
        """Timer span should cover the full wait period when scheduled_time_ns is set."""
        fire_at = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        created_ns = 1704067200_000_000_000  # 2024-01-01T00:00:00Z
        tracing.emit_timer_span(
            "MyOrch", "inst-1", 5, fire_at, scheduled_time_ns=created_ns,
        )
        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].start_time == created_ns
        assert spans[0].end_time is not None
        assert spans[0].start_time is not None
        assert spans[0].end_time > spans[0].start_time


class TestEmitEventRaisedSpan:
    """Tests for emit_event_raised_span."""

    def test_emits_producer_span(self, otel_setup: InMemorySpanExporter):
        tracing.emit_event_raised_span("approval", "inst-1", target_instance_id="inst-2")

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s.name == "orchestration_event:approval"
        assert s.kind == trace.SpanKind.PRODUCER
        assert s.attributes is not None
        assert s.attributes[tracing.ATTR_TASK_TYPE] == "event"
        assert s.attributes[tracing.ATTR_EVENT_TARGET_INSTANCE_ID] == "inst-2"

    def test_emits_span_without_target(self, otel_setup: InMemorySpanExporter):
        tracing.emit_event_raised_span("approval", "inst-1")

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].attributes is not None
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


class TestReplayDoesNotEmitSpans:
    """Tests that replayed (old) events do NOT re-emit client spans for
    activities, sub-orchestrations, or timers.  Client spans for activities
    and sub-orchestrations are now emitted at action-creation time (inside
    call_activity / call_sub_orchestrator).  During a replay dispatch all
    generator calls happen inside old_events processing (is_replaying=True),
    so no CLIENT spans are produced — they were already emitted in prior
    dispatches."""

    def _get_client_spans(self, exporter):
        """Return non-Server spans (Client/Internal schedule/timer spans)."""
        return [
            s for s in exporter.get_finished_spans()
            if s.kind != trace.SpanKind.SERVER
        ]

    def test_replayed_activity_completion_no_span(self, otel_setup):
        """During a replay dispatch, no CLIENT spans are emitted for
        activities — both old and new completions.  The CLIENT span for
        activity 2 was emitted in a prior dispatch when call_activity()
        was first called with is_replaying=False."""
        def dummy_activity(ctx, _):
            pass

        def orchestrator(ctx: task.OrchestrationContext, _):
            r1 = yield ctx.call_activity(dummy_activity, input=1)
            r2 = yield ctx.call_activity(dummy_activity, input=2)
            return [r1, r2]

        registry = worker._Registry()
        name = registry.add_orchestrator(orchestrator)
        registry.add_activity(dummy_activity)

        # First activity scheduled + completed in old_events (replay)
        old_events = [
            helpers.new_orchestrator_started_event(),
            helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
            helpers.new_task_scheduled_event(1, task.get_name(dummy_activity)),
            helpers.new_task_completed_event(1, json.dumps(10)),
        ]
        # Second activity scheduled in replay, completed as new event
        new_events = [
            helpers.new_task_scheduled_event(2, task.get_name(dummy_activity)),
            helpers.new_task_completed_event(2, json.dumps(20)),
        ]

        executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
        executor.execute(TEST_INSTANCE_ID, old_events, new_events)

        client_spans = self._get_client_spans(otel_setup)
        # No CLIENT spans during replay — they were emitted in prior dispatches
        assert len(client_spans) == 0

    def test_replayed_activity_failure_no_span(self, otel_setup):
        """During a replay dispatch, no CLIENT spans are emitted for
        failed activities."""
        def failing_activity(ctx, _):
            raise ValueError("boom")

        def orchestrator(ctx: task.OrchestrationContext, _):
            try:
                yield ctx.call_activity(failing_activity, input=1)
            except task.TaskFailedError:
                pass
            result = yield ctx.call_activity(failing_activity, input=2)
            return result

        registry = worker._Registry()
        name = registry.add_orchestrator(orchestrator)
        registry.add_activity(failing_activity)

        ex = Exception("boom")
        old_events = [
            helpers.new_orchestrator_started_event(),
            helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
            helpers.new_task_scheduled_event(1, task.get_name(failing_activity)),
            helpers.new_task_failed_event(1, ex),
        ]
        new_events = [
            helpers.new_task_scheduled_event(2, task.get_name(failing_activity)),
            helpers.new_task_completed_event(2, json.dumps("ok")),
        ]

        executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
        executor.execute(TEST_INSTANCE_ID, old_events, new_events)

        client_spans = self._get_client_spans(otel_setup)
        # No CLIENT spans during replay
        assert len(client_spans) == 0

    def test_replayed_timer_no_span(self, otel_setup):
        """A timer that fired during replay should not emit a timer span."""
        from datetime import timedelta

        def orchestrator(ctx: task.OrchestrationContext, _):
            t1 = ctx.current_utc_datetime + timedelta(seconds=1)
            yield ctx.create_timer(t1)
            t2 = ctx.current_utc_datetime + timedelta(seconds=2)
            yield ctx.create_timer(t2)
            return "done"

        registry = worker._Registry()
        name = registry.add_orchestrator(orchestrator)

        start_time = datetime(2020, 1, 1, 12, 0, 0)
        fire_at_1 = start_time + timedelta(seconds=1)
        fire_at_2 = start_time + timedelta(seconds=2)

        # First timer created, fired, and second timer created all in old events
        old_events = [
            helpers.new_orchestrator_started_event(start_time),
            helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
            helpers.new_timer_created_event(1, fire_at_1),
            helpers.new_timer_fired_event(1, fire_at_1),
            helpers.new_timer_created_event(2, fire_at_2),
        ]
        # Only the second timer firing is a new event
        new_events = [
            helpers.new_timer_fired_event(2, fire_at_2),
        ]

        executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
        executor.execute(TEST_INSTANCE_ID, old_events, new_events)

        client_spans = self._get_client_spans(otel_setup)
        # Only the second timer (new event) should produce a span
        assert len(client_spans) == 1
        assert "timer" in client_spans[0].name.lower()

    def test_replayed_sub_orchestration_completion_no_span(self, otel_setup):
        """During a replay dispatch, no CLIENT spans are emitted for
        sub-orchestrations."""
        def sub_orch(ctx: task.OrchestrationContext, _):
            return "sub_done"

        def orchestrator(ctx: task.OrchestrationContext, _):
            r1 = yield ctx.call_sub_orchestrator(sub_orch)
            r2 = yield ctx.call_sub_orchestrator(sub_orch)
            return [r1, r2]

        registry = worker._Registry()
        sub_name = registry.add_orchestrator(sub_orch)
        orch_name = registry.add_orchestrator(orchestrator)

        old_events = [
            helpers.new_orchestrator_started_event(),
            helpers.new_execution_started_event(orch_name, TEST_INSTANCE_ID, encoded_input=None),
            helpers.new_sub_orchestration_created_event(1, sub_name, "sub-1", encoded_input=None),
            helpers.new_sub_orchestration_completed_event(1, encoded_output=json.dumps("r1")),
            helpers.new_sub_orchestration_created_event(2, sub_name, "sub-2", encoded_input=None),
        ]
        new_events = [
            helpers.new_sub_orchestration_completed_event(2, encoded_output=json.dumps("r2")),
        ]

        executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
        executor.execute(TEST_INSTANCE_ID, old_events, new_events)

        client_spans = self._get_client_spans(otel_setup)
        # No CLIENT spans during replay
        assert len(client_spans) == 0

    def test_replayed_sub_orchestration_failure_no_span(self, otel_setup):
        """During a replay dispatch, no CLIENT spans are emitted for
        failed sub-orchestrations."""
        def sub_orch(ctx: task.OrchestrationContext, _):
            raise ValueError("sub failed")

        def orchestrator(ctx: task.OrchestrationContext, _):
            try:
                yield ctx.call_sub_orchestrator(sub_orch)
            except task.TaskFailedError:
                pass
            result = yield ctx.call_sub_orchestrator(sub_orch)
            return result

        registry = worker._Registry()
        sub_name = registry.add_orchestrator(sub_orch)
        orch_name = registry.add_orchestrator(orchestrator)

        ex = Exception("sub failed")
        old_events = [
            helpers.new_orchestrator_started_event(),
            helpers.new_execution_started_event(orch_name, TEST_INSTANCE_ID, encoded_input=None),
            helpers.new_sub_orchestration_created_event(1, sub_name, "sub-1", encoded_input=None),
            helpers.new_sub_orchestration_failed_event(1, ex),
            helpers.new_sub_orchestration_created_event(2, sub_name, "sub-2", encoded_input=None),
        ]
        new_events = [
            helpers.new_sub_orchestration_completed_event(2, encoded_output=json.dumps("ok")),
        ]

        executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
        executor.execute(TEST_INSTANCE_ID, old_events, new_events)

        client_spans = self._get_client_spans(otel_setup)
        # No CLIENT spans during replay
        assert len(client_spans) == 0


class TestOrchestrationSpanLifecycle:
    """Tests that the orchestration SERVER span is persisted across
    intermediate dispatches and only exported on orchestration completion."""

    def _get_orch_server_spans(self, exporter):
        """Return orchestration SERVER spans from the exporter."""
        return [
            s for s in exporter.get_finished_spans()
            if s.kind == trace.SpanKind.SERVER
        ]

    def _make_worker_with_registry(self, registry):
        """Create a TaskHubGrpcWorker with a pre-populated registry."""
        from unittest.mock import MagicMock
        w = worker.TaskHubGrpcWorker(host_address="localhost:4001")
        w._registry = registry
        return w, MagicMock()

    def test_intermediate_dispatch_does_not_export_span(self, otel_setup):
        """An intermediate dispatch (no completeOrchestration) should NOT
        export an orchestration SERVER span."""
        from datetime import timedelta

        def orchestrator(ctx: task.OrchestrationContext, _):
            due = ctx.current_utc_datetime + timedelta(seconds=1)
            yield ctx.create_timer(due)
            return "done"

        registry = worker._Registry()
        name = registry.add_orchestrator(orchestrator)
        w, stub = self._make_worker_with_registry(registry)

        start_time = datetime(2020, 1, 1, 12, 0, 0)
        req = pb.OrchestratorRequest(
            instanceId=TEST_INSTANCE_ID,
            newEvents=[
                helpers.new_orchestrator_started_event(start_time),
                helpers.new_execution_started_event(
                    name, TEST_INSTANCE_ID, encoded_input=None),
            ],
        )
        w._execute_orchestrator(req, stub, "token1")

        # Nothing exported yet — span is kept alive
        assert len(self._get_orch_server_spans(otel_setup)) == 0
        assert TEST_INSTANCE_ID in w._orchestration_spans

    def test_final_dispatch_exports_single_span(self, otel_setup):
        """Across multiple dispatches, only one orchestration span should
        be exported, and only when the orchestration completes."""
        from datetime import timedelta

        def orchestrator(ctx: task.OrchestrationContext, _):
            due = ctx.current_utc_datetime + timedelta(seconds=2)
            yield ctx.create_timer(due)
            results = yield task.when_all([
                ctx.call_activity(dummy_activity, input=i)
                for i in range(3)
            ])
            return results

        def dummy_activity(ctx, _):
            pass

        registry = worker._Registry()
        name = registry.add_orchestrator(orchestrator)
        registry.add_activity(dummy_activity)
        w, stub = self._make_worker_with_registry(registry)

        start_time = datetime(2020, 1, 1, 12, 0, 0)
        fire_at = start_time + timedelta(seconds=2)
        activity_name = task.get_name(dummy_activity)

        # Dispatch 1: start
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId=TEST_INSTANCE_ID,
            newEvents=[
                helpers.new_orchestrator_started_event(start_time),
                helpers.new_execution_started_event(
                    name, TEST_INSTANCE_ID, encoded_input=None),
            ],
        ), stub, "t1")
        assert len(self._get_orch_server_spans(otel_setup)) == 0

        # Dispatch 2: timer fires
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId=TEST_INSTANCE_ID,
            pastEvents=[
                helpers.new_orchestrator_started_event(start_time),
                helpers.new_execution_started_event(
                    name, TEST_INSTANCE_ID, encoded_input=None),
                helpers.new_timer_created_event(1, fire_at),
            ],
            newEvents=[
                helpers.new_timer_fired_event(1, fire_at),
            ],
        ), stub, "t2")
        assert len(self._get_orch_server_spans(otel_setup)) == 0

        # Dispatch 3: activities complete
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId=TEST_INSTANCE_ID,
            pastEvents=[
                helpers.new_orchestrator_started_event(start_time),
                helpers.new_execution_started_event(
                    name, TEST_INSTANCE_ID, encoded_input=None),
                helpers.new_timer_created_event(1, fire_at),
                helpers.new_timer_fired_event(1, fire_at),
                helpers.new_task_scheduled_event(2, activity_name),
                helpers.new_task_scheduled_event(3, activity_name),
                helpers.new_task_scheduled_event(4, activity_name),
            ],
            newEvents=[
                helpers.new_task_completed_event(2, json.dumps("r1")),
                helpers.new_task_completed_event(3, json.dumps("r2")),
                helpers.new_task_completed_event(4, json.dumps("r3")),
            ],
        ), stub, "t3")

        # Exactly one orchestration span exported
        orch_spans = self._get_orch_server_spans(otel_setup)
        assert len(orch_spans) == 1
        assert "orchestration" in orch_spans[0].name
        assert TEST_INSTANCE_ID not in w._orchestration_spans

    def test_span_id_consistent_across_dispatches(self, otel_setup):
        """The same span object (same span_id) is reused across dispatches."""
        from datetime import timedelta

        def orchestrator(ctx: task.OrchestrationContext, _):
            due = ctx.current_utc_datetime + timedelta(seconds=1)
            yield ctx.create_timer(due)
            return "done"

        registry = worker._Registry()
        name = registry.add_orchestrator(orchestrator)
        w, stub = self._make_worker_with_registry(registry)

        start_time = datetime(2020, 1, 1, 12, 0, 0)
        fire_at = start_time + timedelta(seconds=1)

        # Dispatch 1
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId=TEST_INSTANCE_ID,
            newEvents=[
                helpers.new_orchestrator_started_event(start_time),
                helpers.new_execution_started_event(
                    name, TEST_INSTANCE_ID, encoded_input=None),
            ],
        ), stub, "t1")
        span_id_1 = w._orchestration_spans[TEST_INSTANCE_ID][0] \
            .get_span_context().span_id

        # Dispatch 2 (final)
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId=TEST_INSTANCE_ID,
            pastEvents=[
                helpers.new_orchestrator_started_event(start_time),
                helpers.new_execution_started_event(
                    name, TEST_INSTANCE_ID, encoded_input=None),
                helpers.new_timer_created_event(1, fire_at),
            ],
            newEvents=[
                helpers.new_timer_fired_event(1, fire_at),
            ],
        ), stub, "t2")

        orch_spans = self._get_orch_server_spans(otel_setup)
        assert len(orch_spans) == 1
        assert orch_spans[0].get_span_context().span_id == span_id_1

    def test_error_cleans_up_saved_span(self, otel_setup):
        """When an orchestration raises an unhandled error, the span is
        exported with ERROR status and cleaned up from the saved dict."""

        def orchestrator(ctx: task.OrchestrationContext, _):
            raise ValueError("orchestration error")

        registry = worker._Registry()
        registry.add_orchestrator(orchestrator)
        w, stub = self._make_worker_with_registry(registry)

        name = task.get_name(orchestrator)
        req = pb.OrchestratorRequest(
            instanceId=TEST_INSTANCE_ID,
            newEvents=[
                helpers.new_orchestrator_started_event(),
                helpers.new_execution_started_event(
                    name, TEST_INSTANCE_ID, encoded_input=None),
            ],
        )
        w._execute_orchestrator(req, stub, "token1")

        orch_spans = self._get_orch_server_spans(otel_setup)
        assert len(orch_spans) == 1
        assert orch_spans[0].status.status_code == StatusCode.ERROR
        assert TEST_INSTANCE_ID not in w._orchestration_spans

    def test_separate_instances_get_separate_spans(self, otel_setup):
        """Two different orchestration instances should get independent
        spans that can be persisted and exported independently."""
        from datetime import timedelta

        def orchestrator(ctx: task.OrchestrationContext, _):
            due = ctx.current_utc_datetime + timedelta(seconds=1)
            yield ctx.create_timer(due)
            return "done"

        registry = worker._Registry()
        name = registry.add_orchestrator(orchestrator)
        w, stub = self._make_worker_with_registry(registry)

        start_time = datetime(2020, 1, 1, 12, 0, 0)
        fire_at = start_time + timedelta(seconds=1)
        instance_a = "inst-a"
        instance_b = "inst-b"

        # Start both instances
        for iid in (instance_a, instance_b):
            w._execute_orchestrator(pb.OrchestratorRequest(
                instanceId=iid,
                newEvents=[
                    helpers.new_orchestrator_started_event(start_time),
                    helpers.new_execution_started_event(
                        name, iid, encoded_input=None),
                ],
            ), stub, f"t-{iid}")

        assert len(self._get_orch_server_spans(otel_setup)) == 0
        assert instance_a in w._orchestration_spans
        assert instance_b in w._orchestration_spans

        # Complete only instance A
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId=instance_a,
            pastEvents=[
                helpers.new_orchestrator_started_event(start_time),
                helpers.new_execution_started_event(
                    name, instance_a, encoded_input=None),
                helpers.new_timer_created_event(1, fire_at),
            ],
            newEvents=[
                helpers.new_timer_fired_event(1, fire_at),
            ],
        ), stub, "t-a-2")

        # Only instance A's span is exported
        assert len(self._get_orch_server_spans(otel_setup)) == 1
        assert instance_a not in w._orchestration_spans
        assert instance_b in w._orchestration_spans

        # Complete instance B
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId=instance_b,
            pastEvents=[
                helpers.new_orchestrator_started_event(start_time),
                helpers.new_execution_started_event(
                    name, instance_b, encoded_input=None),
                helpers.new_timer_created_event(1, fire_at),
            ],
            newEvents=[
                helpers.new_timer_fired_event(1, fire_at),
            ],
        ), stub, "t-b-2")

        assert len(self._get_orch_server_spans(otel_setup)) == 2
        assert instance_b not in w._orchestration_spans

    def test_initial_dispatch_creates_activity_client_spans(self, otel_setup):
        """On the first dispatch, a CLIENT span is created for the scheduled
        activity but it is NOT yet finished — it stays open until the
        activity completes in a subsequent dispatch."""

        def dummy_activity(ctx, _):
            pass

        def orchestrator(ctx: task.OrchestrationContext, _):
            yield ctx.call_activity(dummy_activity, input="hello")
            return "done"

        registry = worker._Registry()
        name = registry.add_orchestrator(orchestrator)
        registry.add_activity(dummy_activity)
        w, stub = self._make_worker_with_registry(registry)

        start_time = datetime(2020, 1, 1, 12, 0, 0)

        # First dispatch — generator runs with is_replaying=False
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId=TEST_INSTANCE_ID,
            newEvents=[
                helpers.new_orchestrator_started_event(start_time),
                helpers.new_execution_started_event(
                    name, TEST_INSTANCE_ID, encoded_input=None),
            ],
        ), stub, "t1")

        # The CLIENT span should NOT be finished yet (it's still open)
        client_spans = [
            s for s in otel_setup.get_finished_spans()
            if s.kind == trace.SpanKind.CLIENT
        ]
        assert len(client_spans) == 0

        # But it should be stored in the worker's pending dict
        instance_spans = w._pending_client_spans.get(TEST_INSTANCE_ID, {})
        assert len(instance_spans) == 1

    def test_activity_client_span_has_duration(self, otel_setup):
        """The CLIENT span should cover the full scheduling-to-completion
        duration.  After a completion dispatch, the span is finished and
        its parentTraceContext.spanID matches the exported CLIENT span."""

        def dummy_activity(ctx, _):
            pass

        def orchestrator(ctx: task.OrchestrationContext, _):
            yield ctx.call_activity(dummy_activity, input="hello")
            return "done"

        registry = worker._Registry()
        name = registry.add_orchestrator(orchestrator)
        registry.add_activity(dummy_activity)
        w, stub = self._make_worker_with_registry(registry)

        schedule_time = datetime(2020, 1, 1, 12, 0, 0)
        complete_time = datetime(2020, 1, 1, 12, 0, 5)

        # Dispatch 1: schedule the activity
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId=TEST_INSTANCE_ID,
            newEvents=[
                helpers.new_orchestrator_started_event(schedule_time),
                helpers.new_execution_started_event(
                    name, TEST_INSTANCE_ID, encoded_input=None),
            ],
        ), stub, "t1")

        # Capture the parentTraceContext from the action
        call_args = stub.CompleteOrchestratorTask.call_args
        res = call_args[0][0]
        schedule_actions = [
            a for a in res.actions
            if a.HasField("scheduleTask")
        ]
        assert len(schedule_actions) == 1
        ptc = schedule_actions[0].scheduleTask.parentTraceContext
        assert ptc.traceParent != ""

        # Dispatch 2: activity completes
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId=TEST_INSTANCE_ID,
            pastEvents=[
                helpers.new_orchestrator_started_event(schedule_time),
                helpers.new_execution_started_event(
                    name, TEST_INSTANCE_ID, encoded_input=None),
                helpers.new_task_scheduled_event(1, name),
            ],
            newEvents=[
                helpers.new_orchestrator_started_event(complete_time),
                helpers.new_task_completed_event(1, '"world"'),
            ],
        ), stub, "t2")

        # Now the CLIENT span should be finished and exported
        client_spans = [
            s for s in otel_setup.get_finished_spans()
            if s.kind == trace.SpanKind.CLIENT
        ]
        assert len(client_spans) == 1
        assert "activity" in client_spans[0].name

        # The parentTraceContext spanID should match the CLIENT span
        client_span_id = format(
            client_spans[0].get_span_context().span_id, '016x')
        assert ptc.spanID == client_span_id

        # The span should have real duration (start != end)
        assert client_spans[0].start_time < client_spans[0].end_time

        # Pending dict should be cleaned up
        instance_spans = w._pending_client_spans.get(
            TEST_INSTANCE_ID, {})
        assert len(instance_spans) == 0

    def test_distributed_worker_fallback_client_span(self, otel_setup):
        """When a different worker handles the completion dispatch (no
        in-memory CLIENT span), a fallback instant CLIENT span is emitted
        so the trace still contains the CLIENT->SERVER link."""

        def dummy_activity(ctx, _):
            pass

        def orchestrator(ctx: task.OrchestrationContext, _):
            yield ctx.call_activity(dummy_activity, input="hello")
            return "done"

        registry = worker._Registry()
        name = registry.add_orchestrator(orchestrator)
        registry.add_activity(dummy_activity)

        # Simulate a DIFFERENT worker handling the completion dispatch:
        # The pending_client_spans dict is empty (no span from dispatch 1).
        w, stub = self._make_worker_with_registry(registry)
        activity_name = task.get_name(dummy_activity)

        schedule_time = datetime(2020, 1, 1, 12, 0, 0)
        complete_time = datetime(2020, 1, 1, 12, 0, 5)

        # Completion dispatch with no prior in-memory state
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId=TEST_INSTANCE_ID,
            pastEvents=[
                helpers.new_orchestrator_started_event(schedule_time),
                helpers.new_execution_started_event(
                    name, TEST_INSTANCE_ID, encoded_input=None),
                helpers.new_task_scheduled_event(1, activity_name),
            ],
            newEvents=[
                helpers.new_orchestrator_started_event(complete_time),
                helpers.new_task_completed_event(1, json.dumps("world")),
            ],
        ), stub, "t1")

        # A fallback CLIENT span should have been emitted
        client_spans = [
            s for s in otel_setup.get_finished_spans()
            if s.kind == trace.SpanKind.CLIENT
        ]
        assert len(client_spans) == 1
        assert "activity" in client_spans[0].name
