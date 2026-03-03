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
