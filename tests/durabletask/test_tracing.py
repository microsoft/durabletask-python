# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for distributed tracing utilities and integration."""

import json
import logging
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from google.protobuf import timestamp_pb2, wrappers_pb2

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
# Shared test constants and helpers
# ---------------------------------------------------------------------------

_SAMPLE_TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
_SAMPLE_PARENT_SPAN_ID = "b7ad6b7169203331"
_SAMPLE_CLIENT_SPAN_ID = "00f067aa0ba902b7"


def _make_parent_trace_ctx():
    return pb.TraceContext(
        traceParent=f"00-{_SAMPLE_TRACE_ID}-{_SAMPLE_PARENT_SPAN_ID}-01",
        spanID=_SAMPLE_PARENT_SPAN_ID,
    )


def _make_client_trace_ctx():
    return pb.TraceContext(
        traceParent=f"00-{_SAMPLE_TRACE_ID}-{_SAMPLE_CLIENT_SPAN_ID}-01",
        spanID=_SAMPLE_CLIENT_SPAN_ID,
    )


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
        assert ctx._orchestration_trace_context is None

    def test_execution_started_generates_orchestration_trace_context(self):
        """process_event should generate _orchestration_trace_context
        from the parent trace context for use as child span parent."""
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
        assert ctx._orchestration_trace_context is None

        executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
        event = pb.HistoryEvent(
            eventId=-1,
            executionStarted=pb.ExecutionStartedEvent(
                name="simple_orchestrator",
                orchestrationInstance=pb.OrchestrationInstance(instanceId=TEST_INSTANCE_ID),
                parentTraceContext=parent_ctx,
            )
        )

        executor.process_event(ctx, event)
        orch_ctx = ctx._orchestration_trace_context
        assert orch_ctx is not None
        # The orchestration context should have the same trace ID
        # but a different span ID (pre-determined for the SERVER span)
        parts = orch_ctx.traceParent.split("-")
        assert parts[1] == "0af7651916cd43dd8448eb211c80319c"
        assert parts[2] != "b7ad6b7169203331"
        assert orch_ctx.spanID == parts[2]

    def test_execution_started_reuses_persisted_span_id(self):
        """When a persisted orchestration span ID exists from a prior dispatch,
        process_event should reuse that span ID instead of generating a new one."""
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        parent_ctx = pb.TraceContext(
            traceParent=traceparent,
            spanID="b7ad6b7169203331",
        )
        persisted_span_id = "00f067aa0ba902b7"

        def simple_orchestrator(ctx: task.OrchestrationContext, _):
            return "done"

        registry = worker._Registry()
        registry.add_orchestrator(simple_orchestrator)

        ctx = worker._RuntimeOrchestrationContext(TEST_INSTANCE_ID, registry)
        executor = worker._OrchestrationExecutor(
            registry, TEST_LOGGER,
            persisted_orch_span_id=persisted_span_id,
        )

        event = pb.HistoryEvent(
            eventId=-1,
            executionStarted=pb.ExecutionStartedEvent(
                name="simple_orchestrator",
                orchestrationInstance=pb.OrchestrationInstance(instanceId=TEST_INSTANCE_ID),
                parentTraceContext=parent_ctx,
            )
        )

        executor.process_event(ctx, event)
        orch_ctx = ctx._orchestration_trace_context
        assert orch_ctx is not None
        # Should reuse the persisted span ID
        parts = orch_ctx.traceParent.split("-")
        assert parts[1] == "0af7651916cd43dd8448eb211c80319c"  # same trace ID
        assert parts[2] == persisted_span_id  # reused span ID
        assert orch_ctx.spanID == persisted_span_id


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

    def test_emit_orchestration_span_without_otel(self):
        """emit_orchestration_span is a no-op when OTel is unavailable."""
        with patch.object(tracing, '_OTEL_AVAILABLE', False):
            tracing.emit_orchestration_span(
                "test_orch", "inst1", 1000, False)

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

    def test_emit_event_raised_span_without_otel(self):
        """emit_event_raised_span is a no-op when OTel is unavailable."""
        with patch.object(tracing, '_OTEL_AVAILABLE', False):
            tracing.emit_event_raised_span("evt", "inst1")


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
    """Tests for emit_orchestration_span."""

    def test_emits_server_span(self, otel_setup: InMemorySpanExporter):
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        parent_ctx = pb.TraceContext(
            traceParent=traceparent,
            spanID="b7ad6b7169203331",
        )
        import time
        start_ns = time.time_ns()
        tracing.emit_orchestration_span(
            "MyOrch", "inst-100", start_ns, False,
            parent_trace_context=parent_ctx,
        )

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

    def test_server_span_failure(self, otel_setup: InMemorySpanExporter):
        import time
        tracing.emit_orchestration_span(
            "FailOrch", "inst-200", time.time_ns(), True, "boom",
        )

        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].status.status_code == StatusCode.ERROR
        assert spans[0].attributes is not None
        assert spans[0].attributes[tracing.ATTR_TASK_STATUS] == "Failed"

    def test_server_span_backdated(self, otel_setup: InMemorySpanExporter):
        """The span start time should honour the provided start_time_ns."""
        start_ns = 1704067200000000000  # 2024-01-01T00:00:00Z
        tracing.emit_orchestration_span(
            "BackdatedOrch", "inst-300", start_ns, False,
        )
        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].start_time == start_ns

    def test_deferred_span_uses_predetermined_span_id(self, otel_setup: InMemorySpanExporter):
        """When orchestration_trace_context is provided, the SERVER span
        should use the pre-determined span ID from that context."""
        parent_tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        parent_ctx = pb.TraceContext(
            traceParent=parent_tp, spanID="b7ad6b7169203331",
        )
        orch_span_id = "00f067aa0ba902b7"
        orch_tp = f"00-0af7651916cd43dd8448eb211c80319c-{orch_span_id}-01"
        orch_ctx = pb.TraceContext(
            traceParent=orch_tp, spanID=orch_span_id,
        )
        import time
        start_ns = time.time_ns()
        tracing.emit_orchestration_span(
            "DeferredOrch", "inst-400", start_ns, False,
            parent_trace_context=parent_ctx,
            orchestration_trace_context=orch_ctx,
        )
        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s.name == "orchestration:DeferredOrch"
        assert s.kind == trace.SpanKind.SERVER
        # Span ID should match the pre-determined orchestration context
        assert s.context is not None
        assert s.context.span_id == int(orch_span_id, 16)
        # Parent should be the PRODUCER span
        assert s.parent is not None
        assert s.parent.span_id == int("b7ad6b7169203331", 16)

    def test_deferred_span_with_failure(self, otel_setup: InMemorySpanExporter):
        """When orchestration_trace_context is provided and is_failed=True,
        the deferred span should have ERROR status."""
        parent_tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        parent_ctx = pb.TraceContext(
            traceParent=parent_tp, spanID="b7ad6b7169203331",
        )
        orch_span_id = "00f067aa0ba902b7"
        orch_tp = f"00-0af7651916cd43dd8448eb211c80319c-{orch_span_id}-01"
        orch_ctx = pb.TraceContext(
            traceParent=orch_tp, spanID=orch_span_id,
        )
        import time
        tracing.emit_orchestration_span(
            "FailOrch", "inst-500", time.time_ns(), True, "kaboom",
            parent_trace_context=parent_ctx,
            orchestration_trace_context=orch_ctx,
        )
        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s.status.status_code == StatusCode.ERROR
        assert s.attributes is not None
        assert s.attributes[tracing.ATTR_TASK_STATUS] == "Failed"
        assert s.context is not None
        assert s.context.span_id == int(orch_span_id, 16)


# ---------------------------------------------------------------------------
# Tests for emit_timer_span
# ---------------------------------------------------------------------------


class TestTimerSpan:
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

    def test_parent_trace_context(self, otel_setup: InMemorySpanExporter):
        """Timer span should be parented under the given trace context."""
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        parent_ctx = pb.TraceContext(
            traceParent=traceparent,
            spanID="b7ad6b7169203331",
        )
        fire_at = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        tracing.emit_timer_span(
            "MyOrch", "inst-1", 5, fire_at,
            parent_trace_context=parent_ctx,
        )
        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        # The span should share the same trace ID as the parent
        expected_trace_id = int("0af7651916cd43dd8448eb211c80319c", 16)
        assert s.context is not None
        assert s.context.trace_id == expected_trace_id
        # The parent span ID should match the parent context
        expected_parent_span_id = int("b7ad6b7169203331", 16)
        assert s.parent is not None
        assert s.parent.span_id == expected_parent_span_id


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

    def test_parent_trace_context(self, otel_setup: InMemorySpanExporter):
        """Event span should be parented under the given trace context."""
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        parent_ctx = pb.TraceContext(
            traceParent=traceparent,
            spanID="b7ad6b7169203331",
        )
        tracing.emit_event_raised_span(
            "approval", "inst-1",
            parent_trace_context=parent_ctx,
        )
        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        expected_trace_id = int("0af7651916cd43dd8448eb211c80319c", 16)
        assert s.context is not None
        assert s.context.trace_id == expected_trace_id
        expected_parent_span_id = int("b7ad6b7169203331", 16)
        assert s.parent is not None
        assert s.parent.span_id == expected_parent_span_id


# ---------------------------------------------------------------------------
# Tests for build_orchestration_trace_context
# ---------------------------------------------------------------------------


class TestReconstructTraceContext:
    """Tests for tracing.reconstruct_trace_context."""

    def test_returns_context_with_given_span_id(self):
        parent = pb.TraceContext(
            traceParent="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            spanID="b7ad6b7169203331",
        )
        result = tracing.reconstruct_trace_context(parent, "00f067aa0ba902b7")
        assert result is not None
        parts = result.traceParent.split("-")
        assert parts[1] == "0af7651916cd43dd8448eb211c80319c"  # same trace ID
        assert parts[2] == "00f067aa0ba902b7"  # new span ID
        assert result.spanID == "00f067aa0ba902b7"

    def test_preserves_trace_flags(self):
        parent = pb.TraceContext(
            traceParent="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00",
            spanID="b7ad6b7169203331",
        )
        result = tracing.reconstruct_trace_context(parent, "1234567890abcdef")
        assert result is not None
        parts = result.traceParent.split("-")
        assert parts[3] == "00"  # flags preserved

    def test_returns_none_for_invalid_parent(self):
        parent = pb.TraceContext(traceParent="invalid", spanID="bad")
        result = tracing.reconstruct_trace_context(parent, "1234567890abcdef")
        assert result is None

    @patch.object(tracing, '_OTEL_AVAILABLE', False)
    def test_returns_none_without_otel(self):
        parent = pb.TraceContext(
            traceParent="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            spanID="b7ad6b7169203331",
        )
        result = tracing.reconstruct_trace_context(parent, "00f067aa0ba902b7")
        assert result is None


class TestBuildOrchestrationTraceContext:
    """Tests for build_orchestration_trace_context."""

    def test_returns_none_when_start_time_none(self):
        result = tracing.build_orchestration_trace_context(None)
        assert result is None

    def test_builds_context_with_start_time(self):
        start_time_ns = 1704067200000000000  # 2024-01-01T00:00:00Z
        result = tracing.build_orchestration_trace_context(start_time_ns)
        assert result is not None
        assert result.spanStartTime.seconds == 1704067200
        assert result.spanStartTime.nanos == 0

    def test_builds_context_with_span_id(self):
        start_time_ns = 1704067200000000000
        result = tracing.build_orchestration_trace_context(
            start_time_ns, span_id="00f067aa0ba902b7")
        assert result is not None
        assert result.spanStartTime.seconds == 1704067200
        assert result.HasField("spanID")
        assert result.spanID.value == "00f067aa0ba902b7"

    def test_builds_context_without_span_id(self):
        start_time_ns = 1704067200000000000
        result = tracing.build_orchestration_trace_context(start_time_ns)
        assert result is not None
        assert not result.HasField("spanID")


class TestReplayDoesNotEmitSpans:
    """Tests that replayed (old) events do NOT re-emit client spans for
    activities, sub-orchestrations, or timers.  CLIENT spans are deferred
    until the completion event arrives as a new event; during replay
    (is_replaying=True) no CLIENT spans are produced."""

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
    """Tests that orchestration SERVER spans are only emitted on completion
    (emit-and-close pattern) — no inter-dispatch storage."""

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

        # No span exported — orchestration is not yet complete
        assert len(self._get_orch_server_spans(otel_setup)) == 0

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

    def test_error_exports_failed_span(self, otel_setup):
        """When an orchestration raises an unhandled error, a span is
        exported with ERROR status."""

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

    def test_separate_instances_get_separate_spans(self, otel_setup):
        """Two different orchestration instances should produce independent
        spans when each completes."""

        def orchestrator(ctx: task.OrchestrationContext, _):
            return "done"

        registry = worker._Registry()
        name = registry.add_orchestrator(orchestrator)
        w, stub = self._make_worker_with_registry(registry)

        start_time = datetime(2020, 1, 1, 12, 0, 0)
        instance_a = "inst-a"
        instance_b = "inst-b"

        for iid in (instance_a, instance_b):
            w._execute_orchestrator(pb.OrchestratorRequest(
                instanceId=iid,
                newEvents=[
                    helpers.new_orchestrator_started_event(start_time),
                    helpers.new_execution_started_event(
                        name, iid, encoded_input=None),
                ],
            ), stub, f"t-{iid}")

        orch_spans = self._get_orch_server_spans(otel_setup)
        assert len(orch_spans) == 2

    def test_initial_dispatch_defers_activity_client_spans(self, otel_setup):
        """On the first dispatch, no CLIENT span is emitted because the
        span is deferred until the activity completes (taskCompleted /
        taskFailed arrives in a later dispatch)."""

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

        # No CLIENT span yet — deferred until completion
        client_spans = [
            s for s in otel_setup.get_finished_spans()
            if s.kind == trace.SpanKind.CLIENT
        ]
        assert len(client_spans) == 0

    def test_span_id_consistent_across_dispatches(self, otel_setup):
        """The orchestration span ID must be persisted across dispatches
        so that child spans (activities, timers) are all parented under
        the same orchestration SERVER span."""
        from datetime import timedelta  # noqa: F401

        def dummy_activity(ctx, _):
            pass

        def orchestrator(ctx: task.OrchestrationContext, _):
            r1 = yield ctx.call_activity(dummy_activity, input=1)
            r2 = yield ctx.call_activity(dummy_activity, input=2)
            return [r1, r2]

        registry = worker._Registry()
        name = registry.add_orchestrator(orchestrator)
        registry.add_activity(dummy_activity)
        w, stub = self._make_worker_with_registry(registry)

        start_time = datetime(2020, 1, 1, 12, 0, 0)
        activity_name = task.get_name(dummy_activity)

        # Dispatch 1: orchestration starts, first activity scheduled
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId=TEST_INSTANCE_ID,
            newEvents=[
                helpers.new_orchestrator_started_event(start_time),
                helpers.new_execution_started_event(
                    name, TEST_INSTANCE_ID, encoded_input=None,
                    parent_trace_context=pb.TraceContext(
                        traceParent=f"00-{_SAMPLE_TRACE_ID}-{_SAMPLE_PARENT_SPAN_ID}-01",
                        spanID=_SAMPLE_PARENT_SPAN_ID,
                    ),
                ),
            ],
        ), stub, "t1")

        # Capture the orchestration trace context from the response
        call_args = stub.CompleteOrchestratorTask.call_args
        resp1 = call_args[0][0]
        orch_trace_ctx_1 = resp1.orchestrationTraceContext
        assert orch_trace_ctx_1.HasField("spanID")
        span_id_1 = orch_trace_ctx_1.spanID.value
        assert span_id_1 != ""

        otel_setup.clear()

        # Dispatch 2: first activity completes, second activity scheduled
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId=TEST_INSTANCE_ID,
            orchestrationTraceContext=orch_trace_ctx_1,
            pastEvents=[
                helpers.new_orchestrator_started_event(start_time),
                helpers.new_execution_started_event(
                    name, TEST_INSTANCE_ID, encoded_input=None,
                    parent_trace_context=pb.TraceContext(
                        traceParent=f"00-{_SAMPLE_TRACE_ID}-{_SAMPLE_PARENT_SPAN_ID}-01",
                        spanID=_SAMPLE_PARENT_SPAN_ID,
                    ),
                ),
                helpers.new_task_scheduled_event(1, activity_name),
            ],
            newEvents=[
                helpers.new_task_completed_event(1, json.dumps(10)),
            ],
        ), stub, "t2")

        # Capture the orchestration trace context from the second response
        call_args = stub.CompleteOrchestratorTask.call_args
        resp2 = call_args[0][0]
        orch_trace_ctx_2 = resp2.orchestrationTraceContext
        assert orch_trace_ctx_2.HasField("spanID")
        span_id_2 = orch_trace_ctx_2.spanID.value

        # The span ID must be the same across dispatches
        assert span_id_1 == span_id_2

    def test_child_spans_parented_under_orchestrator_span(self, otel_setup):
        """Activities and timers should be parented under the orchestration
        SERVER span, and the orchestrator span ID must be consistent across
        dispatches."""
        from datetime import timedelta

        def dummy_activity(ctx, _):
            pass

        def orchestrator(ctx: task.OrchestrationContext, _):
            due = ctx.current_utc_datetime + timedelta(seconds=1)
            yield ctx.create_timer(due)
            r1 = yield ctx.call_activity(dummy_activity, input=1)
            return r1

        registry = worker._Registry()
        name = registry.add_orchestrator(orchestrator)
        registry.add_activity(dummy_activity)
        w, stub = self._make_worker_with_registry(registry)

        start_time = datetime(2020, 1, 1, 12, 0, 0)
        fire_at = start_time + timedelta(seconds=1)
        activity_name = task.get_name(dummy_activity)

        parent_tp = f"00-{_SAMPLE_TRACE_ID}-{_SAMPLE_PARENT_SPAN_ID}-01"
        parent_ctx = pb.TraceContext(
            traceParent=parent_tp,
            spanID=_SAMPLE_PARENT_SPAN_ID,
        )

        # Dispatch 1: start, timer scheduled
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId=TEST_INSTANCE_ID,
            newEvents=[
                helpers.new_orchestrator_started_event(start_time),
                helpers.new_execution_started_event(
                    name, TEST_INSTANCE_ID, encoded_input=None,
                    parent_trace_context=parent_ctx,
                ),
            ],
        ), stub, "t1")

        call_args = stub.CompleteOrchestratorTask.call_args
        resp1 = call_args[0][0]
        orch_trace_ctx = resp1.orchestrationTraceContext
        orch_span_id = orch_trace_ctx.spanID.value
        otel_setup.clear()

        # Dispatch 2: timer fires, activity scheduled
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId=TEST_INSTANCE_ID,
            orchestrationTraceContext=orch_trace_ctx,
            pastEvents=[
                helpers.new_orchestrator_started_event(start_time),
                helpers.new_execution_started_event(
                    name, TEST_INSTANCE_ID, encoded_input=None,
                    parent_trace_context=parent_ctx,
                ),
                helpers.new_timer_created_event(1, fire_at),
            ],
            newEvents=[
                helpers.new_timer_fired_event(1, fire_at),
            ],
        ), stub, "t2")

        # Timer span should be parented under the orchestration span
        timer_spans = [
            s for s in otel_setup.get_finished_spans()
            if s.kind == trace.SpanKind.INTERNAL and "timer" in s.name
        ]
        assert len(timer_spans) == 1
        assert timer_spans[0].parent is not None
        assert timer_spans[0].parent.span_id == int(orch_span_id, 16)

        call_args = stub.CompleteOrchestratorTask.call_args
        resp2 = call_args[0][0]
        orch_trace_ctx_2 = resp2.orchestrationTraceContext
        # Span ID must be consistent
        assert orch_trace_ctx_2.spanID.value == orch_span_id
        otel_setup.clear()

        # Dispatch 3: activity completes, orchestration finishes
        w._execute_orchestrator(pb.OrchestratorRequest(
            instanceId=TEST_INSTANCE_ID,
            orchestrationTraceContext=orch_trace_ctx_2,
            pastEvents=[
                helpers.new_orchestrator_started_event(start_time),
                helpers.new_execution_started_event(
                    name, TEST_INSTANCE_ID, encoded_input=None,
                    parent_trace_context=parent_ctx,
                ),
                helpers.new_timer_created_event(1, fire_at),
                helpers.new_timer_fired_event(1, fire_at),
                helpers.new_task_scheduled_event(2, activity_name),
            ],
            newEvents=[
                helpers.new_task_completed_event(2, json.dumps("result")),
            ],
        ), stub, "t3")

        # Orchestration SERVER span should use the same span ID
        orch_spans = self._get_orch_server_spans(otel_setup)
        assert len(orch_spans) == 1
        assert orch_spans[0].context.span_id == int(orch_span_id, 16)

        # Orchestration should be parented under the PRODUCER span
        assert orch_spans[0].parent is not None
        assert orch_spans[0].parent.span_id == int(_SAMPLE_PARENT_SPAN_ID, 16)


# ---------------------------------------------------------------------------
# Tests for _parse_traceparent
# ---------------------------------------------------------------------------


class TestParseTraceparent:
    """Tests for tracing._parse_traceparent."""

    def test_valid_traceparent(self):
        tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        result = tracing._parse_traceparent(tp)
        assert result is not None
        trace_id, span_id, flags = result
        assert trace_id == int("0af7651916cd43dd8448eb211c80319c", 16)
        assert span_id == int("b7ad6b7169203331", 16)
        assert flags == 1

    def test_invalid_format(self):
        assert tracing._parse_traceparent("not-a-traceparent") is None

    def test_too_few_parts(self):
        assert tracing._parse_traceparent("00-abc") is None

    def test_zero_trace_id(self):
        tp = "00-00000000000000000000000000000000-b7ad6b7169203331-01"
        assert tracing._parse_traceparent(tp) is None

    def test_zero_span_id(self):
        tp = "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01"
        assert tracing._parse_traceparent(tp) is None

    def test_non_hex_values(self):
        tp = "00-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-b7ad6b7169203331-01"
        assert tracing._parse_traceparent(tp) is None

    def test_flags_zero(self):
        tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"
        result = tracing._parse_traceparent(tp)
        assert result is not None
        assert result[2] == 0


# ---------------------------------------------------------------------------
# Tests for generate_client_trace_context
# ---------------------------------------------------------------------------


class TestGenerateClientTraceContext:
    """Tests for tracing.generate_client_trace_context."""

    def test_returns_none_without_parent(self):
        assert tracing.generate_client_trace_context(None) is None

    def test_returns_none_with_invalid_parent(self):
        ctx = pb.TraceContext(traceParent="invalid", spanID="bad")
        assert tracing.generate_client_trace_context(ctx) is None

    def test_generates_valid_traceparent(self):
        parent = pb.TraceContext(
            traceParent="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            spanID="b7ad6b7169203331",
        )
        result = tracing.generate_client_trace_context(parent)
        assert result is not None
        assert result.traceParent != ""
        assert result.spanID != ""

    def test_preserves_trace_id(self):
        parent = pb.TraceContext(
            traceParent="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            spanID="b7ad6b7169203331",
        )
        result = tracing.generate_client_trace_context(parent)
        assert result is not None
        parts = result.traceParent.split("-")
        assert parts[1] == "0af7651916cd43dd8448eb211c80319c"

    def test_generates_different_span_id(self):
        parent = pb.TraceContext(
            traceParent="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            spanID="b7ad6b7169203331",
        )
        result = tracing.generate_client_trace_context(parent)
        assert result is not None
        assert result.spanID != parent.spanID

    def test_span_id_matches_traceparent(self):
        parent = pb.TraceContext(
            traceParent="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            spanID="b7ad6b7169203331",
        )
        result = tracing.generate_client_trace_context(parent)
        assert result is not None
        parts = result.traceParent.split("-")
        assert parts[2] == result.spanID

    @patch.object(tracing, '_OTEL_AVAILABLE', False)
    def test_returns_none_without_otel(self):
        parent = pb.TraceContext(
            traceParent="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            spanID="b7ad6b7169203331",
        )
        assert tracing.generate_client_trace_context(parent) is None


# ---------------------------------------------------------------------------
# Tests for emit_client_span
# ---------------------------------------------------------------------------


class TestEmitClientSpan:
    """Tests for tracing.emit_client_span."""

    def test_emits_span_with_correct_attributes(self, otel_setup: InMemorySpanExporter):
        tracing.emit_client_span(
            "activity", "SayHello", "inst-1", task_id=5,
            client_trace_context=_make_client_trace_ctx(),
            parent_trace_context=_make_parent_trace_ctx(),
            start_time_ns=1_000_000_000,
            end_time_ns=2_000_000_000,
        )
        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s.name == "activity:SayHello"
        assert s.kind == trace.SpanKind.CLIENT
        assert s.attributes is not None
        assert s.attributes[tracing.ATTR_TASK_TYPE] == "activity"
        assert s.attributes[tracing.ATTR_TASK_NAME] == "SayHello"
        assert s.attributes[tracing.ATTR_TASK_INSTANCE_ID] == "inst-1"
        assert s.attributes[tracing.ATTR_TASK_TASK_ID] == "5"

    def test_span_has_correct_trace_and_span_ids(self, otel_setup: InMemorySpanExporter):
        tracing.emit_client_span(
            "activity", "Act", "inst-1", task_id=1,
            client_trace_context=_make_client_trace_ctx(),
            parent_trace_context=_make_parent_trace_ctx(),
            start_time_ns=1_000_000_000,
            end_time_ns=2_000_000_000,
        )
        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s.context is not None
        assert s.context.trace_id == int(_SAMPLE_TRACE_ID, 16)
        assert s.context.span_id == int(_SAMPLE_CLIENT_SPAN_ID, 16)

    def test_span_parent_matches_parent_trace_context(self, otel_setup: InMemorySpanExporter):
        tracing.emit_client_span(
            "activity", "Act", "inst-1", task_id=1,
            client_trace_context=_make_client_trace_ctx(),
            parent_trace_context=_make_parent_trace_ctx(),
        )
        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s.parent is not None
        assert s.parent.span_id == int(_SAMPLE_PARENT_SPAN_ID, 16)

    def test_error_span(self, otel_setup: InMemorySpanExporter):
        tracing.emit_client_span(
            "activity", "Act", "inst-1", task_id=1,
            client_trace_context=_make_client_trace_ctx(),
            parent_trace_context=_make_parent_trace_ctx(),
            is_error=True,
            error_message="task failed",
        )
        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].status.status_code == StatusCode.ERROR
        assert spans[0].status.description is not None
        assert "task failed" in spans[0].status.description

    def test_custom_timestamps(self, otel_setup: InMemorySpanExporter):
        tracing.emit_client_span(
            "activity", "Act", "inst-1", task_id=1,
            client_trace_context=_make_client_trace_ctx(),
            start_time_ns=5_000_000_000,
            end_time_ns=10_000_000_000,
        )
        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].start_time == 5_000_000_000
        assert spans[0].end_time == 10_000_000_000

    def test_version_included(self, otel_setup: InMemorySpanExporter):
        tracing.emit_client_span(
            "orchestration", "SubOrch", "inst-1", task_id=1,
            client_trace_context=_make_client_trace_ctx(),
            version="2.0",
        )
        spans = otel_setup.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].name == "orchestration:SubOrch@(2.0)"
        assert spans[0].attributes is not None
        assert spans[0].attributes[tracing.ATTR_TASK_VERSION] == "2.0"

    def test_noop_with_invalid_client_trace_context(self, otel_setup: InMemorySpanExporter):
        tracing.emit_client_span(
            "activity", "Act", "inst-1", task_id=1,
            client_trace_context=pb.TraceContext(traceParent="bad"),
        )
        assert len(otel_setup.get_finished_spans()) == 0

    @patch.object(tracing, '_OTEL_AVAILABLE', False)
    def test_noop_without_otel(self, otel_setup: InMemorySpanExporter):
        tracing.emit_client_span(
            "activity", "Act", "inst-1", task_id=1,
            client_trace_context=_make_client_trace_ctx(),
        )
        assert len(otel_setup.get_finished_spans()) == 0


# ---------------------------------------------------------------------------
# Integration tests for deferred CLIENT span lifecycle
# ---------------------------------------------------------------------------


class TestDeferredClientSpanIntegration:
    """End-to-end tests verifying that CLIENT spans are emitted with proper
    timestamps when taskCompleted / taskFailed / sub-orchestration events
    arrive as new events."""

    def _make_traced_task_scheduled_event(
        self, event_id, name, client_traceparent, timestamp_seconds=100,
    ):
        """Build a taskScheduled event with parentTraceContext and timestamp."""
        ts = timestamp_pb2.Timestamp()
        ts.FromSeconds(timestamp_seconds)
        return pb.HistoryEvent(
            eventId=event_id,
            timestamp=ts,
            taskScheduled=pb.TaskScheduledEvent(
                name=name,
                parentTraceContext=pb.TraceContext(
                    traceParent=client_traceparent,
                    spanID=client_traceparent.split("-")[2],
                ),
            ),
        )

    def _make_traced_task_completed_event(self, task_id, result, timestamp_seconds=200):
        ts = timestamp_pb2.Timestamp()
        ts.FromSeconds(timestamp_seconds)
        return pb.HistoryEvent(
            eventId=-1,
            timestamp=ts,
            taskCompleted=pb.TaskCompletedEvent(
                taskScheduledId=task_id,
                result=wrappers_pb2.StringValue(value=result) if result else None,
            ),
        )

    def _make_traced_task_failed_event(self, task_id, error_msg, timestamp_seconds=200):
        ts = timestamp_pb2.Timestamp()
        ts.FromSeconds(timestamp_seconds)
        return pb.HistoryEvent(
            eventId=-1,
            timestamp=ts,
            taskFailed=pb.TaskFailedEvent(
                taskScheduledId=task_id,
                failureDetails=pb.TaskFailureDetails(
                    errorMessage=error_msg, errorType="TaskFailedError",
                ),
            ),
        )

    def _make_execution_started_with_trace(self, name, instance_id):
        parent_tp = f"00-{_SAMPLE_TRACE_ID}-{_SAMPLE_PARENT_SPAN_ID}-01"
        return pb.HistoryEvent(
            eventId=-1,
            timestamp=timestamp_pb2.Timestamp(),
            executionStarted=pb.ExecutionStartedEvent(
                name=name,
                orchestrationInstance=pb.OrchestrationInstance(instanceId=instance_id),
                parentTraceContext=pb.TraceContext(
                    traceParent=parent_tp,
                    spanID=_SAMPLE_PARENT_SPAN_ID,
                ),
            ),
        )

    def test_activity_completed_emits_client_span(self, otel_setup: InMemorySpanExporter):
        """When taskCompleted arrives as a new event, a CLIENT span is
        emitted with start_time=taskScheduled.timestamp and
        end_time=taskCompleted.timestamp."""

        def dummy_activity(ctx, _):
            pass

        def orchestrator(ctx: task.OrchestrationContext, _):
            result = yield ctx.call_activity(dummy_activity, input="hi")
            return result

        registry = worker._Registry()
        orch_name = registry.add_orchestrator(orchestrator)
        registry.add_activity(dummy_activity)
        act_name = task.get_name(dummy_activity)

        client_tp = f"00-{_SAMPLE_TRACE_ID}-{_SAMPLE_CLIENT_SPAN_ID}-01"

        # Dispatch 2: old events replay the scheduling, new event completes
        old_events = [
            helpers.new_orchestrator_started_event(),
            self._make_execution_started_with_trace(orch_name, TEST_INSTANCE_ID),
            self._make_traced_task_scheduled_event(1, act_name, client_tp, timestamp_seconds=100),
        ]
        new_events = [
            self._make_traced_task_completed_event(1, json.dumps("result"), timestamp_seconds=200),
        ]

        executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
        result = executor.execute(TEST_INSTANCE_ID, old_events, new_events)

        client_spans = [
            s for s in otel_setup.get_finished_spans()
            if s.kind == trace.SpanKind.CLIENT
        ]
        assert len(client_spans) == 1
        s = client_spans[0]
        assert "activity" in s.name
        assert s.context is not None
        assert s.context.span_id == int(_SAMPLE_CLIENT_SPAN_ID, 16)
        assert s.context.trace_id == int(_SAMPLE_TRACE_ID, 16)
        assert s.start_time == 100_000_000_000  # 100 seconds in ns
        assert s.end_time == 200_000_000_000  # 200 seconds in ns
        # Parent should be the orchestration span, not the PRODUCER span
        assert s.parent is not None
        orch_ctx = result._orchestration_trace_context
        assert orch_ctx is not None
        assert s.parent.span_id == int(orch_ctx.spanID, 16)
        assert s.parent.span_id != int(_SAMPLE_PARENT_SPAN_ID, 16)

    def test_activity_failed_emits_error_client_span(self, otel_setup: InMemorySpanExporter):
        """When taskFailed arrives as a new event, a CLIENT span is
        emitted with ERROR status."""

        def dummy_activity(ctx, _):
            pass

        def orchestrator(ctx: task.OrchestrationContext, _):
            try:
                yield ctx.call_activity(dummy_activity, input="hi")
            except task.TaskFailedError:
                return "caught"

        registry = worker._Registry()
        orch_name = registry.add_orchestrator(orchestrator)
        registry.add_activity(dummy_activity)
        act_name = task.get_name(dummy_activity)

        client_tp = f"00-{_SAMPLE_TRACE_ID}-{_SAMPLE_CLIENT_SPAN_ID}-01"

        old_events = [
            helpers.new_orchestrator_started_event(),
            self._make_execution_started_with_trace(orch_name, TEST_INSTANCE_ID),
            self._make_traced_task_scheduled_event(1, act_name, client_tp, timestamp_seconds=100),
        ]
        new_events = [
            self._make_traced_task_failed_event(1, "boom", timestamp_seconds=250),
        ]

        executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
        executor.execute(TEST_INSTANCE_ID, old_events, new_events)

        client_spans = [
            s for s in otel_setup.get_finished_spans()
            if s.kind == trace.SpanKind.CLIENT
        ]
        assert len(client_spans) == 1
        s = client_spans[0]
        assert s.status.status_code == StatusCode.ERROR
        assert s.status.description is not None
        assert "boom" in s.status.description
        assert s.start_time == 100_000_000_000
        assert s.end_time == 250_000_000_000

    def test_sub_orchestration_completed_emits_client_span(self, otel_setup: InMemorySpanExporter):
        """When subOrchestrationInstanceCompleted arrives as a new event,
        a CLIENT span is emitted."""

        def sub_orch(ctx: task.OrchestrationContext, _):
            return "sub_result"

        def orchestrator(ctx: task.OrchestrationContext, _):
            result = yield ctx.call_sub_orchestrator(sub_orch)
            return result

        registry = worker._Registry()
        sub_name = registry.add_orchestrator(sub_orch)
        orch_name = registry.add_orchestrator(orchestrator)

        client_tp = f"00-{_SAMPLE_TRACE_ID}-{_SAMPLE_CLIENT_SPAN_ID}-01"
        sub_instance_id = f"{TEST_INSTANCE_ID}:0001"

        ts_created = timestamp_pb2.Timestamp()
        ts_created.FromSeconds(150)
        ts_completed = timestamp_pb2.Timestamp()
        ts_completed.FromSeconds(300)

        old_events = [
            helpers.new_orchestrator_started_event(),
            self._make_execution_started_with_trace(orch_name, TEST_INSTANCE_ID),
            pb.HistoryEvent(
                eventId=1,
                timestamp=ts_created,
                subOrchestrationInstanceCreated=pb.SubOrchestrationInstanceCreatedEvent(
                    name=sub_name,
                    instanceId=sub_instance_id,
                    parentTraceContext=pb.TraceContext(
                        traceParent=client_tp,
                        spanID=_SAMPLE_CLIENT_SPAN_ID,
                    ),
                ),
            ),
        ]
        new_events = [
            pb.HistoryEvent(
                eventId=-1,
                timestamp=ts_completed,
                subOrchestrationInstanceCompleted=pb.SubOrchestrationInstanceCompletedEvent(
                    taskScheduledId=1,
                    result=wrappers_pb2.StringValue(value=json.dumps("sub_result")),
                ),
            ),
        ]

        executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
        executor.execute(TEST_INSTANCE_ID, old_events, new_events)

        client_spans = [
            s for s in otel_setup.get_finished_spans()
            if s.kind == trace.SpanKind.CLIENT
        ]
        assert len(client_spans) == 1
        s = client_spans[0]
        assert "orchestration" in s.name
        assert s.context is not None
        assert s.context.span_id == int(_SAMPLE_CLIENT_SPAN_ID, 16)
        assert s.start_time == 150_000_000_000
        assert s.end_time == 300_000_000_000

    def test_replayed_completion_does_not_emit_client_span(self, otel_setup: InMemorySpanExporter):
        """When both taskScheduled and taskCompleted are in old_events
        (full replay), no CLIENT span is emitted."""

        def dummy_activity(ctx, _):
            pass

        def orchestrator(ctx: task.OrchestrationContext, _):
            r1 = yield ctx.call_activity(dummy_activity, input=1)
            r2 = yield ctx.call_activity(dummy_activity, input=2)
            return [r1, r2]

        registry = worker._Registry()
        orch_name = registry.add_orchestrator(orchestrator)
        registry.add_activity(dummy_activity)
        act_name = task.get_name(dummy_activity)

        client_tp = f"00-{_SAMPLE_TRACE_ID}-{_SAMPLE_CLIENT_SPAN_ID}-01"

        old_events = [
            helpers.new_orchestrator_started_event(),
            self._make_execution_started_with_trace(orch_name, TEST_INSTANCE_ID),
            self._make_traced_task_scheduled_event(1, act_name, client_tp, timestamp_seconds=100),
            self._make_traced_task_completed_event(1, json.dumps(10), timestamp_seconds=200),
        ]
        # Second activity completes as new event — but NO parentTraceContext
        # on its taskScheduled, so no CLIENT span for it either
        new_events = [
            helpers.new_task_scheduled_event(2, act_name),
            helpers.new_task_completed_event(2, json.dumps(20)),
        ]

        executor = worker._OrchestrationExecutor(registry, TEST_LOGGER)
        executor.execute(TEST_INSTANCE_ID, old_events, new_events)

        client_spans = [
            s for s in otel_setup.get_finished_spans()
            if s.kind == trace.SpanKind.CLIENT
        ]
        # taskCompleted(1) in old_events -> replaying -> no span
        # taskCompleted(2) in new_events -> no parentTraceContext on taskScheduled -> no span
        assert len(client_spans) == 0
