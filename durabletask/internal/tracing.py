# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""OpenTelemetry distributed tracing utilities for the Durable Task SDK.

This module provides helpers for propagating W3C Trace Context between
orchestrations, activities, sub-orchestrations, and entities via the
``TraceContext`` protobuf message carried over gRPC.

OpenTelemetry is an **optional** dependency.  When the ``opentelemetry-api``
package is not installed every helper gracefully degrades to a no-op so
that the rest of the SDK continues to work without any tracing overhead.
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Optional

from google.protobuf import timestamp_pb2, wrappers_pb2

import durabletask.internal.orchestrator_service_pb2 as pb

logger = logging.getLogger("durabletask-tracing")

# ---------------------------------------------------------------------------
# Lazy / optional OpenTelemetry imports
# ---------------------------------------------------------------------------
try:
    from opentelemetry import context as otel_context
    from opentelemetry import trace
    from opentelemetry.trace import (
        SpanKind,  # type: ignore[no-redef]
        StatusCode,  # type: ignore[no-redef]
    )
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )

    _OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _OTEL_AVAILABLE = False
    # Provide stub for SpanKind so callers can reference tracing.SpanKind
    # without guarding every reference with OTEL_AVAILABLE checks.

    class SpanKind:  # type: ignore[no-redef]
        INTERNAL: Any = None
        CLIENT: Any = None
        SERVER: Any = None
        PRODUCER: Any = None
        CONSUMER: Any = None

    class StatusCode:  # type: ignore[no-redef]
        OK: Any = None
        ERROR: Any = None
        UNSET: Any = None

# Re-export so callers can check without importing opentelemetry themselves.
OTEL_AVAILABLE = _OTEL_AVAILABLE

# The instrumentation scope name used when creating spans.
_TRACER_NAME = "durabletask"


# ---------------------------------------------------------------------------
# Span attribute keys (mirrors Schema.cs from .NET SDK)
# ---------------------------------------------------------------------------

ATTR_TASK_TYPE = "durabletask.type"
ATTR_TASK_NAME = "durabletask.task.name"
ATTR_TASK_VERSION = "durabletask.task.version"
ATTR_TASK_INSTANCE_ID = "durabletask.task.instance_id"
ATTR_TASK_EXECUTION_ID = "durabletask.task.execution_id"
ATTR_TASK_STATUS = "durabletask.task.status"
ATTR_TASK_TASK_ID = "durabletask.task.task_id"
ATTR_EVENT_TARGET_INSTANCE_ID = "durabletask.event.target_instance_id"
ATTR_FIRE_AT = "durabletask.fire_at"


# ---------------------------------------------------------------------------
# Span name helpers (mirrors TraceActivityConstants / TraceHelper naming)
# ---------------------------------------------------------------------------

def create_span_name(
    span_type: str, task_name: str, version: Optional[str] = None,
) -> str:
    """Build a span name with optional version suffix.

    Examples::

        create_span_name("orchestration", "MyOrch") -> "orchestration:MyOrch"
        create_span_name("activity", "Say", "1.0") -> "activity:Say@(1.0)"
    """
    if version:
        return f"{span_type}:{task_name}@({version})"
    return f"{span_type}:{task_name}"


def create_timer_span_name(orchestration_name: str) -> str:
    """Build a timer span name: ``orchestration:<name>:timer``."""
    return f"orchestration:{orchestration_name}:timer"


# ---------------------------------------------------------------------------
# Public helpers – extracting / injecting trace context
# ---------------------------------------------------------------------------


def _trace_context_from_carrier(carrier: dict[str, str]) -> Optional[pb.TraceContext]:
    """Build a ``TraceContext`` protobuf from a W3C propagation carrier.

    Returns ``None`` when the carrier does not contain a valid
    ``traceparent`` header.
    """
    traceparent = carrier.get("traceparent")
    if not traceparent:
        return None

    tracestate = carrier.get("tracestate")
    # Format: 00-<trace-id>-<span-id>-<flags>
    parts = traceparent.split("-")
    span_id = parts[2] if len(parts) >= 4 else ""

    return pb.TraceContext(
        traceParent=traceparent,
        spanID=span_id,
        traceState=wrappers_pb2.StringValue(value=tracestate)
        if tracestate else None,
    )


def get_current_trace_context() -> Optional[pb.TraceContext]:
    """Capture the current OpenTelemetry span context as a protobuf ``TraceContext``.

    Returns ``None`` when OpenTelemetry is not installed or there is no
    active span.
    """
    if not _OTEL_AVAILABLE:
        return None

    propagator = TraceContextTextMapPropagator()
    carrier: dict[str, str] = {}
    propagator.inject(carrier)
    return _trace_context_from_carrier(carrier)


def extract_trace_context(proto_ctx: Optional[pb.TraceContext]) -> Optional[Any]:
    """Convert a protobuf ``TraceContext`` into an OpenTelemetry ``Context``.

    Returns ``None`` when OpenTelemetry is not installed or the supplied
    context is empty / ``None``.
    """
    if not _OTEL_AVAILABLE or proto_ctx is None:
        return None

    traceparent = proto_ctx.traceParent
    if not traceparent:
        return None

    carrier: dict[str, str] = {"traceparent": traceparent}
    if proto_ctx.HasField("traceState") and proto_ctx.traceState.value:
        carrier["tracestate"] = proto_ctx.traceState.value

    propagator = TraceContextTextMapPropagator()
    ctx = propagator.extract(carrier)
    return ctx


@contextmanager
def start_span(
    name: str,
    trace_context: Optional[pb.TraceContext] = None,
    kind: Any = None,
    attributes: Optional[dict[str, str]] = None,
):
    """Context manager that starts an OpenTelemetry span linked to a parent trace context.

    If OpenTelemetry is not installed, the block executes without tracing.

    Parameters
    ----------
    name:
        Human-readable span name (e.g. ``"activity:say_hello"``).
    trace_context:
        The protobuf ``TraceContext`` received from the sidecar.  When
        provided the new span will be created as a **child** of this
        context.
    kind:
        The ``SpanKind`` for the new span.  Defaults to ``SpanKind.INTERNAL``.
    attributes:
        Optional dictionary of span attributes.
    """
    if not _OTEL_AVAILABLE:
        yield None
        return

    parent_ctx = extract_trace_context(trace_context)

    if kind is None:
        kind = SpanKind.INTERNAL

    tracer = trace.get_tracer(_TRACER_NAME)

    if parent_ctx is not None:
        token = otel_context.attach(parent_ctx)
        try:
            with tracer.start_as_current_span(
                name, kind=kind, attributes=attributes
            ) as span:
                yield span
        finally:
            otel_context.detach(token)
    else:
        with tracer.start_as_current_span(
            name, kind=kind, attributes=attributes
        ) as span:
            yield span


def set_span_error(span: Any, ex: Exception) -> None:
    """Record an exception on the given span (if tracing is available)."""
    if not _OTEL_AVAILABLE or span is None:
        return
    span.set_status(StatusCode.ERROR, str(ex))
    span.record_exception(ex)


# ---------------------------------------------------------------------------
# Orchestration-level span helpers
# ---------------------------------------------------------------------------

def start_orchestration_span(
    name: str,
    instance_id: str,
    parent_trace_context: Optional[pb.TraceContext] = None,
    orchestration_trace_context: Optional[pb.OrchestrationTraceContext] = None,
    version: Optional[str] = None,
) -> tuple[Any, Any, Optional[str], Optional[int]]:
    """Start a Server span for an orchestration execution.

    Returns a tuple ``(span, token, span_id, start_time_ns)`` where
    *token* is the OTel context token(s) that must be detached later, and
    *span_id* / *start_time_ns* are the values to feed back to the sidecar
    on the first execution.

    If OpenTelemetry is not available every element of the tuple is ``None``.
    """
    if not _OTEL_AVAILABLE:
        return None, None, None, None

    span_name = create_span_name("orchestration", name, version)

    attrs: dict[str, str] = {
        ATTR_TASK_TYPE: "orchestration",
        ATTR_TASK_NAME: name,
        ATTR_TASK_INSTANCE_ID: instance_id,
    }
    if version:
        attrs[ATTR_TASK_VERSION] = version

    tracer = trace.get_tracer(_TRACER_NAME)
    parent_ctx = extract_trace_context(parent_trace_context)

    # Determine start time: prefer the value persisted in the
    # OrchestrationTraceContext (replay / cross-worker), otherwise
    # capture "now" so the value can be fed back to the sidecar.
    start_time_ns: Optional[int] = None
    if orchestration_trace_context is not None and orchestration_trace_context.HasField("spanStartTime"):
        start_time_ns = orchestration_trace_context.spanStartTime.ToNanoseconds()
    else:
        start_time_ns = time.time_ns()

    token = None
    if parent_ctx is not None:
        token = otel_context.attach(parent_ctx)

    span = tracer.start_span(
        span_name,
        kind=SpanKind.SERVER,
        attributes=attrs,
        start_time=start_time_ns,
    )

    # Make this span the current span
    ctx_with_span = trace.set_span_in_context(span)
    span_token = otel_context.attach(ctx_with_span)

    # Extract the span ID and start time to return to sidecar
    span_ctx = span.get_span_context()
    span_id_hex = format(span_ctx.span_id, '016x')

    return span, (token, span_token), span_id_hex, start_time_ns


def reattach_orchestration_span(span: Any) -> Any:
    """Re-attach a saved orchestration span as the current span.

    Returns the context token that must be detached later.
    Returns ``None`` when OTel is not available or *span* is ``None``.
    """
    if not _OTEL_AVAILABLE or span is None:
        return None

    ctx_with_span = trace.set_span_in_context(span)
    return otel_context.attach(ctx_with_span)


def detach_orchestration_tokens(tokens: Any) -> None:
    """Detach context tokens without ending the span.

    Use this on intermediate dispatches where the orchestration is not
    yet complete so the span is kept alive for subsequent dispatches.
    """
    if tokens is None:
        return
    parent_token, span_token = tokens
    if span_token is not None:
        otel_context.detach(span_token)
    if parent_token is not None:
        otel_context.detach(parent_token)


def end_orchestration_span(
    span: Any,
    tokens: Any,
    is_complete: bool,
    is_failed: bool,
    failure_details: Any = None,
) -> None:
    """End the orchestration Server span, setting status and detaching context."""
    if not _OTEL_AVAILABLE or span is None:
        return

    if is_complete:
        if is_failed:
            msg = ""
            if failure_details is not None:
                msg = (
                    str(failure_details.errorMessage)
                    if hasattr(failure_details, 'errorMessage')
                    else str(failure_details)
                )
            span.set_status(StatusCode.ERROR, msg)
            span.set_attribute(ATTR_TASK_STATUS, "Failed")
        else:
            span.set_attribute(ATTR_TASK_STATUS, "Completed")

    span.end()

    detach_orchestration_tokens(tokens)


# ---------------------------------------------------------------------------
# CLIENT span helpers (create / end)
# ---------------------------------------------------------------------------


def create_client_span_context(
    task_type: str,
    name: str,
    instance_id: str,
    task_id: Optional[int] = None,
    version: Optional[str] = None,
) -> Optional[tuple[pb.TraceContext, Any]]:
    """Create a CLIENT span and return its trace context for propagation.

    The span is **not** ended here — the caller must keep a reference
    and call :func:`end_client_span` when the downstream task completes
    so the CLIENT span captures the full scheduling-to-completion duration.

    Returns a ``(TraceContext, span)`` tuple, or ``None`` when
    OpenTelemetry is not installed.
    """
    if not _OTEL_AVAILABLE:
        return None

    span_name = create_span_name(task_type, name, version)
    attrs: dict[str, str] = {
        ATTR_TASK_TYPE: task_type,
        ATTR_TASK_NAME: name,
        ATTR_TASK_INSTANCE_ID: instance_id,
    }
    if task_id is not None:
        attrs[ATTR_TASK_TASK_ID] = str(task_id)
    if version:
        attrs[ATTR_TASK_VERSION] = version

    tracer = trace.get_tracer(_TRACER_NAME)
    span = tracer.start_span(
        span_name,
        kind=SpanKind.CLIENT,
        attributes=attrs,
    )

    # Capture the trace context with this CLIENT span as the current span,
    # so that the downstream SERVER span is parented by this CLIENT span.
    ctx = trace.set_span_in_context(span)
    propagator = TraceContextTextMapPropagator()
    carrier: dict[str, str] = {}
    propagator.inject(carrier, context=ctx)

    trace_ctx = _trace_context_from_carrier(carrier)
    if trace_ctx is None:
        span.end()
        return None

    return trace_ctx, span


def end_client_span(
    span,
    is_error: bool = False,
    error_message: Optional[str] = None,
) -> None:
    """End a CLIENT span previously created by :func:`create_client_span_context`.

    If *is_error* is ``True`` the span status is set to ERROR before closing.
    """
    if span is None or not _OTEL_AVAILABLE:
        return
    if is_error:
        span.set_status(StatusCode.ERROR, error_message or "")
    span.end()


def emit_timer_span(
    orchestration_name: str,
    instance_id: str,
    timer_id: int,
    fire_at: datetime,
    scheduled_time_ns: Optional[int] = None,
) -> None:
    """Emit an Internal span for a timer (emit-and-close pattern).

    When *scheduled_time_ns* is provided the span start time is backdated
    to when the timer was originally created, so the span duration covers
    the full wait period.
    """
    if not _OTEL_AVAILABLE:
        return

    span_name = create_timer_span_name(orchestration_name)
    attrs: dict[str, str] = {
        ATTR_TASK_TYPE: "timer",
        ATTR_TASK_NAME: orchestration_name,
        ATTR_TASK_INSTANCE_ID: instance_id,
        ATTR_TASK_TASK_ID: str(timer_id),
        ATTR_FIRE_AT: fire_at.isoformat(),
    }

    tracer = trace.get_tracer(_TRACER_NAME)
    span = tracer.start_span(
        span_name,
        kind=SpanKind.INTERNAL,
        attributes=attrs,
        start_time=scheduled_time_ns,
    )
    span.end()


def emit_event_raised_span(
    event_name: str,
    instance_id: str,
    target_instance_id: Optional[str] = None,
) -> None:
    """Emit a Producer span for an event raised from the orchestration."""
    if not _OTEL_AVAILABLE:
        return

    span_name = create_span_name("orchestration_event", event_name)
    attrs: dict[str, str] = {
        ATTR_TASK_TYPE: "event",
        ATTR_TASK_NAME: event_name,
        ATTR_TASK_INSTANCE_ID: instance_id,
    }
    if target_instance_id:
        attrs[ATTR_EVENT_TARGET_INSTANCE_ID] = target_instance_id

    tracer = trace.get_tracer(_TRACER_NAME)
    span = tracer.start_span(
        span_name,
        kind=SpanKind.PRODUCER,
        attributes=attrs,
    )
    span.end()


# ---------------------------------------------------------------------------
# Client-side Producer span helpers
# ---------------------------------------------------------------------------

@contextmanager
def start_create_orchestration_span(
    name: str,
    instance_id: str,
    version: Optional[str] = None,
):
    """Context manager for a Producer span when scheduling a new orchestration.

    Yields the span; caller should capture the trace context after entering
    the span context so it can be injected into the gRPC request.
    """
    if not _OTEL_AVAILABLE:
        yield None
        return

    span_name = create_span_name("create_orchestration", name, version)
    attrs: dict[str, str] = {
        ATTR_TASK_TYPE: "orchestration",
        ATTR_TASK_NAME: name,
        ATTR_TASK_INSTANCE_ID: instance_id,
    }
    if version:
        attrs[ATTR_TASK_VERSION] = version

    tracer = trace.get_tracer(_TRACER_NAME)
    with tracer.start_as_current_span(
        span_name,
        kind=SpanKind.PRODUCER,
        attributes=attrs,
    ) as span:
        yield span


@contextmanager
def start_raise_event_span(
    event_name: str,
    target_instance_id: str,
):
    """Context manager for a Producer span when raising an event from the client."""
    if not _OTEL_AVAILABLE:
        yield None
        return

    span_name = create_span_name("orchestration_event", event_name)
    attrs: dict[str, str] = {
        ATTR_TASK_TYPE: "event",
        ATTR_TASK_NAME: event_name,
        ATTR_EVENT_TARGET_INSTANCE_ID: target_instance_id,
    }

    tracer = trace.get_tracer(_TRACER_NAME)
    with tracer.start_as_current_span(
        span_name,
        kind=SpanKind.PRODUCER,
        attributes=attrs,
    ) as span:
        yield span


def build_orchestration_trace_context(
    span_id: Optional[str],
    start_time_ns: Optional[int],
) -> Optional[pb.OrchestrationTraceContext]:
    """Build an ``OrchestrationTraceContext`` protobuf to return to the sidecar.

    This preserves the span ID and start time across replays.
    """
    if span_id is None:
        return None

    ctx = pb.OrchestrationTraceContext()
    ctx.spanID.CopyFrom(wrappers_pb2.StringValue(value=span_id))

    if start_time_ns is not None:
        ts = timestamp_pb2.Timestamp()
        ts.FromNanoseconds(start_time_ns)
        ctx.spanStartTime.CopyFrom(ts)

    return ctx
