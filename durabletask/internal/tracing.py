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
from contextlib import contextmanager
from typing import Optional

from google.protobuf import wrappers_pb2

import durabletask.internal.orchestrator_service_pb2 as pb

logger = logging.getLogger("durabletask-tracing")

# ---------------------------------------------------------------------------
# Lazy / optional OpenTelemetry imports
# ---------------------------------------------------------------------------
try:
    from opentelemetry import context as otel_context
    from opentelemetry import trace
    from opentelemetry.trace import (
        SpanKind,
        StatusCode,
    )
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )

    _OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _OTEL_AVAILABLE = False

# Re-export so callers can check without importing opentelemetry themselves.
OTEL_AVAILABLE = _OTEL_AVAILABLE

# The instrumentation scope name used when creating spans.
_TRACER_NAME = "durabletask"


# ---------------------------------------------------------------------------
# Public helpers – extracting / injecting trace context
# ---------------------------------------------------------------------------

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

    traceparent = carrier.get("traceparent")
    if not traceparent:
        return None

    tracestate = carrier.get("tracestate")

    # Extract the span ID from the traceparent header.
    # Format: 00-<trace-id>-<span-id>-<flags>
    parts = traceparent.split("-")
    span_id = parts[2] if len(parts) >= 4 else ""

    return pb.TraceContext(
        traceParent=traceparent,
        spanID=span_id,
        traceState=wrappers_pb2.StringValue(value=tracestate) if tracestate else None,
    )


def extract_trace_context(proto_ctx: Optional[pb.TraceContext]) -> Optional[object]:
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
    kind: Optional[object] = None,
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


def set_span_error(span, ex: Exception) -> None:
    """Record an exception on the given span (if tracing is available)."""
    if not _OTEL_AVAILABLE or span is None:
        return
    span.set_status(StatusCode.ERROR, str(ex))
    span.record_exception(ex)
