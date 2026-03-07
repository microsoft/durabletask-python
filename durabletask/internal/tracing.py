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
import random
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
ATTR_TASK_STATUS = "durabletask.task.status"
ATTR_TASK_TASK_ID = "durabletask.task.task_id"
ATTR_EVENT_TARGET_INSTANCE_ID = "durabletask.event.target_instance_id"
ATTR_FIRE_AT = "durabletask.fire_at"

# Task type values (used in span names and as attribute values)
TASK_TYPE_ORCHESTRATION = "orchestration"
TASK_TYPE_TIMER = "timer"
TASK_TYPE_EVENT = "event"

# Span name type prefixes (composite types)
SPAN_TYPE_CREATE_ORCHESTRATION = "create_orchestration"
SPAN_TYPE_ORCHESTRATION_EVENT = "orchestration_event"

# Task status values
TASK_STATUS_COMPLETED = "Completed"
TASK_STATUS_FAILED = "Failed"

# W3C Trace Context carrier keys
CARRIER_KEY_TRACEPARENT = "traceparent"
CARRIER_KEY_TRACESTATE = "tracestate"


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
    return f"{TASK_TYPE_ORCHESTRATION}:{orchestration_name}:{TASK_TYPE_TIMER}"


# ---------------------------------------------------------------------------
# Public helpers – extracting / injecting trace context
# ---------------------------------------------------------------------------


def _trace_context_from_carrier(carrier: dict[str, str]) -> Optional[pb.TraceContext]:
    """Build a ``TraceContext`` protobuf from a W3C propagation carrier.

    Returns ``None`` when the carrier does not contain a valid
    ``traceparent`` header.
    """
    traceparent = carrier.get(CARRIER_KEY_TRACEPARENT)
    if not traceparent:
        return None

    tracestate = carrier.get(CARRIER_KEY_TRACESTATE)
    # Format: 00-<trace-id>-<span-id>-<flags>
    parts = traceparent.split("-")
    span_id = parts[2] if len(parts) >= 4 else ""

    return pb.TraceContext(
        traceParent=traceparent,
        spanID=span_id,
        traceState=wrappers_pb2.StringValue(value=tracestate)
        if tracestate else None,
    )


def _parse_traceparent(traceparent: str) -> Optional[tuple[int, int, int]]:
    """Parse a W3C traceparent string into ``(trace_id, span_id, trace_flags)``.

    Returns ``None`` when the string is not a valid traceparent.
    """
    parts = traceparent.split("-")
    if len(parts) < 4:
        return None
    try:
        trace_id = int(parts[1], 16)
        span_id = int(parts[2], 16)
        flags = int(parts[3], 16)
        if trace_id == 0 or span_id == 0:
            return None
        return trace_id, span_id, flags
    except ValueError:
        return None


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

    carrier: dict[str, str] = {CARRIER_KEY_TRACEPARENT: traceparent}
    if proto_ctx.HasField("traceState") and proto_ctx.traceState.value:
        carrier[CARRIER_KEY_TRACESTATE] = proto_ctx.traceState.value

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

def emit_orchestration_span(
    name: str,
    instance_id: str,
    start_time_ns: Optional[int],
    is_failed: bool,
    failure_details: Any = None,
    parent_trace_context: Optional[pb.TraceContext] = None,
    orchestration_trace_context: Optional[pb.TraceContext] = None,
    version: Optional[str] = None,
) -> None:
    """Emit a SERVER span for a completed orchestration (create-and-end).

    The span is created with *start_time_ns* as its start time and
    ended immediately.  This avoids storing spans across dispatches.

    When *orchestration_trace_context* is provided, the span is emitted
    with the pre-determined span ID from that context using the deferred
    ``ReadableSpan`` approach.  This ensures child spans (activities,
    timers, sub-orchestrations) that were created with this context as
    their parent are correctly nested under the orchestration span.

    Falls back to ``tracer.start_span()`` (which generates its own
    span ID) when *orchestration_trace_context* is ``None``.
    """
    if not _OTEL_AVAILABLE:
        return

    span_name = create_span_name(TASK_TYPE_ORCHESTRATION, name, version)

    attrs: dict[str, str] = {
        ATTR_TASK_TYPE: TASK_TYPE_ORCHESTRATION,
        ATTR_TASK_NAME: name,
        ATTR_TASK_INSTANCE_ID: instance_id,
    }
    if version:
        attrs[ATTR_TASK_VERSION] = version

    # When we have a pre-determined orchestration span ID, use the
    # deferred ReadableSpan approach so child spans match.
    if orchestration_trace_context is not None:
        if _emit_orchestration_span_deferred(
            span_name, attrs, start_time_ns, is_failed,
            failure_details, parent_trace_context,
            orchestration_trace_context,
        ):
            return

    # Fallback: use the normal tracer.start_span() approach.
    tracer = trace.get_tracer(_TRACER_NAME)
    parent_ctx = extract_trace_context(parent_trace_context)

    token = None
    if parent_ctx is not None:
        token = otel_context.attach(parent_ctx)

    try:
        span = tracer.start_span(
            span_name,
            kind=SpanKind.SERVER,
            attributes=attrs,
            start_time=start_time_ns,
        )

        if is_failed:
            msg = ""
            if failure_details is not None:
                msg = (
                    str(failure_details.errorMessage)
                    if hasattr(failure_details, 'errorMessage')
                    else str(failure_details)
                )
            span.set_status(StatusCode.ERROR, msg)
            span.set_attribute(ATTR_TASK_STATUS, TASK_STATUS_FAILED)
        else:
            span.set_attribute(ATTR_TASK_STATUS, TASK_STATUS_COMPLETED)

        span.end()
    finally:
        if token is not None:
            otel_context.detach(token)


def _emit_orchestration_span_deferred(
    span_name: str,
    attrs: dict[str, str],
    start_time_ns: Optional[int],
    is_failed: bool,
    failure_details: Any,
    parent_trace_context: Optional[pb.TraceContext],
    orchestration_trace_context: pb.TraceContext,
) -> bool:
    """Emit an orchestration SERVER span with a pre-determined span ID.

    Uses the same ``ReadableSpan`` approach as :func:`emit_client_span`
    to reconstruct the span with the span ID from
    *orchestration_trace_context*.  The span is parented under the
    PRODUCER span identified by *parent_trace_context*.

    Returns ``True`` when the deferred span was emitted successfully,
    ``False`` when it could not be emitted (caller should fall back).
    """
    try:
        from opentelemetry.sdk.trace import ReadableSpan as SdkReadableSpan
        from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
        from opentelemetry.sdk.util.instrumentation import InstrumentationScope
        from opentelemetry.trace.status import Status
    except (ImportError, AttributeError):
        return False

    orch_parsed = _parse_traceparent(orchestration_trace_context.traceParent)
    if orch_parsed is None:
        return False
    trace_id_val, span_id_val, flags_val = orch_parsed

    # Parent is the PRODUCER span
    parent_span_id_val = None
    if parent_trace_context is not None:
        parent_parsed = _parse_traceparent(parent_trace_context.traceParent)
        if parent_parsed is not None:
            parent_span_id_val = parent_parsed[1]

    span_context = trace.SpanContext(
        trace_id=trace_id_val,
        span_id=span_id_val,
        is_remote=False,
        trace_flags=trace.TraceFlags(flags_val),
    )

    parent_context = None
    if parent_span_id_val is not None:
        parent_context = trace.SpanContext(
            trace_id=trace_id_val,
            span_id=parent_span_id_val,
            is_remote=True,
            trace_flags=trace.TraceFlags(flags_val),
        )

    provider = trace.get_tracer_provider()
    if not isinstance(provider, SdkTracerProvider):
        return False

    if start_time_ns is None:
        start_time_ns = time.time_ns()
    end_time_ns = time.time_ns()

    try:
        if is_failed:
            msg = ""
            if failure_details is not None:
                msg = (
                    str(failure_details.errorMessage)
                    if hasattr(failure_details, 'errorMessage')
                    else str(failure_details)
                )
            status = Status(StatusCode.ERROR, msg)
            attrs[ATTR_TASK_STATUS] = TASK_STATUS_FAILED
        else:
            status = Status(StatusCode.UNSET)
            attrs[ATTR_TASK_STATUS] = TASK_STATUS_COMPLETED

        readable_span = SdkReadableSpan(
            name=span_name,
            context=span_context,
            parent=parent_context,
            kind=SpanKind.SERVER,
            attributes=attrs,
            start_time=start_time_ns,
            end_time=end_time_ns,
            resource=provider.resource,
            instrumentation_scope=InstrumentationScope(_TRACER_NAME),
            status=status,
        )

        processor = getattr(provider, '_active_span_processor', None)
        if processor is not None:
            processor.on_end(readable_span)
            return True
        return False
    except Exception:
        logger.debug(
            "Failed to emit deferred orchestration SERVER span (OTel SDK "
            "internals may have changed). Falling back to normal span.",
            exc_info=True,
        )
        return False


# ---------------------------------------------------------------------------
# CLIENT span helpers (deferred create / emit)
#
# Deferred CLIENT spans use opentelemetry-sdk internals (ReadableSpan
# constructor, TracerProvider._active_span_processor) to emit spans
# with pre-determined span IDs.  _is_deferred_span_capable() validates
# these internals are accessible; if not, the SDK falls back to
# parenting SERVER spans directly under the orchestration span.
# ---------------------------------------------------------------------------


def _is_deferred_span_capable() -> bool:
    """Check whether the OTel SDK internals needed for deferred CLIENT spans are available.

    Returns ``False`` when:
    * ``opentelemetry-sdk`` is not installed (only the API is present),
    * the active ``TracerProvider`` is not the SDK implementation, or
    * the private ``_active_span_processor`` attribute is missing.

    Callers should skip CLIENT span generation when this returns ``False``
    so that the orchestration's own trace context is used as the parent
    for downstream SERVER spans instead.
    """
    try:
        from opentelemetry.sdk.trace import ReadableSpan  # noqa: F401
        from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
    except (ImportError, AttributeError):
        return False

    provider = trace.get_tracer_provider()
    if not isinstance(provider, SdkTracerProvider):
        return False
    if not hasattr(provider, '_active_span_processor'):
        return False

    return True


def generate_client_trace_context(
    parent_trace_context: Optional[pb.TraceContext] = None,
) -> Optional[pb.TraceContext]:
    """Generate a trace context for a deferred CLIENT span.

    Creates a new span ID and builds a W3C traceparent string **without**
    creating an OpenTelemetry ``Span``.  The actual CLIENT span will be
    reconstructed later with proper timestamps via :func:`emit_client_span`.

    Returns ``None`` when OpenTelemetry is not available, the SDK
    internals required for deferred span emission are not accessible,
    or *parent_trace_context* is empty / invalid.  When ``None`` is
    returned the caller should fall back to the orchestration's own
    trace context as the parent for downstream spans.
    """
    if not _OTEL_AVAILABLE:
        return None
    if parent_trace_context is None:
        return None

    # Pre-flight: ensure the SDK internals we need in emit_client_span()
    # are accessible.  If not, return None so the caller falls back to
    # the orchestration trace context — the SERVER span will be parented
    # directly under the orchestration span instead of a CLIENT span.
    if not _is_deferred_span_capable():
        return None

    parsed = _parse_traceparent(parent_trace_context.traceParent)
    if parsed is None:
        return None

    trace_id, _parent_span_id, flags = parsed

    # Generate a new span ID for the CLIENT span
    span_id = random.getrandbits(64)
    while span_id == 0:
        span_id = random.getrandbits(64)

    traceparent = f"00-{trace_id:032x}-{span_id:016x}-{flags:02x}"
    return pb.TraceContext(
        traceParent=traceparent,
        spanID=format(span_id, '016x'),
    )


def emit_client_span(
    task_type: str,
    name: str,
    instance_id: str,
    task_id: int,
    client_trace_context: pb.TraceContext,
    parent_trace_context: Optional[pb.TraceContext] = None,
    start_time_ns: Optional[int] = None,
    end_time_ns: Optional[int] = None,
    is_error: bool = False,
    error_message: Optional[str] = None,
    version: Optional[str] = None,
) -> None:
    """Emit a CLIENT span with a specific span ID reconstructed from history.

    The span ID is extracted from *client_trace_context* so that it matches
    the one previously propagated to the downstream SERVER span.  A
    ``ReadableSpan`` is constructed and fed directly to the active span
    processor for export.

    *start_time_ns* and *end_time_ns* should come from the history event
    timestamps (``taskScheduled`` / ``taskCompleted``, etc.).

    If the SDK internals are unavailable or have changed, this function
    logs a debug message and returns silently.  The CLIENT span is
    simply omitted from the trace; the activity/sub-orchestration
    SERVER span remains connected to the orchestration span via the
    fallback parenting established in :func:`generate_client_trace_context`.
    """
    if not _OTEL_AVAILABLE:
        return

    # SDK-internal imports — see _is_deferred_span_capable() for details.
    try:
        from opentelemetry.sdk.trace import ReadableSpan as SdkReadableSpan
        from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
        from opentelemetry.sdk.util.instrumentation import InstrumentationScope
        from opentelemetry.trace.status import Status
    except (ImportError, AttributeError):
        return

    client_parsed = _parse_traceparent(client_trace_context.traceParent)
    if client_parsed is None:
        return
    trace_id_val, span_id_val, flags_val = client_parsed

    # Determine parent span ID from the orchestration's trace context
    parent_span_id_val = None
    if parent_trace_context is not None:
        parent_parsed = _parse_traceparent(parent_trace_context.traceParent)
        if parent_parsed is not None:
            parent_span_id_val = parent_parsed[1]

    span_context = trace.SpanContext(
        trace_id=trace_id_val,
        span_id=span_id_val,
        is_remote=False,
        trace_flags=trace.TraceFlags(flags_val),
    )

    parent_context = None
    if parent_span_id_val is not None:
        parent_context = trace.SpanContext(
            trace_id=trace_id_val,
            span_id=parent_span_id_val,
            is_remote=True,
            trace_flags=trace.TraceFlags(flags_val),
        )

    span_name = create_span_name(task_type, name, version)
    attrs: dict[str, str] = {
        ATTR_TASK_TYPE: task_type,
        ATTR_TASK_NAME: name,
        ATTR_TASK_INSTANCE_ID: instance_id,
        ATTR_TASK_TASK_ID: str(task_id),
    }
    if version:
        attrs[ATTR_TASK_VERSION] = version

    provider = trace.get_tracer_provider()
    if not isinstance(provider, SdkTracerProvider):
        return

    if start_time_ns is None or end_time_ns is None:
        now_ns = time.time_ns()
        if start_time_ns is None:
            start_time_ns = now_ns
        if end_time_ns is None:
            end_time_ns = now_ns

    # Construct a ReadableSpan with the pre-determined span ID and feed
    # it to the span processor for export.
    try:
        status = Status(
            StatusCode.ERROR, error_message or ""
        ) if is_error else Status(StatusCode.UNSET)

        readable_span = SdkReadableSpan(
            name=span_name,
            context=span_context,
            parent=parent_context,
            kind=SpanKind.CLIENT,
            attributes=attrs,
            start_time=start_time_ns,
            end_time=end_time_ns,
            resource=provider.resource,
            instrumentation_scope=InstrumentationScope(_TRACER_NAME),
            status=status,
        )

        processor = getattr(provider, '_active_span_processor', None)
        if processor is not None:
            processor.on_end(readable_span)
    except Exception:
        logger.debug(
            "Failed to emit deferred CLIENT span (OTel SDK internals may "
            "have changed). The trace will be missing the CLIENT span "
            "layer but remains connected via the orchestration span.",
            exc_info=True,
        )


def emit_timer_span(
    orchestration_name: str,
    instance_id: str,
    timer_id: int,
    fire_at: datetime,
    scheduled_time_ns: Optional[int] = None,
    parent_trace_context: Optional[pb.TraceContext] = None,
) -> None:
    """Emit an Internal span for a timer (emit-and-close pattern).

    When *scheduled_time_ns* is provided the span start time is backdated
    to when the timer was originally created, so the span duration covers
    the full wait period.

    When *parent_trace_context* is provided the span is created as a
    child of that context; otherwise it inherits the ambient context.
    """
    if not _OTEL_AVAILABLE:
        return

    span_name = create_timer_span_name(orchestration_name)
    attrs: dict[str, str] = {
        ATTR_TASK_TYPE: TASK_TYPE_TIMER,
        ATTR_TASK_NAME: orchestration_name,
        ATTR_TASK_INSTANCE_ID: instance_id,
        ATTR_TASK_TASK_ID: str(timer_id),
        ATTR_FIRE_AT: fire_at.isoformat(),
    }

    tracer = trace.get_tracer(_TRACER_NAME)
    parent_ctx = extract_trace_context(parent_trace_context)

    token = None
    if parent_ctx is not None:
        token = otel_context.attach(parent_ctx)

    try:
        span = tracer.start_span(
            span_name,
            kind=SpanKind.INTERNAL,
            attributes=attrs,
            start_time=scheduled_time_ns,
        )
        span.end()
    finally:
        if token is not None:
            otel_context.detach(token)


def emit_event_raised_span(
    event_name: str,
    instance_id: str,
    target_instance_id: Optional[str] = None,
    parent_trace_context: Optional[pb.TraceContext] = None,
) -> None:
    """Emit a Producer span for an event raised from the orchestration.

    When *parent_trace_context* is provided the span is created as a
    child of that context; otherwise it inherits the ambient context.
    """
    if not _OTEL_AVAILABLE:
        return

    span_name = create_span_name(SPAN_TYPE_ORCHESTRATION_EVENT, event_name)
    attrs: dict[str, str] = {
        ATTR_TASK_TYPE: TASK_TYPE_EVENT,
        ATTR_TASK_NAME: event_name,
        ATTR_TASK_INSTANCE_ID: instance_id,
    }
    if target_instance_id:
        attrs[ATTR_EVENT_TARGET_INSTANCE_ID] = target_instance_id

    tracer = trace.get_tracer(_TRACER_NAME)
    parent_ctx = extract_trace_context(parent_trace_context)

    token = None
    if parent_ctx is not None:
        token = otel_context.attach(parent_ctx)

    try:
        span = tracer.start_span(
            span_name,
            kind=SpanKind.PRODUCER,
            attributes=attrs,
        )
        span.end()
    finally:
        if token is not None:
            otel_context.detach(token)


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

    span_name = create_span_name(SPAN_TYPE_CREATE_ORCHESTRATION, name, version)
    attrs: dict[str, str] = {
        ATTR_TASK_TYPE: TASK_TYPE_ORCHESTRATION,
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

    span_name = create_span_name(SPAN_TYPE_ORCHESTRATION_EVENT, event_name)
    attrs: dict[str, str] = {
        ATTR_TASK_TYPE: TASK_TYPE_EVENT,
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


def reconstruct_trace_context(
    parent_trace_context: pb.TraceContext,
    span_id: str,
) -> Optional[pb.TraceContext]:
    """Reconstruct a ``TraceContext`` with a specific span ID.

    Uses the trace ID and flags from *parent_trace_context* but replaces
    the span ID with *span_id*.  This is used to reuse a pre-determined
    orchestration span ID across replays.
    """
    if not _OTEL_AVAILABLE:
        return None

    parsed = _parse_traceparent(parent_trace_context.traceParent)
    if parsed is None:
        return None
    trace_id, _, flags = parsed
    traceparent = f"00-{trace_id:032x}-{span_id}-{flags:02x}"
    return pb.TraceContext(
        traceParent=traceparent,
        spanID=span_id,
    )


def build_orchestration_trace_context(
    start_time_ns: Optional[int],
    span_id: Optional[str] = None,
) -> Optional[pb.OrchestrationTraceContext]:
    """Build an ``OrchestrationTraceContext`` protobuf to return to the sidecar.

    This preserves both the orchestration start time and span ID across
    replays so that all dispatches produce a consistent orchestration
    SERVER span.
    """
    if start_time_ns is None:
        return None

    ctx = pb.OrchestrationTraceContext()

    ts = timestamp_pb2.Timestamp()
    ts.FromNanoseconds(start_time_ns)
    ctx.spanStartTime.CopyFrom(ts)

    if span_id:
        ctx.spanID.CopyFrom(wrappers_pb2.StringValue(value=span_id))

    return ctx
