# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""End-to-end distributed tracing validation using an OTEL collector."""

import time
from typing import Any

import pytest

from ._harness import FunctionApp, OtelCollector

pytestmark = pytest.mark.functions_e2e


def _attribute_value(span: dict[str, Any], key: str) -> str | None:
    for attribute in span.get("attributes", []):
        if attribute.get("key") == key:
            return attribute.get("value", {}).get("stringValue")
    return None


def test_user_span_correlates_to_host_without_worker_lifecycle_duplicates(
        tracing_app: tuple[FunctionApp, OtelCollector]) -> None:
    app, collector = tracing_app
    instance_id = app.start_orchestration("correlated_orchestrator")
    status = app.wait_for_completion(instance_id)
    assert status["runtimeStatus"] == "COMPLETED"

    deadline = time.time() + 30
    spans: list[dict[str, Any]] = []
    user_spans: list[dict[str, Any]] = []
    parent_spans: list[dict[str, Any]] = []
    while time.time() < deadline:
        spans = collector.get_spans()
        user_spans = [
            span for span in spans
            if span.get("name") == "user-orchestrator"
            and _attribute_value(span, "test.instance_id") == instance_id
        ]
        if user_spans:
            user_span = user_spans[0]
            parent_spans = [
                span for span in spans
                if span.get("traceId") == user_span.get("traceId")
                and span.get("spanId") == user_span.get("parentSpanId")
            ]
            if parent_spans:
                break
        time.sleep(0.5)

    assert len(user_spans) == 1, collector.get_logs()
    user_span = user_spans[0]
    summary = [
        (
            span.get("name"),
            span.get("scopeName"),
            span.get("traceId"),
            span.get("spanId"),
            span.get("parentSpanId"),
        )
        for span in spans
    ]
    assert len(parent_spans) == 1, summary
    assert parent_spans[0].get("scopeName") != "durabletask"

    python_lifecycle_spans = [
        span for span in spans
        if span.get("scopeName") == "durabletask"
        and _attribute_value(
            span, "durabletask.task.instance_id") == instance_id
    ]
    assert python_lifecycle_spans == []
