# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the Functions history-export enumeration shim.

The shim replaces the core ``list_terminal_instances`` activity (which uses the
unimplemented ``ListInstanceIds`` gRPC call) with a ``QueryInstances``-based
implementation that pages the matching instances client-side. These tests drive
the activity directly with a stubbed history-export context.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import azure.durable_functions.internal.history_export_compat as hec

_FROM = "2025-01-01T00:00:00+00:00"
_COMPLETED = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _state(instance_id: str, completed_at: datetime = _COMPLETED) -> SimpleNamespace:
    return SimpleNamespace(instance_id=instance_id, last_updated_at=completed_at)


def _bind_states(monkeypatch, states) -> MagicMock:
    client = MagicMock()
    client.get_all_orchestration_states.return_value = states
    monkeypatch.setattr(hec, "_require_context", lambda: SimpleNamespace(client=client))
    return client


def test_single_page_when_all_fit(monkeypatch):
    _bind_states(monkeypatch, [_state("id-1"), _state("id-0")])
    page = hec.list_terminal_instances(
        None, {"completed_time_from": _FROM, "page_size": 10})
    # Sorted deterministically, single page, no further pages.
    assert page["instance_ids"] == ["id-0", "id-1"]
    assert page["continuation_token"] is None


def test_pages_by_page_size_with_keyset_cursor(monkeypatch):
    states = [_state(f"id-{i}") for i in (3, 1, 4, 0, 2)]
    _bind_states(monkeypatch, states)
    base = {"completed_time_from": _FROM, "page_size": 2}

    p1 = hec.list_terminal_instances(None, dict(base))
    assert p1["instance_ids"] == ["id-0", "id-1"]
    assert p1["continuation_token"] == "id-1"

    p2 = hec.list_terminal_instances(
        None, {**base, "continuation_token": p1["continuation_token"]})
    assert p2["instance_ids"] == ["id-2", "id-3"]
    assert p2["continuation_token"] == "id-3"

    # Final page has fewer than page_size items -> no continuation token.
    p3 = hec.list_terminal_instances(
        None, {**base, "continuation_token": p2["continuation_token"]})
    assert p3["instance_ids"] == ["id-4"]
    assert p3["continuation_token"] is None


def test_exact_multiple_of_page_size_terminates(monkeypatch):
    _bind_states(monkeypatch, [_state("id-0"), _state("id-1")])
    base = {"completed_time_from": _FROM, "page_size": 2}

    p1 = hec.list_terminal_instances(None, dict(base))
    assert p1["instance_ids"] == ["id-0", "id-1"]
    # Exactly page_size items remained, so this is the last page.
    assert p1["continuation_token"] is None


def test_completed_time_window_is_applied(monkeypatch):
    early = datetime(2020, 1, 1, tzinfo=timezone.utc)
    late = datetime(2030, 1, 1, tzinfo=timezone.utc)
    _bind_states(monkeypatch, [
        _state("early", early), _state("in-window", _COMPLETED), _state("late", late)])
    page = hec.list_terminal_instances(None, {
        "completed_time_from": _FROM,
        "completed_time_to": "2027-01-01T00:00:00+00:00",
        "page_size": 10,
    })
    assert page["instance_ids"] == ["in-window"]
    assert page["continuation_token"] is None


def test_requires_completed_time_from(monkeypatch):
    _bind_states(monkeypatch, [])
    with pytest.raises(ValueError, match="completed_time_from"):
        hec.list_terminal_instances(None, {"page_size": 10})


def test_empty_result(monkeypatch):
    _bind_states(monkeypatch, [])
    page = hec.list_terminal_instances(
        None, {"completed_time_from": _FROM, "page_size": 10})
    assert page["instance_ids"] == []
    assert page["continuation_token"] is None
