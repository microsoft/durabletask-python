# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the export-job state-transition matrix and custom exceptions."""

from __future__ import annotations

import pytest

from durabletask.extensions.history_export import (
    ExportJobError,
    ExportJobInvalidTransitionError,
    ExportJobNotFoundError,
    ExportJobStatus,
)
from durabletask.extensions.history_export.transitions import (
    TRANSITIONS,
    assert_valid_transition,
    is_valid_transition,
)


class TestTransitionsMatrix:
    def test_create_from_none_is_active(self) -> None:
        # ``create`` schedules the orchestrator inline, so the job
        # goes straight to ACTIVE.  Mirrors the .NET flow.
        assert is_valid_transition("create", None, ExportJobStatus.ACTIVE)

    def test_create_from_active_is_rejected(self) -> None:
        assert not is_valid_transition(
            "create", ExportJobStatus.ACTIVE, ExportJobStatus.ACTIVE
        )

    def test_create_from_terminal_revives_job(self) -> None:
        assert is_valid_transition(
            "create", ExportJobStatus.COMPLETED, ExportJobStatus.ACTIVE
        )
        assert is_valid_transition(
            "create", ExportJobStatus.FAILED, ExportJobStatus.ACTIVE
        )

    def test_commit_checkpoint_can_fail_active_job(self) -> None:
        assert is_valid_transition(
            "commit_checkpoint", ExportJobStatus.ACTIVE, ExportJobStatus.FAILED,
        )

    def test_commit_checkpoint_not_allowed_on_terminal(self) -> None:
        assert not is_valid_transition(
            "commit_checkpoint", ExportJobStatus.COMPLETED, ExportJobStatus.ACTIVE,
        )

    def test_mark_completed_requires_active(self) -> None:
        assert is_valid_transition(
            "mark_completed", ExportJobStatus.ACTIVE, ExportJobStatus.COMPLETED,
        )
        assert not is_valid_transition(
            "mark_completed", ExportJobStatus.FAILED, ExportJobStatus.COMPLETED,
        )

    def test_mark_failed_allowed_from_active_or_failed(self) -> None:
        assert is_valid_transition(
            "mark_failed", ExportJobStatus.ACTIVE, ExportJobStatus.FAILED,
        )
        assert is_valid_transition(
            "mark_failed", ExportJobStatus.FAILED, ExportJobStatus.FAILED,
        )

    def test_unknown_operation_rejected(self) -> None:
        assert not is_valid_transition(
            "frobnicate", ExportJobStatus.ACTIVE, ExportJobStatus.COMPLETED,
        )

    def test_assert_valid_raises_on_invalid_transition(self) -> None:
        with pytest.raises(ExportJobInvalidTransitionError) as excinfo:
            assert_valid_transition(
                "mark_completed",
                ExportJobStatus.FAILED,
                ExportJobStatus.COMPLETED,
                job_id="job-x",
            )
        ex = excinfo.value
        assert ex.operation == "mark_completed"
        assert ex.from_status == ExportJobStatus.FAILED.value
        assert ex.to_status == ExportJobStatus.COMPLETED.value
        assert ex.job_id == "job-x"

    def test_assert_valid_no_op_when_allowed(self) -> None:
        # Should not raise.
        assert_valid_transition(
            "create", None, ExportJobStatus.ACTIVE,
        )

    def test_matrix_is_self_consistent(self) -> None:
        for (op, frm), targets in TRANSITIONS.items():
            assert isinstance(op, str) and op
            for t in targets:
                assert isinstance(t, ExportJobStatus)
                assert is_valid_transition(op, frm, t)


class TestExceptions:
    def test_invalid_transition_is_value_error(self) -> None:
        err = ExportJobInvalidTransitionError(
            operation="create",
            from_status="Active",
            to_status="Active",
            job_id="j",
        )
        assert isinstance(err, ValueError)
        assert isinstance(err, ExportJobError)
        assert "Active" in str(err)
        assert "create" in str(err)
        assert err.job_id == "j"

    def test_not_found_is_lookup_error(self) -> None:
        err = ExportJobNotFoundError("j2")
        assert isinstance(err, LookupError)
        assert isinstance(err, ExportJobError)
        assert err.job_id == "j2"
        assert "j2" in str(err)

    def test_base_carries_job_id(self) -> None:
        err = ExportJobError("boom", job_id="abc")
        assert err.job_id == "abc"
