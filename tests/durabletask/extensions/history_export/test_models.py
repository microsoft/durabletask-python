# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the history-export public data models."""

from datetime import datetime, timezone, timedelta

import pytest

from durabletask.client import OrchestrationStatus
from durabletask.extensions.history_export import (
    ExportCheckpoint,
    ExportDestination,
    ExportFailure,
    ExportFilter,
    ExportFormat,
    ExportFormatKind,
    ExportJobConfiguration,
    ExportJobCreationOptions,
    ExportJobDescription,
    ExportJobStatus,
    ExportMode,
)
from durabletask.extensions.history_export.models import (
    STATE_SCHEMA_VERSION,
    ExportJobState,
)


_WINDOW_START = datetime(2025, 1, 1, tzinfo=timezone.utc)
_WINDOW_END = datetime(2025, 1, 2, tzinfo=timezone.utc)


def _basic_destination() -> ExportDestination:
    return ExportDestination(container="exports", prefix="run-1")


def _basic_options() -> ExportJobCreationOptions:
    return ExportJobCreationOptions(
        mode=ExportMode.BATCH,
        completed_time_from=_WINDOW_START,
        completed_time_to=_WINDOW_END,
        destination=_basic_destination(),
    )


class TestDestination:
    def test_requires_container(self) -> None:
        with pytest.raises(ValueError):
            ExportDestination(container="")


class TestConfigurationValidation:
    def test_batch_requires_completed_time_to(self) -> None:
        with pytest.raises(ValueError, match="completed_time_to"):
            ExportJobConfiguration(
                mode=ExportMode.BATCH,
                filter=ExportFilter(completed_time_from=_WINDOW_START),
                destination=_basic_destination(),
            )

    def test_max_instances_per_batch_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="max_instances_per_batch"):
            ExportJobConfiguration(
                mode=ExportMode.BATCH,
                filter=ExportFilter(
                    completed_time_from=_WINDOW_START,
                    completed_time_to=_WINDOW_END,
                ),
                destination=_basic_destination(),
                max_instances_per_batch=0,
            )

    def test_valid_config(self) -> None:
        cfg = _basic_options().to_configuration()
        assert cfg.mode is ExportMode.BATCH
        assert cfg.filter.completed_time_to == _WINDOW_END
        assert cfg.format.kind is ExportFormatKind.JSONL_GZIP
        assert cfg.format.schema_version == "1.0"
        assert cfg.max_instances_per_batch == 100
        assert cfg.max_parallel_exports == 32

    def test_max_parallel_exports_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="max_parallel_exports"):
            ExportJobConfiguration(
                mode=ExportMode.BATCH,
                filter=ExportFilter(
                    completed_time_from=_WINDOW_START,
                    completed_time_to=_WINDOW_END,
                ),
                destination=_basic_destination(),
                max_parallel_exports=0,
            )


class TestFilterDefaults:
    def test_default_runtime_statuses(self) -> None:
        f = ExportFilter(completed_time_from=_WINDOW_START, completed_time_to=_WINDOW_END)
        statuses = f.effective_runtime_status()
        assert OrchestrationStatus.COMPLETED in statuses
        assert OrchestrationStatus.FAILED in statuses
        assert OrchestrationStatus.TERMINATED in statuses
        assert OrchestrationStatus.RUNNING not in statuses

    def test_explicit_runtime_statuses(self) -> None:
        f = ExportFilter(
            completed_time_from=_WINDOW_START,
            completed_time_to=_WINDOW_END,
            runtime_status=[OrchestrationStatus.COMPLETED],
        )
        assert f.effective_runtime_status() == [OrchestrationStatus.COMPLETED]


class TestRoundTrip:
    def test_configuration_round_trip(self) -> None:
        cfg = ExportJobConfiguration(
            mode=ExportMode.BATCH,
            filter=ExportFilter(
                completed_time_from=_WINDOW_START,
                completed_time_to=_WINDOW_END,
                runtime_status=[OrchestrationStatus.COMPLETED, OrchestrationStatus.FAILED],
            ),
            destination=ExportDestination(container="c", prefix="p"),
            format=ExportFormat(kind=ExportFormatKind.JSON, schema_version="1.0"),
            max_instances_per_batch=25,
        )
        restored = ExportJobConfiguration.from_dict(cfg.to_dict())
        assert restored == cfg

    def test_checkpoint_round_trip(self) -> None:
        cp = ExportCheckpoint(last_instance_key="abc|xyz")
        assert ExportCheckpoint.from_dict(cp.to_dict()) == cp

    def test_failure_round_trip(self) -> None:
        f = ExportFailure(
            instance_id="i1",
            reason="boom",
            attempt_count=3,
            last_attempt=_WINDOW_END,
        )
        assert ExportFailure.from_dict(f.to_dict()) == f

    def test_naive_datetimes_are_treated_as_utc(self) -> None:
        naive = datetime(2025, 1, 1, 12, 0, 0)
        f = ExportFilter(completed_time_from=naive, completed_time_to=_WINDOW_END)
        d = f.to_dict()
        restored = ExportFilter.from_dict(d)
        assert restored.completed_time_from == naive.replace(tzinfo=timezone.utc)


class TestDescriptionFromState:
    def test_from_new_state(self) -> None:
        cfg = _basic_options().to_configuration()
        created = _WINDOW_END + timedelta(minutes=5)
        state = ExportJobState.new(
            cfg,
            created_at=created,
            orchestrator_instance_id="orch-1",
        )
        desc = ExportJobDescription.from_state_dict("job-1", state.to_dict())

        assert desc.job_id == "job-1"
        assert desc.status is ExportJobStatus.ACTIVE
        assert desc.created_at == created
        assert desc.last_modified_at == created
        assert desc.orchestrator_instance_id == "orch-1"
        assert desc.scanned_instances == 0
        assert desc.exported_instances == 0
        assert desc.failed_instances == 0
        assert desc.last_error is None
        assert desc.config is not None
        assert desc.config.mode is ExportMode.BATCH
        assert desc.checkpoint is not None
        assert desc.checkpoint.last_instance_key is None
        assert desc.failures == []


class TestExportJobStateRoundTrip:
    def test_state_round_trip_preserves_schema_version(self) -> None:
        cfg = _basic_options().to_configuration()
        created = _WINDOW_END
        state = ExportJobState.new(cfg, created_at=created)
        d = state.to_dict()
        assert d["schema_version"] == STATE_SCHEMA_VERSION
        assert "__class__" not in d  # no Python type metadata
        assert "__type__" not in d
        restored = ExportJobState.from_dict(d)
        assert restored == state

    def test_unknown_schema_version_is_rejected(self) -> None:
        cfg = _basic_options().to_configuration()
        state = ExportJobState.new(cfg, created_at=_WINDOW_END)
        bad = state.to_dict()
        bad["schema_version"] = "99.0"
        with pytest.raises(ValueError, match="schema_version"):
            ExportJobState.from_dict(bad)

    def test_state_carries_failures(self) -> None:
        cfg = _basic_options().to_configuration()
        f = ExportFailure(
            instance_id="i",
            reason="r",
            attempt_count=1,
            last_attempt=_WINDOW_END,
        )
        state = ExportJobState.new(cfg, created_at=_WINDOW_END)
        state.failures.append(f)
        restored = ExportJobState.from_dict(state.to_dict())
        assert restored.failures == [f]
