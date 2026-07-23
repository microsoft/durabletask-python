# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the Azure Functions history-export client.

The Functions ``ExportHistoryClient`` rejects ``ExportMode.CONTINUOUS`` (which
the host cannot tail safely) and otherwise passes through to the core client.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from azure.durable_functions.extensions.history_export import (
    ExportDestination,
    ExportHistoryClient,
    ExportJobCreationOptions,
    ExportMode,
)

_FROM = datetime(2026, 1, 1, tzinfo=timezone.utc)
_TO = datetime(2026, 2, 1, tzinfo=timezone.utc)


def _client() -> tuple[ExportHistoryClient, MagicMock]:
    sync_client = MagicMock()
    return ExportHistoryClient(sync_client, MagicMock()), sync_client


def test_create_job_rejects_continuous_mode():
    client, sync_client = _client()
    options = ExportJobCreationOptions(
        mode=ExportMode.CONTINUOUS,
        completed_time_from=_FROM,
        destination=ExportDestination(container="exports"))

    with pytest.raises(ValueError, match="Continuous history export is not supported"):
        client.create_job(options)
    # Rejected before any backend call.
    sync_client.signal_entity.assert_not_called()


def test_create_job_allows_batch_mode():
    client, sync_client = _client()
    options = ExportJobCreationOptions(
        mode=ExportMode.BATCH,
        completed_time_from=_FROM,
        completed_time_to=_TO,
        destination=ExportDestination(container="exports"))

    desc = client.create_job(options, job_id="job-1")

    assert desc.job_id == "job-1"
    # Passed through to the core client, which signals the export entity.
    sync_client.signal_entity.assert_called_once()
