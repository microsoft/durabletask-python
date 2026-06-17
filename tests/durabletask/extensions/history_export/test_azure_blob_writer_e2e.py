# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""End-to-end tests for :class:`AzureBlobHistoryExportWriter` using Azurite.

Prerequisites:
    - Azurite must be running locally on the default blob port (10000).
      Start it with:  ``azurite --silent --blobPort 10000``
    - ``azure-storage-blob`` must be installed.
"""

from __future__ import annotations

import gzip
import json
import uuid

import pytest

# Skip the entire module if azure-storage-blob is not installed.
azure_blob = pytest.importorskip("azure.storage.blob")

from durabletask.extensions.history_export import (  # noqa: E402
    ExportFormat,
    ExportFormatKind,
)
from durabletask.extensions.history_export.azure_blob import (  # noqa: E402
    AzureBlobHistoryExportWriter,
    AzureBlobHistoryExportWriterOptions,
)
from durabletask.extensions.history_export.serialization import (  # noqa: E402
    content_encoding_for,
    content_type_for,
    serialize_history,
)


AZURITE_CONN_STR = "UseDevelopmentStorage=true"
AZURITE_API_VERSION = "2024-08-04"
TEST_CONTAINER = f"e2e-history-export-{uuid.uuid4().hex[:8]}"


def _azurite_is_running() -> bool:
    try:
        svc = azure_blob.BlobServiceClient.from_connection_string(
            AZURITE_CONN_STR, api_version=AZURITE_API_VERSION,
        )
        next(iter(svc.list_containers(results_per_page=1)), None)
        return True
    except Exception:
        return False


pytestmark = [
    pytest.mark.azurite,
    pytest.mark.skipif(
        not _azurite_is_running(),
        reason="Azurite blob service is not running on 127.0.0.1:10000",
    ),
]


@pytest.fixture(scope="module")
def writer():
    w = AzureBlobHistoryExportWriter(
        AzureBlobHistoryExportWriterOptions(
            container_name=TEST_CONTAINER,
            connection_string=AZURITE_CONN_STR,
            api_version=AZURITE_API_VERSION,
        )
    )
    yield w
    w.close()
    try:
        svc = azure_blob.BlobServiceClient.from_connection_string(
            AZURITE_CONN_STR, api_version=AZURITE_API_VERSION,
        )
        svc.delete_container(TEST_CONTAINER)
    except Exception:
        pass


def test_options_validate_required_fields():
    with pytest.raises(ValueError):
        AzureBlobHistoryExportWriterOptions(container_name="")
    with pytest.raises(ValueError):
        AzureBlobHistoryExportWriterOptions(container_name="c")


def test_write_json_blob(writer):
    fmt = ExportFormat(kind=ExportFormatKind.JSON)
    payload = serialize_history([], instance_id="inst-json", fmt=fmt)
    blob_name = f"json/{uuid.uuid4().hex}.json"
    writer.write(
        instance_id="inst-json",
        container=TEST_CONTAINER,
        blob_name=blob_name,
        payload=payload,
        content_type=content_type_for(fmt),
        content_encoding=content_encoding_for(fmt),
    )

    svc = azure_blob.BlobServiceClient.from_connection_string(
        AZURITE_CONN_STR, api_version=AZURITE_API_VERSION,
    )
    container = svc.get_container_client(TEST_CONTAINER)
    blob = container.get_blob_client(blob_name)
    props = blob.get_blob_properties()
    assert props.content_settings.content_type == "application/json"
    downloaded = blob.download_blob().readall()
    doc = json.loads(downloaded)
    assert doc["instance_id"] == "inst-json"
    assert doc["events"] == []


def test_write_jsonl_gzip_blob(writer):
    fmt = ExportFormat(kind=ExportFormatKind.JSONL_GZIP)
    payload = serialize_history([], instance_id="inst-gz", fmt=fmt)
    blob_name = f"gz/{uuid.uuid4().hex}.jsonl.gz"
    writer.write(
        instance_id="inst-gz",
        container=TEST_CONTAINER,
        blob_name=blob_name,
        payload=payload,
        content_type=content_type_for(fmt),
        content_encoding=content_encoding_for(fmt),
    )

    svc = azure_blob.BlobServiceClient.from_connection_string(
        AZURITE_CONN_STR, api_version=AZURITE_API_VERSION,
    )
    container = svc.get_container_client(TEST_CONTAINER)
    blob = container.get_blob_client(blob_name)
    props = blob.get_blob_properties()
    assert props.content_settings.content_type == "application/x-ndjson"
    assert props.content_settings.content_encoding == "gzip"
    downloaded = blob.download_blob().readall()
    # The Azure SDK may auto-decompress when Content-Encoding: gzip is set;
    # accept either the gzipped bytes or the decoded text.
    if downloaded[:2] == b"\x1f\x8b":
        text = gzip.decompress(downloaded).decode("utf-8")
    else:
        text = downloaded.decode("utf-8")
    first = json.loads(text.strip().split("\n")[0])
    assert first["instance_id"] == "inst-gz"
    assert first["kind"] == "metadata"
