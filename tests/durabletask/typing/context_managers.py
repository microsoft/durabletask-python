# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Static regression checks for subclass-preserving instance return types."""

from typing import assert_type

from durabletask.azuremanaged.client import (
    AsyncDurableTaskSchedulerClient,
    DurableTaskSchedulerClient,
)
from durabletask.azuremanaged.preview.sandboxes import SandboxActivitiesClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from durabletask.entities import EntityInstanceId, EntityLock
from durabletask.extensions.azure_blob_payloads import BlobPayloadStore
from durabletask.extensions.history_export.azure_blob import AzureBlobHistoryExportWriter


class CustomEntityLock(EntityLock):
    pass


class CustomEntityId(EntityInstanceId):
    pass


class CustomPayloadStore(BlobPayloadStore):
    pass


class CustomHistoryWriter(AzureBlobHistoryExportWriter):
    pass


class CustomSandboxClient(SandboxActivitiesClient):
    pass


def check_sync_context_managers(
    client: DurableTaskSchedulerClient,
    worker: DurableTaskSchedulerWorker,
    entity_lock: CustomEntityLock,
    store: CustomPayloadStore,
    writer: CustomHistoryWriter,
    sandbox: CustomSandboxClient,
) -> None:
    with client as entered_client:
        assert_type(entered_client, DurableTaskSchedulerClient)
    with worker as entered_worker:
        assert_type(entered_worker, DurableTaskSchedulerWorker)
    with entity_lock as entered_lock:
        assert_type(entered_lock, CustomEntityLock)
    with store as entered_store:
        assert_type(entered_store, CustomPayloadStore)
    with writer as entered_writer:
        assert_type(entered_writer, CustomHistoryWriter)
    with sandbox as entered_sandbox:
        assert_type(entered_sandbox, CustomSandboxClient)


async def check_async_context_managers(
    client: AsyncDurableTaskSchedulerClient,
    store: CustomPayloadStore,
) -> None:
    async with client as entered_client:
        assert_type(entered_client, AsyncDurableTaskSchedulerClient)
    async with store as entered_store:
        assert_type(entered_store, CustomPayloadStore)


def check_entity_copy_types(entity_id: CustomEntityId) -> None:
    assert_type(entity_id.__copy__(), CustomEntityId)
    assert_type(entity_id.__deepcopy__({}), CustomEntityId)
    assert_type(entity_id.__reduce__(), tuple[type[CustomEntityId], tuple[str, str]])
