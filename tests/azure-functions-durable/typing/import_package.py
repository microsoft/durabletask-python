# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from datetime import timedelta
from typing import assert_type

import azure.durable_functions as df
from azure.durable_functions.client import DurableFunctionsClient, SyncDurableFunctionsClient
from azure.durable_functions.worker import DurableFunctionsWorker


def create_app() -> df.DFApp:
    app = df.DFApp()
    app.configure_serialization(ensure_ascii=True)
    return app


def create_retry_policy() -> df.RetryPolicy:
    return df.RetryPolicy(
        first_retry_interval=timedelta(seconds=1),
        max_number_of_attempts=3,
    )


def activity_is_complete(context: df.DurableOrchestrationContext) -> bool:
    return context.call_activity("activity").is_complete


def check_sync_context_managers(
    client: SyncDurableFunctionsClient,
    worker: DurableFunctionsWorker,
) -> None:
    with client as entered_client:
        assert_type(entered_client, SyncDurableFunctionsClient)
    with worker as entered_worker:
        assert_type(entered_worker, DurableFunctionsWorker)


async def check_async_context_manager(client: DurableFunctionsClient) -> None:
    async with client as entered_client:
        assert_type(entered_client, DurableFunctionsClient)
