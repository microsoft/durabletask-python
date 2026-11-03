# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import asyncio
from datetime import timedelta
from typing import assert_type

import azure.durable_functions as df
from azure.durable_functions.client import DurableFunctionsClient, SyncDurableFunctionsClient
from azure.durable_functions.worker import DurableFunctionsWorker
from durabletask import task


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


async def check_cold_operations(
    context: df.DurableOrchestrationContext,
    native: task.OrchestrationContext,
) -> None:
    ops = context.as_async()
    assert_type(ops, task.AsyncOrchestrationContext)
    assert_type(native.as_async(), task.AsyncOrchestrationContext)
    pending = asyncio.create_task(ops.call_activity("activity", return_type=int))
    assert_type(pending, asyncio.Task[int])
    assert_type(await pending, int)
    event = asyncio.create_task(ops.receive_event(
        "approval", timeout=timedelta(seconds=1), data_type=str))
    assert_type(await event, str)
    operation = native.call_activity("shared", return_type=int)
    borrower = asyncio.create_task(operation.wait())
    assert_type(borrower, asyncio.Task[int])
