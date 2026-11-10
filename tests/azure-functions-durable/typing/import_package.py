# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import asyncio
from collections.abc import Generator
from datetime import timedelta
from typing import Any, assert_type

import azure.durable_functions as df
from azure.durable_functions.client import DurableFunctionsClient, SyncDurableFunctionsClient
from azure.durable_functions.worker import DurableFunctionsWorker
from azure.functions.decorators.function_app import FunctionBuilder
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
    context: df.AsyncDurableOrchestrationContext,
    native: task.AsyncOrchestrationContext,
) -> None:
    pending = asyncio.create_task(native.call_activity("activity", return_type=int))
    assert_type(pending, asyncio.Task[int])
    assert_type(await pending, int)
    event = asyncio.create_task(native.wait_for_external_event(
        "approval", timeout=timedelta(seconds=1), data_type=str))
    assert_type(await event, str)
    assert_type(await context.wait_for_external_event("approval", str), str)
    assert_type(context.as_task_context(), df.DurableOrchestrationContext)
    assert_type(await context.task_all([pending, pending]), list[int])
    assert_type(await context.task_any([pending]), asyncio.Future[int])
    operation = native.as_task_context().call_activity("shared", return_type=int)
    borrower = asyncio.create_task(operation.wait())
    assert_type(borrower, asyncio.Task[int])


def register_orchestrators(app: df.DFApp, worker: DurableFunctionsWorker) -> None:
    def generator(context: df.DurableOrchestrationContext) -> Generator[task.Task[Any], Any, int]:
        return (yield context.call_activity("activity"))

    async def compatible(context: df.AsyncDurableOrchestrationContext) -> int:
        return await context.call_activity("activity")

    async def native(context: task.AsyncOrchestrationContext, value: int) -> int:
        return await context.call_activity("activity", input=value, return_type=int)

    assert_type(app.orchestration_trigger("context")(generator), FunctionBuilder)
    assert_type(app.orchestration_trigger("context")(compatible), FunctionBuilder)
    assert_type(app.orchestration_trigger("context")(native), FunctionBuilder)
    assert_type(worker.execute_orchestration_request(generator, ""), str)
    assert_type(worker.execute_orchestration_request(compatible, ""), str)
    assert_type(worker.execute_orchestration_request(native, ""), str)
