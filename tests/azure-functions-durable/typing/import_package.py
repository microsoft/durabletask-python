# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from collections.abc import Coroutine, Generator
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


async def check_call_http(
    native: task.AsyncOrchestrationContext,
    eager: task.OrchestrationContext,
    context: df.DurableOrchestrationContext,
) -> None:
    retry = df.HttpRetryOptions(first_retry_interval=timedelta(seconds=1), max_number_of_attempts=3)
    pending = assert_type(
        df.call_http(native, "GET", "https://example.com", retry_options=retry),
        Coroutine[Any, Any, df.DurableHttpResponse])
    assert_type(await pending, df.DurableHttpResponse)
    assert_type(df.call_http(eager, "GET", "https://example.com"), task.Task[df.DurableHttpResponse])
    assert_type(context.call_http("GET", "https://example.com"), task.Task[Any])


def register_orchestrators(app: df.DFApp, worker: DurableFunctionsWorker) -> None:
    def generator(context: df.DurableOrchestrationContext) -> Generator[task.Task[Any], Any, int]:
        return (yield context.call_activity("activity"))

    async def native(context: task.AsyncOrchestrationContext, value: int) -> int:
        return await context.call_activity("activity", input=value, return_type=int)

    assert_type(app.orchestration_trigger("context")(generator), FunctionBuilder)
    assert_type(app.orchestration_trigger("context")(native), FunctionBuilder)
    assert_type(worker.execute_orchestration_request(generator, ""), str)
    assert_type(worker.execute_orchestration_request(native, ""), str)
