# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Exercise both workers over gRPC and optionally against the DTS emulator."""

import asyncio
import importlib.util
import json
import os
from collections.abc import Callable, Generator
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from durabletask import client, task, worker
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from durabletask.testing import create_test_backend


def _load_find_free_port() -> Callable[[], int]:
    # pytest imports this hyphenated directory's modules top-level, so the shared
    # core test helper cannot be imported through the `tests` package here.
    path = Path(__file__).resolve().parents[1] / "durabletask" / "_port_utils.py"
    spec = importlib.util.spec_from_file_location("_durabletask_test_port_utils", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return cast(Callable[[], int], module.find_free_port)


find_free_port = _load_find_free_port()
taskhub_name = os.getenv("TASKHUB", "default")
endpoint = os.getenv("ENDPOINT", "http://localhost:8080")


def _get_credential():
    """Returns DefaultAzureCredential if endpoint is https, otherwise None (for emulator)."""
    if endpoint.startswith("https://"):
        from azure.identity import DefaultAzureCredential
        return DefaultAzureCredential()
    return None


@pytest.mark.parametrize("transport", [
    "core",
    "azuremanaged",
    pytest.param("dts", marks=pytest.mark.dts),
])
@pytest.mark.parametrize("coroutine_parent", [False, True], ids=["generator-parent", "coroutine-parent"])
@pytest.mark.parametrize("borrowed", [False, True], ids=["coroutines", "borrowed-handles"])
def test_mixed_orchestrators_over_grpc(transport: str, coroutine_parent: bool, borrowed: bool) -> None:
    def increment(ctx: task.ActivityContext, value: int) -> int:
        return value + 1

    async def coroutine_child(ctx: task.AsyncOrchestrationContext, value: int) -> int:
        if not borrowed:
            await ctx.create_timer(timedelta(milliseconds=10))
            return await ctx.call_activity(increment, input=value, return_type=int)
        await asyncio.sleep(0)
        await asyncio.sleep(0.01)
        return await ctx.as_task_context().call_activity(increment, input=value, return_type=int)

    def generator_child(ctx: task.OrchestrationContext, value: int) -> Generator[task.Task[Any], Any, int]:
        return (yield ctx.call_activity(increment, input=value))

    async def coroutine(ctx: task.AsyncOrchestrationContext, value: int) -> list[Any]:
        if not borrowed:
            async with asyncio.TaskGroup() as group:
                first = group.create_task(ctx.call_sub_orchestrator(coroutine_child, input=value))
                second = group.create_task(ctx.call_sub_orchestrator(generator_child, input=value + 1))
            return [first.result(), second.result(), await ctx.wait_for_external_event("go")]
        handles = ctx.as_task_context()
        values = list(await asyncio.gather(
            handles.call_sub_orchestrator(coroutine_child, input=value),
            handles.call_sub_orchestrator(generator_child, input=value + 1),
        ))
        event = await ctx.wait_for_external_event("go")
        return values + [event]

    def generator(ctx: task.OrchestrationContext, value: int) -> Generator[task.Task[Any], Any, list[Any]]:
        values = yield task.when_all([
            ctx.call_sub_orchestrator(coroutine_child, input=value),
            ctx.call_sub_orchestrator(generator_child, input=value + 1),
        ])
        event = yield ctx.wait_for_external_event("go")
        return values + [event]

    if transport == "dts":
        backend = None
        host = endpoint
        taskhub = taskhub_name
        credential = _get_credential()
    else:
        port = find_free_port()
        backend = create_test_backend(port=port)
        # The in-memory backend has no TLS; the http:// scheme selects an insecure channel.
        host = f"http://localhost:{port}"
        taskhub = "test"
        credential = None
    if transport != "core":
        w = DurableTaskSchedulerWorker(
            host_address=host, secure_channel=True, taskhub=taskhub, token_credential=credential)
        c = DurableTaskSchedulerClient(
            host_address=host, secure_channel=True, taskhub=taskhub, token_credential=credential)
    else:
        w = worker.TaskHubGrpcWorker(host_address=host)
        c = client.TaskHubGrpcClient(host_address=host)
    try:
        with w, c:
            parent = coroutine if coroutine_parent else generator
            for fn in (parent, coroutine_child, generator_child):
                w.add_orchestrator(fn)
            w.add_activity(increment)
            w.start()
            instance_id = c.schedule_new_orchestration(parent, input=3)
            c.raise_orchestration_event(instance_id, "go", data="approved")
            state = c.wait_for_orchestration_completion(instance_id, timeout=15)
            assert state is not None
            assert state.runtime_status == client.OrchestrationStatus.COMPLETED, state.failure_details
            assert state.serialized_output is not None
            assert json.loads(state.serialized_output) == [4, 5, "approved"]
    finally:
        if backend is not None:
            backend.stop()
            backend.reset()
