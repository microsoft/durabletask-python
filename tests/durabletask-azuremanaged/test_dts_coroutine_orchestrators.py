# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Exercise both workers over gRPC and optionally against the DTS emulator."""

import json
import os
import socket
from collections.abc import Generator
from datetime import timedelta
from typing import Any

import pytest

from durabletask import client, task, worker
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from durabletask.testing import create_test_backend


@pytest.mark.parametrize("transport", [
    "core",
    "azuremanaged",
    pytest.param("dts", marks=pytest.mark.dts),
])
@pytest.mark.parametrize("coroutine_parent", [False, True], ids=["generator-parent", "coroutine-parent"])
def test_mixed_orchestrators_over_grpc(transport: str, coroutine_parent: bool) -> None:
    def increment(ctx: task.ActivityContext, value: int) -> int:
        return value + 1

    async def coroutine_child(ctx: task.OrchestrationContext, value: int) -> int:
        await ctx.create_timer(timedelta(milliseconds=10))
        return await ctx.call_activity(increment, input=value, return_type=int)

    def generator_child(ctx: task.OrchestrationContext, value: int) -> Generator[task.Task[Any], Any, int]:
        return (yield ctx.call_activity(increment, input=value))

    async def coroutine(ctx: task.OrchestrationContext, value: int) -> list[Any]:
        values = await task.when_all([
            ctx.call_sub_orchestrator(coroutine_child, input=value),
            ctx.call_sub_orchestrator(generator_child, input=value + 1),
        ])
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
        host = os.environ["ENDPOINT"]
        taskhub = os.environ["TASKHUB"]
    else:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        backend = create_test_backend(port=port)
        host = f"localhost:{port}"
        taskhub = "test"
    if transport != "core":
        w = DurableTaskSchedulerWorker(
            host_address=host, secure_channel=False, taskhub=taskhub, token_credential=None)
        c = DurableTaskSchedulerClient(
            host_address=host, secure_channel=False, taskhub=taskhub, token_credential=None)
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
