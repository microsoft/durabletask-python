# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for per-invocation durable-client lifecycle cleanup.

Each ``durable_client_input`` decode opens a distinct async gRPC channel; the
app-level post-invocation extension closes it so invocations do not leak
channels. ``grpc.aio`` channels can only be created while an event loop is
available, so the clients here are built inside a loop.
"""

from __future__ import annotations

import asyncio
import logging

from azure.durable_functions.client import DurableFunctionsClient
from azure.durable_functions.internal.lifecycle import (
    _DurableClientLifecycleExtension,
)


def _make_client_in_loop(loop: asyncio.AbstractEventLoop) -> DurableFunctionsClient:
    async def _make() -> DurableFunctionsClient:
        return DurableFunctionsClient("{}")

    return loop.run_until_complete(_make())


def test_schedule_close_closes_channel_on_owning_loop():
    loop = asyncio.new_event_loop()
    try:
        client = _make_client_in_loop(loop)
        real_close = client.close
        closes = {"n": 0}

        async def _tracked_close() -> None:
            closes["n"] += 1
            await real_close()

        client.close = _tracked_close  # type: ignore[method-assign]

        client.schedule_close()
        client.schedule_close()  # idempotent: schedules at most once

        loop.run_until_complete(asyncio.sleep(0.05))
        assert closes["n"] == 1
    finally:
        loop.close()


def test_post_invocation_closes_only_durable_clients():
    loop = asyncio.new_event_loop()
    try:
        client = _make_client_in_loop(loop)
        scheduled = {"n": 0}
        client.schedule_close = lambda: scheduled.__setitem__("n", scheduled["n"] + 1)  # type: ignore[method-assign]

        _DurableClientLifecycleExtension.post_invocation_app_level(
            logging.getLogger(),
            None,  # type: ignore[arg-type]
            func_args={"client": client, "req": object(), "count": 5},
            func_ret=None,
        )
        assert scheduled["n"] == 1

        # Missing/empty func_args must be handled without error.
        _DurableClientLifecycleExtension.post_invocation_app_level(
            logging.getLogger(), None, func_args=None, func_ret=None)  # type: ignore[arg-type]
    finally:
        loop.run_until_complete(client.close())
        loop.close()


def test_extension_is_registered_at_import():
    from azure.functions.extension.extension_meta import ExtensionMeta

    info = ExtensionMeta.get_registered_extensions_json()
    assert "_DurableClientLifecycleExtension" in info
