# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the durabletask-native activity adapter (``wrap_activity``)."""

from __future__ import annotations

import inspect
import asyncio
import json
import sys
import threading
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from azure.functions import meta
from azure.durable_functions.internal import invocation, payloads
from azure.durable_functions.internal.compat.activity import wrap_activity, wrap_activity_payloads
from azure.durable_functions.internal.converters import ActivityTriggerConverter


def test_one_param_activity_passes_through_unchanged():
    def act(x):
        return x

    assert wrap_activity(act, "x") is act


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("user_async", [False, True])
async def test_activity_wrappers_preserve_indexed_source_directory(monkeypatch, native, user_async):
    monkeypatch.setattr(payloads, "_payload_store", None)

    def activity(payload):
        return payload

    async def async_activity(payload):
        return payload

    def native_activity(context, payload):
        return payload

    async def async_native_activity(context, payload):
        return payload

    original = (async_native_activity if user_async else native_activity) if native else (
        async_activity if user_async else activity)
    adapted = wrap_activity(original, "payload")
    wrapped = wrap_activity_payloads(adapted, "payload")
    assert inspect.getfile(adapted) == inspect.getfile(original)
    assert inspect.getfile(wrapped) == inspect.getfile(original)
    assert inspect.iscoroutinefunction(wrapped) == user_async
    result = await wrapped("value") if user_async else wrapped("value")
    assert result == "value"


def test_two_param_activity_is_adapted_to_single_input():
    def act(ctx, payload):
        return (ctx, payload)

    adapted = wrap_activity(act, "payload")

    assert adapted is not act
    assert list(inspect.signature(adapted).parameters) == ["payload"]
    assert adapted.__name__ == "act"
    # A placeholder context is supplied; the input is passed through.
    ctx, payload = adapted("value")
    assert payload == "value"
    # Reading the placeholder context raises a clear error rather than an
    # opaque AttributeError on None.
    with pytest.raises(NotImplementedError, match="ActivityContext is not available"):
        _ = ctx.orchestration_id


def test_adapter_invokes_original_positionally_regardless_of_param_name():
    # The original's second parameter name differs from input_name; the adapter
    # still binds correctly because it calls the original positionally.
    def act(ctx, original_name):
        return original_name

    adapted = wrap_activity(act, "input")
    assert list(inspect.signature(adapted).parameters) == ["input"]
    assert adapted("hi") == "hi"


def test_adapter_sanitizes_parameterized_generic_annotations():
    def act(ctx, payload: Mapping[str, Any]) -> dict[str, Any]:
        return dict(payload)

    adapted = wrap_activity(act, "payload")
    # Parameterized generics (rejected by the worker indexer) are reduced to
    # concrete builtins.
    assert adapted.__annotations__ == {"payload": dict, "return": dict}


def test_native_activity_with_extra_binding_passes_through_unchanged():
    # A native Functions activity whose first positional parameter IS the
    # trigger input (== input_name) may declare additional host bindings (for
    # example a durable client) as further parameters. It must be left untouched
    # so the host binds each parameter by name, not adapted as a
    # durabletask-native ``(ctx, input)`` activity.
    def act(input, client):
        return (input, client)

    assert wrap_activity(act, "input") is act


def test_adapter_preserves_concrete_annotations():
    def act(ctx, name: str) -> str:
        return name

    adapted = wrap_activity(act, "name")
    assert adapted.__annotations__ == {"name": str, "return": str}


def test_adapter_without_annotations_has_none():
    def act(ctx, payload):
        return payload

    adapted = wrap_activity(act, "payload")
    assert adapted.__annotations__ == {}


def test_adapter_rejects_invalid_input_name():
    def act(ctx, payload):
        return payload

    with pytest.raises(ValueError, match="valid Python identifier"):
        wrap_activity(act, "not an identifier")
    with pytest.raises(ValueError, match="valid Python identifier"):
        wrap_activity(act, "class")  # a keyword


def test_activity_converters_do_not_access_storage(monkeypatch, payload_store_factory):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    monkeypatch.setattr(store, "download", Mock(side_effect=AssertionError("converter download")))
    monkeypatch.setattr(store, "upload", Mock(side_effect=AssertionError("converter upload")))
    value = json.dumps("blob:v1:test-container:missing")
    decoded = ActivityTriggerConverter.decode(meta.Datum(type="json", value=value), trigger_metadata=None)
    assert isinstance(decoded, payloads.ActivityPayload)
    assert decoded.value == value
    encoded = ActivityTriggerConverter.encode({"large": "x" * 200}, expected_type=None)
    assert json.loads(encoded.value) == {"large": "x" * 200}
    assert ActivityTriggerConverter.encode(decoded, expected_type=None).value == value


@pytest.mark.asyncio
@pytest.mark.parametrize("native", [False, True])
async def test_async_activity_awaits_storage_and_preserves_signature(
        monkeypatch, payload_store_factory, native):
    store = payload_store_factory()
    entered = asyncio.Event()
    release = asyncio.Event()
    value = {"large": "x" * 200}
    token = store.upload(json.dumps(value).encode())
    upload = store.upload

    async def download_async(reference):
        entered.set()
        await release.wait()
        return store._blobs[reference]

    async def upload_async(data, *, instance_id=None):
        return upload(data, instance_id=instance_id)

    monkeypatch.setattr(store, "download_async", AsyncMock(side_effect=download_async))
    monkeypatch.setattr(store, "upload_async", AsyncMock(side_effect=upload_async))
    monkeypatch.setattr(store, "download", Mock(side_effect=AssertionError("sync download")))
    monkeypatch.setattr(store, "upload", Mock(side_effect=AssertionError("sync upload")))
    monkeypatch.setattr(payloads, "_payload_store", None)

    async def activity(payload, client):
        assert client == "extra binding"
        assert payload == value
        return payload

    async def native_activity(context, payload):
        assert payload == value
        return payload

    adapted = wrap_activity(native_activity if native else activity, "payload")
    wrapper = wrap_activity_payloads(adapted, "payload")
    assert inspect.iscoroutinefunction(wrapper)
    assert inspect.signature(wrapper) == inspect.signature(adapted)
    monkeypatch.setattr(payloads, "_payload_store", store)
    decoded = ActivityTriggerConverter.decode(
        meta.Datum(type="json", value=json.dumps(token)), trigger_metadata=None)
    kwargs = {} if native else {"client": "extra binding"}
    async with asyncio.timeout(5):
        pending = asyncio.create_task(wrapper(payload=decoded, **kwargs))
        await entered.wait()
        assert not pending.done()
        release.set()
        result = await pending
    encoded = ActivityTriggerConverter.encode(result, expected_type=None)
    assert json.loads(store._blobs[json.loads(encoded.value)]) == value
    store.download_async.assert_awaited_once()
    store.upload_async.assert_awaited_once()


@pytest.mark.parametrize("user_async", [False, True])
@pytest.mark.parametrize("value", [None, "small", {"small": True}])
async def test_host_activity_externalizes_large_output_from_inline_input(
        monkeypatch, payload_store_factory, user_async, value):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    output = {"large": "x" * 200}

    def activity(payload):
        assert payload == value
        return output

    async def async_activity(payload):
        return activity(payload)

    wrapper = wrap_activity_payloads(async_activity if user_async else activity, "payload")
    decoded = ActivityTriggerConverter.decode(
        meta.Datum(type="json", value=json.dumps(value)), trigger_metadata=None)
    result = await wrapper(decoded) if user_async else wrapper(decoded)
    encoded = ActivityTriggerConverter.encode(result, expected_type=None)
    assert json.loads(store.download(json.loads(encoded.value))) == output


def test_sync_activity_uses_sync_storage_on_the_calling_thread(monkeypatch, payload_store_factory):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    value = {"large": "x" * 200}
    token = store.upload(json.dumps(value).encode())
    thread_ids = []
    original_download = store.download
    original_upload = store.upload

    def download(reference):
        thread_ids.append(threading.get_ident())
        return original_download(reference)

    def upload(data, *, instance_id=None):
        thread_ids.append(threading.get_ident())
        return original_upload(data, instance_id=instance_id)

    def activity(payload):
        thread_ids.append(threading.get_ident())
        assert payload == value
        return payload

    monkeypatch.setattr(store, "download", Mock(side_effect=download))
    monkeypatch.setattr(store, "upload", Mock(side_effect=upload))
    monkeypatch.setattr(store, "download_async", AsyncMock(side_effect=AssertionError("async download")))
    monkeypatch.setattr(store, "upload_async", AsyncMock(side_effect=AssertionError("async upload")))
    wrapper = wrap_activity_payloads(activity, "payload")
    assert not inspect.iscoroutinefunction(wrapper)
    decoded = ActivityTriggerConverter.decode(
        meta.Datum(type="string", value=token), trigger_metadata=None)
    result = wrapper(decoded)
    encoded = ActivityTriggerConverter.encode(result, expected_type=None)
    assert json.loads(original_download(json.loads(encoded.value))) == value
    assert thread_ids == [threading.get_ident()] * 3
    store.download.assert_called_once_with(token)
    store.upload.assert_called_once()
    store.download_async.assert_not_called()
    store.upload_async.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("operation", ["download", "upload"])
async def test_activity_storage_errors_propagate_without_retry(
        monkeypatch, payload_store_factory, use_async, operation):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    token = store.upload(json.dumps("x" * 200).encode())
    error = OSError("payload storage unavailable")
    failure = AsyncMock(side_effect=error) if use_async else Mock(side_effect=error)
    monkeypatch.setattr(store, operation + ("_async" if use_async else ""), failure)
    called = []

    def activity(payload):
        called.append(True)
        return payload

    async def async_activity(payload):
        return activity(payload)

    wrapper = wrap_activity_payloads(async_activity if use_async else activity, "payload")
    decoded = ActivityTriggerConverter.decode(
        meta.Datum(type="string", value=token), trigger_metadata=None)
    with pytest.raises(OSError) as raised:
        if use_async:
            await wrapper(decoded)
        else:
            wrapper(decoded)
    assert raised.value is error
    assert called == ([True] if operation == "upload" else [])
    if use_async:
        failure.assert_awaited_once()
    else:
        failure.assert_called_once()


@pytest.mark.parametrize("setting, expected", [(None, None), ("2", 2), ("invalid", None), ("0", None)])
def test_sync_executor_honors_functions_thread_count(monkeypatch, setting, expected):
    if setting is None:
        monkeypatch.delenv("PYTHON_THREADPOOL_THREAD_COUNT", raising=False)
    else:
        monkeypatch.setenv("PYTHON_THREADPOOL_THREAD_COUNT", setting)
    factory = Mock()
    monkeypatch.setattr(invocation, "ThreadPoolExecutor", factory)
    invocation._fallback_executor.__wrapped__()
    factory.assert_called_once_with(max_workers=expected, thread_name_prefix="durable-functions")


async def test_sync_execution_reuses_runtime_pool_and_resets_invocation_context(monkeypatch):
    runtime = ModuleType("azure_functions_runtime")
    host_invocation_id = ContextVar("host_invocation_id", default=None)
    setattr(runtime, "invocation_id_cv", host_invocation_id)
    monkeypatch.setitem(sys.modules, "azure_functions_runtime", runtime)
    storage = threading.local()
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=1) as executor:
        setattr(runtime, "get_threadpool_executor", lambda: executor)
        expected_thread = await loop.run_in_executor(executor, threading.get_ident)
        await loop.run_in_executor(executor, setattr, storage, "invocation_id", "previous")

        def execute(payload):
            assert threading.get_ident() == expected_thread
            assert storage.invocation_id == host_invocation_id.get() == payload
            if payload == "failure":
                raise ValueError("user failure")
            return payload

        async def handle(payload):
            return await invocation.run_sync(execute, payload)

        wrapper = invocation.wrap_invocation(handle, "payload")
        for identifier in ("first", "failure", "second"):
            context = SimpleNamespace(invocation_id=identifier, thread_local_storage=storage)
            if identifier == "failure":
                with pytest.raises(ValueError, match="user failure"):
                    await wrapper(identifier, context=context)
            else:
                assert await wrapper(identifier, context=context) == identifier
            assert await loop.run_in_executor(executor, host_invocation_id.get) is None
            assert await loop.run_in_executor(executor, getattr, storage, "invocation_id") == "previous"
            assert invocation._invocation_context.get() is None
