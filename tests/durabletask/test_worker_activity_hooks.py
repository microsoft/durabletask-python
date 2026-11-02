# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import asyncio

from google.protobuf import wrappers_pb2

import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask.worker import TaskHubGrpcWorker


class RecordingStub:
    def __init__(self):
        self.completed = []

    def CompleteActivityTask(self, response):
        self.completed.append(response)


class HookedWorker(TaskHubGrpcWorker):
    def __init__(self):
        super().__init__()
        self.events = []

    def _on_activity_execution_started(self, req):
        self.events.append(("started", req.name))

    def _on_activity_execution_completed(self, req):
        self.events.append(("completed", req.name))


def test_activity_execution_hooks_run_around_successful_activity() -> None:
    def echo(_ctx, value):
        return value

    worker = HookedWorker()
    worker.add_activity(echo)
    stub = RecordingStub()
    request = _activity_request("echo", "hello")

    worker._execute_activity(request, stub, b"token")

    assert worker.events == [("started", "echo"), ("completed", "echo")]
    assert len(stub.completed) == 1
    assert stub.completed[0].result.value == "\"hello\""


def test_activity_execution_completed_hook_runs_after_failed_activity() -> None:
    def fails(_ctx, _value):
        raise ValueError("boom")

    worker = HookedWorker()
    worker.add_activity(fails)
    stub = RecordingStub()
    request = _activity_request("fails", "hello")

    worker._execute_activity(request, stub, b"token")

    assert worker.events == [("started", "fails"), ("completed", "fails")]
    assert len(stub.completed) == 1
    assert stub.completed[0].failureDetails.errorType == "builtins.ValueError"


def test_activity_execution_hooks_run_around_successful_async_activity() -> None:
    async def echo(_ctx, value):
        await asyncio.sleep(0)
        return value

    worker = HookedWorker()
    worker.add_activity(echo)
    stub = RecordingStub()
    request = _activity_request("echo", "hello")

    asyncio.run(worker._execute_activity_async(request, stub, b"token"))

    assert worker.events == [("started", "echo"), ("completed", "echo")]
    assert len(stub.completed) == 1
    assert stub.completed[0].result.value == "\"hello\""


def test_activity_execution_completed_hook_runs_after_failed_async_activity() -> None:
    async def fails(_ctx, _value):
        await asyncio.sleep(0)
        raise ValueError("boom")

    worker = HookedWorker()
    worker.add_activity(fails)
    stub = RecordingStub()
    request = _activity_request("fails", "hello")

    asyncio.run(worker._execute_activity_async(request, stub, b"token"))

    assert worker.events == [("started", "fails"), ("completed", "fails")]
    assert len(stub.completed) == 1
    assert stub.completed[0].failureDetails.errorType == "builtins.ValueError"


def test_activity_handler_matches_activity_kind() -> None:
    def sync_activity(_ctx, value):
        return value

    async def async_activity(_ctx, value):
        return value

    worker = HookedWorker()
    worker.add_activity(sync_activity)
    worker.add_activity(async_activity)

    assert worker._get_activity_handler(_activity_request("sync_activity", "x")) == worker._execute_activity
    assert worker._get_activity_handler(_activity_request("async_activity", "x")) == worker._execute_activity_async
    # Unknown activities keep the sync path, which reports ActivityNotRegisteredError.
    assert worker._get_activity_handler(_activity_request("missing", "x")) == worker._execute_activity


def _activity_request(name: str, value: str) -> pb.ActivityRequest:
    return pb.ActivityRequest(
        orchestrationInstance=pb.OrchestrationInstance(instanceId="instance-1"),
        name=name,
        taskId=1,
        input=wrappers_pb2.StringValue(value=f"\"{value}\""),
    )
