import asyncio
import grpc
from threading import Event, Timer
from unittest.mock import MagicMock

import pytest

from durabletask.grpc_options import GrpcWorkerResiliencyOptions
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.worker import (
    _AsyncWorkerManager,
    ConcurrencyOptions,
    TaskHubGrpcWorker,
    _WorkItemStreamOutcome,
)


class FakeRpcError(grpc.RpcError):
    def __init__(self, status_code: grpc.StatusCode, details: str):
        super().__init__()
        self._status_code = status_code
        self._details = details

    def code(self):
        return self._status_code

    def details(self):
        return self._details

    def __str__(self):
        return self._details


class FakeResponseStream:
    def __init__(self, items=(), error: grpc.RpcError | None = None):
        self._items = list(items)
        self._error = error
        self.cancelled = False

    def __iter__(self):
        yield from self._items
        if self._error is not None:
            raise self._error

    def cancel(self):
        self.cancelled = True


class BlockingResponseStream:
    def __init__(self):
        self._cancel_event = Event()
        self.cancelled = False

    def __iter__(self):
        if not self._cancel_event.wait(timeout=0.5):
            raise AssertionError("response stream was not cancelled")
        return
        yield

    def cancel(self):
        self.cancelled = True
        self._cancel_event.set()


class DummyWorkerManager:
    def __init__(self):
        self._shutdown_event = asyncio.Event()
        self.submissions: list[tuple[str, tuple]] = []

    def prepare_for_run(self):
        self._shutdown_event = asyncio.Event()

    async def run(self):
        await self._shutdown_event.wait()

    def submit_orchestration(self, *args):
        self.submissions.append(("orchestrator", args))

    def submit_activity(self, *args):
        self.submissions.append(("activity", args))

    def submit_entity_batch(self, *args):
        self.submissions.append(("entity", args))

    def shutdown(self):
        self._shutdown_event.set()


def _complete_activity_request(req, stub, completion_token):
    stub.CompleteActivityTask(
        pb.ActivityResponse(
            instanceId=req.orchestrationInstance.instanceId,
            taskId=req.taskId,
            completionToken=completion_token,
        )
    )


def _make_activity_work_item(
    task_id: int = 1,
    completion_token: str = "token",
    instance_id: str = "instance-id",
) -> pb.WorkItem:
    return pb.WorkItem(
        activityRequest=pb.ActivityRequest(
            name="test_activity",
            taskId=task_id,
            orchestrationInstance=pb.OrchestrationInstance(instanceId=instance_id),
        ),
        completionToken=completion_token,
    )


async def _wait_for_condition(predicate, *, timeout: float = 2.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("condition was not met before timeout")
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_async_worker_manager_honors_shutdown_requested_before_run():
    manager = _AsyncWorkerManager(
        ConcurrencyOptions(maximum_thread_pool_workers=1),
        MagicMock(),
    )

    manager.shutdown()
    await asyncio.wait_for(manager.run(), timeout=1.0)


def test_worker_start_clears_prior_shutdown_request():
    worker = TaskHubGrpcWorker()
    worker._shutdown.set()
    run_started = Event()

    async def fake_run_loop():
        run_started.set()

    worker._async_run_loop = fake_run_loop
    worker.start()
    worker._runLoop.join(timeout=1.0)

    assert run_started.is_set() is True
    assert worker._shutdown.is_set() is False

    worker.stop()


def test_worker_classifies_graceful_close_before_first_message():
    worker = TaskHubGrpcWorker(
        resiliency_options=GrpcWorkerResiliencyOptions(silent_disconnect_timeout_seconds=5.0)
    )
    outcome = worker._classify_stream_outcome(
        saw_message=False,
        timed_out=False,
    )
    assert outcome is _WorkItemStreamOutcome.GRACEFUL_CLOSE_BEFORE_FIRST_MESSAGE


def test_worker_classifies_graceful_close_after_message():
    worker = TaskHubGrpcWorker()
    outcome = worker._classify_stream_outcome(
        saw_message=True,
        timed_out=False,
    )
    assert outcome is _WorkItemStreamOutcome.GRACEFUL_CLOSE_AFTER_MESSAGE


def test_worker_classifies_silent_disconnect():
    worker = TaskHubGrpcWorker(
        resiliency_options=GrpcWorkerResiliencyOptions(silent_disconnect_timeout_seconds=5.0)
    )
    outcome = worker._classify_stream_outcome(
        saw_message=False,
        timed_out=True,
    )
    assert outcome is _WorkItemStreamOutcome.SILENT_DISCONNECT


def test_worker_counts_only_transport_failures_for_recreation():
    worker = TaskHubGrpcWorker(
        resiliency_options=GrpcWorkerResiliencyOptions(channel_recreate_failure_threshold=2)
    )
    assert worker._should_count_worker_failure(grpc.StatusCode.UNAVAILABLE) is True
    assert worker._should_count_worker_failure(grpc.StatusCode.UNAUTHENTICATED) is False


def test_worker_does_not_recreate_caller_owned_channel():
    worker = TaskHubGrpcWorker(channel=MagicMock())
    assert worker._can_recreate_channel() is False


@pytest.mark.asyncio
async def test_worker_applies_configured_hello_timeout(monkeypatch):
    worker = TaskHubGrpcWorker(
        resiliency_options=GrpcWorkerResiliencyOptions(hello_timeout_seconds=12.5)
    )
    worker._async_worker_manager = DummyWorkerManager()
    monkeypatch.setattr(worker._shutdown, "wait", lambda timeout: False)

    stub = MagicMock()
    stub.GetWorkItems.side_effect = FakeRpcError(grpc.StatusCode.CANCELLED, "stop")

    monkeypatch.setattr("durabletask.worker.shared.get_grpc_channel", lambda *args, **kwargs: MagicMock())
    monkeypatch.setattr("durabletask.worker.stubs.TaskHubSidecarServiceStub", lambda channel: stub)

    await worker._async_run_loop()

    assert stub.Hello.call_args.kwargs["timeout"] == 12.5


@pytest.mark.asyncio
async def test_worker_does_not_recreate_sdk_owned_channel_for_non_transport_setup_errors(monkeypatch):
    worker = TaskHubGrpcWorker(
        resiliency_options=GrpcWorkerResiliencyOptions(channel_recreate_failure_threshold=2)
    )
    worker._async_worker_manager = DummyWorkerManager()
    monkeypatch.setattr(worker._shutdown, "wait", lambda timeout: False)

    created_channels = []

    def get_grpc_channel(*args, **kwargs):
        channel = MagicMock(name=f"channel-{len(created_channels) + 1}")
        created_channels.append(channel)
        return channel

    first_stub = MagicMock()
    first_stub.Hello.side_effect = RuntimeError("boom")

    second_stub = MagicMock()
    second_stub.GetWorkItems.side_effect = FakeRpcError(grpc.StatusCode.CANCELLED, "stop")

    stubs = [first_stub, second_stub]

    monkeypatch.setattr("durabletask.worker.shared.get_grpc_channel", get_grpc_channel)
    monkeypatch.setattr(
        "durabletask.worker.stubs.TaskHubSidecarServiceStub",
        lambda channel: stubs.pop(0),
    )

    await worker._async_run_loop()

    assert len(created_channels) == 1


@pytest.mark.asyncio
async def test_worker_recreates_sdk_owned_channel_after_transport_failure_threshold(monkeypatch):
    worker = TaskHubGrpcWorker(
        resiliency_options=GrpcWorkerResiliencyOptions(channel_recreate_failure_threshold=2)
    )
    worker._async_worker_manager = DummyWorkerManager()
    monkeypatch.setattr(worker._shutdown, "wait", lambda timeout: False)

    created_channels = []

    def get_grpc_channel(*args, **kwargs):
        channel = MagicMock(name=f"channel-{len(created_channels) + 1}")
        created_channels.append(channel)
        return channel

    stubs = [
        MagicMock(GetWorkItems=MagicMock(return_value=FakeResponseStream(error=FakeRpcError(
            grpc.StatusCode.UNAVAILABLE,
            "first transport failure",
        )))),
        MagicMock(GetWorkItems=MagicMock(return_value=FakeResponseStream(error=FakeRpcError(
            grpc.StatusCode.UNAVAILABLE,
            "second transport failure",
        )))),
        MagicMock(GetWorkItems=MagicMock(side_effect=FakeRpcError(
            grpc.StatusCode.CANCELLED,
            "stop",
        ))),
    ]
    stub_channels = []

    def create_stub(channel):
        stub_channels.append(channel)
        return stubs.pop(0)

    monkeypatch.setattr("durabletask.worker.shared.get_grpc_channel", get_grpc_channel)
    monkeypatch.setattr("durabletask.worker.stubs.TaskHubSidecarServiceStub", create_stub)

    await worker._async_run_loop()

    assert len(created_channels) == 2
    assert stub_channels[0] is created_channels[0]
    assert stub_channels[1] is created_channels[0]
    assert stub_channels[2] is created_channels[1]
    created_channels[0].close.assert_called_once()
    created_channels[1].close.assert_called_once()


@pytest.mark.asyncio
async def test_worker_recreates_sdk_owned_channel_after_silent_disconnect(monkeypatch):
    worker = TaskHubGrpcWorker(
        resiliency_options=GrpcWorkerResiliencyOptions(
            channel_recreate_failure_threshold=1,
            silent_disconnect_timeout_seconds=0.01,
        )
    )
    worker._async_worker_manager = DummyWorkerManager()

    wait_calls = []

    def shutdown_wait(timeout):
        wait_calls.append(timeout)
        return False

    monkeypatch.setattr(worker._shutdown, "wait", shutdown_wait)

    delay_calls = []

    def fake_delay(attempt, *, base_seconds, cap_seconds):
        delay_calls.append((attempt, base_seconds, cap_seconds))
        return 0.25

    created_channels = []

    def get_grpc_channel(*args, **kwargs):
        channel = MagicMock(name=f"channel-{len(created_channels) + 1}")
        created_channels.append(channel)
        return channel

    blocking_stream = BlockingResponseStream()
    stub_channels = []
    stubs = [
        MagicMock(GetWorkItems=MagicMock(return_value=blocking_stream)),
        MagicMock(GetWorkItems=MagicMock(side_effect=FakeRpcError(
            grpc.StatusCode.CANCELLED,
            "stop",
        ))),
    ]

    def create_stub(channel):
        stub_channels.append(channel)
        return stubs.pop(0)

    monkeypatch.setattr("durabletask.worker.get_full_jitter_delay_seconds", fake_delay)
    monkeypatch.setattr("durabletask.worker.shared.get_grpc_channel", get_grpc_channel)
    monkeypatch.setattr("durabletask.worker.stubs.TaskHubSidecarServiceStub", create_stub)

    await worker._async_run_loop()

    assert blocking_stream.cancelled is True
    assert delay_calls == [(1, 1.0, 30.0)]
    assert wait_calls == [0.25]
    assert len(created_channels) == 2
    assert stub_channels == created_channels
    created_channels[0].close.assert_called_once()
    created_channels[1].close.assert_called_once()


@pytest.mark.asyncio
async def test_worker_closes_sdk_owned_channel_on_graceful_stream_reset(monkeypatch):
    worker = TaskHubGrpcWorker()
    worker._async_worker_manager = DummyWorkerManager()
    monkeypatch.setattr(worker._shutdown, "wait", lambda timeout: False)

    created_channels = []

    def get_grpc_channel(*args, **kwargs):
        channel = MagicMock(name=f"channel-{len(created_channels) + 1}")
        created_channels.append(channel)
        return channel

    stub_channels = []
    stubs = [
        MagicMock(GetWorkItems=MagicMock(return_value=FakeResponseStream())),
        MagicMock(GetWorkItems=MagicMock(side_effect=FakeRpcError(
            grpc.StatusCode.CANCELLED,
            "stop",
        ))),
    ]

    def create_stub(channel):
        stub_channels.append(channel)
        return stubs.pop(0)

    monkeypatch.setattr("durabletask.worker.shared.get_grpc_channel", get_grpc_channel)
    monkeypatch.setattr("durabletask.worker.stubs.TaskHubSidecarServiceStub", create_stub)

    await worker._async_run_loop()

    assert len(created_channels) == 2
    assert stub_channels == created_channels
    created_channels[0].close.assert_called_once()
    created_channels[1].close.assert_called_once()


@pytest.mark.asyncio
async def test_worker_defers_sdk_owned_channel_close_until_inflight_completion_finishes(monkeypatch):
    worker = TaskHubGrpcWorker()
    worker_manager = DummyWorkerManager()
    worker._async_worker_manager = worker_manager
    worker._execute_activity = _complete_activity_request
    monkeypatch.setattr(worker._shutdown, "wait", lambda timeout: False)

    created_channels = []

    def get_grpc_channel(*args, **kwargs):
        channel = MagicMock(name=f"channel-{len(created_channels) + 1}")
        created_channels.append(channel)
        return channel

    completed_responses = []
    first_stub = MagicMock()
    first_stub.GetWorkItems.return_value = FakeResponseStream(items=[_make_activity_work_item()])

    def complete_activity(response):
        assert created_channels[0].close.call_count == 0
        completed_responses.append(response)

    first_stub.CompleteActivityTask.side_effect = complete_activity

    second_stub = MagicMock()
    second_stub.GetWorkItems.side_effect = FakeRpcError(
        grpc.StatusCode.CANCELLED,
        "stop",
    )

    stubs = [first_stub, second_stub]
    stub_channels = []

    def create_stub(channel):
        stub_channels.append(channel)
        return stubs.pop(0)

    monkeypatch.setattr("durabletask.worker.shared.get_grpc_channel", get_grpc_channel)
    monkeypatch.setattr("durabletask.worker.stubs.TaskHubSidecarServiceStub", create_stub)

    await worker._async_run_loop()

    assert len(worker_manager.submissions) == 1
    assert len(created_channels) == 2
    assert stub_channels == created_channels
    created_channels[0].close.assert_not_called()
    created_channels[1].close.assert_called_once()

    _, submission = worker_manager.submissions[0]
    func, _, req, stub, completion_token = submission
    func(req, stub, completion_token)

    assert len(completed_responses) == 1
    assert completed_responses[0].completionToken == "token"
    created_channels[0].close.assert_called_once()


@pytest.mark.asyncio
async def test_worker_shutdown_drains_real_manager_work_before_closing_retired_sdk_channel(monkeypatch):
    worker = TaskHubGrpcWorker(
        concurrency_options=ConcurrencyOptions(
            maximum_concurrent_activity_work_items=1,
            maximum_thread_pool_workers=1,
        )
    )
    worker._execute_activity = _complete_activity_request
    monkeypatch.setattr(worker._shutdown, "wait", lambda timeout: False)

    created_channels = []

    def get_grpc_channel(*args, **kwargs):
        channel = MagicMock(name=f"channel-{len(created_channels) + 1}")
        created_channels.append(channel)
        return channel

    allow_first_completion = Event()
    first_completion_started = Event()
    completed_task_ids = []
    first_stub = MagicMock()
    first_stub.GetWorkItems.return_value = FakeResponseStream(items=[
        _make_activity_work_item(task_id=1, completion_token="token-1"),
        _make_activity_work_item(task_id=2, completion_token="token-2"),
    ])

    def complete_activity(response):
        completed_task_ids.append(response.taskId)
        if response.taskId == 1:
            first_completion_started.set()
            Timer(0.2, allow_first_completion.set).start()
            assert allow_first_completion.wait(timeout=5.0)
        elif response.taskId == 2:
            assert created_channels[0].close.call_count == 0

    first_stub.CompleteActivityTask.side_effect = complete_activity

    second_stub = MagicMock()
    second_stub.GetWorkItems.side_effect = FakeRpcError(
        grpc.StatusCode.CANCELLED,
        "stop",
    )

    stubs = [first_stub, second_stub]
    stub_channels = []

    def create_stub(channel):
        stub_channels.append(channel)
        return stubs.pop(0)

    monkeypatch.setattr("durabletask.worker.shared.get_grpc_channel", get_grpc_channel)
    monkeypatch.setattr("durabletask.worker.stubs.TaskHubSidecarServiceStub", create_stub)

    run_task = asyncio.create_task(worker._async_run_loop())
    await asyncio.wait_for(run_task, timeout=2.0)

    assert first_completion_started.is_set() is True
    assert len(created_channels) == 2
    assert stub_channels == created_channels
    assert completed_task_ids == [1, 2]
    created_channels[0].close.assert_called_once()
    created_channels[1].close.assert_called_once()


@pytest.mark.asyncio
async def test_worker_shutdown_runs_real_manager_cancellation_wrapper_before_closing_retired_sdk_channel(monkeypatch):
    worker = TaskHubGrpcWorker(
        concurrency_options=ConcurrencyOptions(
            maximum_concurrent_activity_work_items=1,
            maximum_thread_pool_workers=1,
        )
    )
    monkeypatch.setattr(worker._shutdown, "wait", lambda timeout: False)

    created_channels = []

    def get_grpc_channel(*args, **kwargs):
        channel = MagicMock(name=f"channel-{len(created_channels) + 1}")
        created_channels.append(channel)
        return channel

    allow_first_completion = Event()
    first_completion_started = Event()
    completed_task_ids = []
    cancelled_task_ids = []

    def execute_activity(req, stub, completion_token):
        if req.taskId == 1:
            _complete_activity_request(req, stub, completion_token)
        else:
            raise RuntimeError("boom")

    def cancel_activity(req, stub, completion_token):
        cancelled_task_ids.append(req.taskId)
        assert created_channels[0].close.call_count == 0

    worker._execute_activity = execute_activity
    worker._cancel_activity = cancel_activity

    first_stub = MagicMock()
    first_stub.GetWorkItems.return_value = FakeResponseStream(items=[
        _make_activity_work_item(task_id=1, completion_token="token-1"),
        _make_activity_work_item(task_id=2, completion_token="token-2"),
    ])

    def complete_activity(response):
        completed_task_ids.append(response.taskId)
        Timer(0.2, allow_first_completion.set).start()
        first_completion_started.set()
        assert allow_first_completion.wait(timeout=5.0)

    first_stub.CompleteActivityTask.side_effect = complete_activity

    second_stub = MagicMock()
    second_stub.GetWorkItems.side_effect = FakeRpcError(
        grpc.StatusCode.CANCELLED,
        "stop",
    )

    stubs = [first_stub, second_stub]
    stub_channels = []

    def create_stub(channel):
        stub_channels.append(channel)
        return stubs.pop(0)

    monkeypatch.setattr("durabletask.worker.shared.get_grpc_channel", get_grpc_channel)
    monkeypatch.setattr("durabletask.worker.stubs.TaskHubSidecarServiceStub", create_stub)

    run_task = asyncio.create_task(worker._async_run_loop())
    await asyncio.wait_for(run_task, timeout=2.0)

    assert first_completion_started.is_set() is True
    assert len(created_channels) == 2
    assert stub_channels == created_channels
    assert completed_task_ids == [1]
    assert cancelled_task_ids == [2]
    created_channels[0].close.assert_called_once()
    created_channels[1].close.assert_called_once()


@pytest.mark.asyncio
async def test_worker_never_closes_caller_owned_channel_after_graceful_reset(monkeypatch):
    provided_channel = MagicMock(name="provided-channel")
    worker = TaskHubGrpcWorker(channel=provided_channel)
    worker_manager = DummyWorkerManager()
    worker._async_worker_manager = worker_manager
    worker._execute_activity = _complete_activity_request
    monkeypatch.setattr(worker._shutdown, "wait", lambda timeout: False)

    completed_responses = []
    first_stub = MagicMock()
    first_stub.GetWorkItems.return_value = FakeResponseStream(items=[_make_activity_work_item()])

    def complete_activity(response):
        assert provided_channel.close.call_count == 0
        completed_responses.append(response)

    first_stub.CompleteActivityTask.side_effect = complete_activity

    second_stub = MagicMock()
    second_stub.GetWorkItems.side_effect = FakeRpcError(
        grpc.StatusCode.CANCELLED,
        "stop",
    )

    stubs = [first_stub, second_stub]
    stub_channels = []

    def create_stub(channel):
        stub_channels.append(channel)
        return stubs.pop(0)

    monkeypatch.setattr(
        "durabletask.worker.shared.get_grpc_channel",
        lambda *args, **kwargs: pytest.fail(
            "SDK channel factory should not run for caller-owned channels"
        ),
    )
    monkeypatch.setattr("durabletask.worker.stubs.TaskHubSidecarServiceStub", create_stub)

    await worker._async_run_loop()

    assert len(worker_manager.submissions) == 1
    assert stub_channels == [provided_channel, provided_channel]
    provided_channel.close.assert_not_called()

    _, submission = worker_manager.submissions[0]
    func, _, req, stub, completion_token = submission
    func(req, stub, completion_token)

    assert len(completed_responses) == 1
    assert completed_responses[0].completionToken == "token"
    provided_channel.close.assert_not_called()


@pytest.mark.asyncio
async def test_worker_uses_reconnect_backoff_helper_after_connection_failure(monkeypatch):
    worker = TaskHubGrpcWorker(
        resiliency_options=GrpcWorkerResiliencyOptions(
            reconnect_backoff_base_seconds=1.5,
            reconnect_backoff_cap_seconds=9.0,
        )
    )
    worker._async_worker_manager = DummyWorkerManager()

    wait_calls = []

    def shutdown_wait(timeout):
        wait_calls.append(timeout)
        return False

    monkeypatch.setattr(worker._shutdown, "wait", shutdown_wait)

    delay_calls = []

    def fake_delay(attempt, *, base_seconds, cap_seconds):
        delay_calls.append((attempt, base_seconds, cap_seconds))
        return 0.75

    channel = MagicMock(name="channel-1")
    stub_channels = []
    first_stub = MagicMock()
    first_stub.Hello.side_effect = FakeRpcError(
        grpc.StatusCode.UNAVAILABLE,
        "connect failed",
    )
    second_stub = MagicMock()
    second_stub.GetWorkItems.side_effect = FakeRpcError(
        grpc.StatusCode.CANCELLED,
        "stop",
    )
    stubs = [first_stub, second_stub]

    def create_stub(current_channel):
        stub_channels.append(current_channel)
        return stubs.pop(0)

    monkeypatch.setattr("durabletask.worker.get_full_jitter_delay_seconds", fake_delay)
    monkeypatch.setattr("durabletask.worker.shared.get_grpc_channel", lambda *args, **kwargs: channel)
    monkeypatch.setattr("durabletask.worker.stubs.TaskHubSidecarServiceStub", create_stub)

    await worker._async_run_loop()

    assert delay_calls == [(1, 1.5, 9.0)]
    assert wait_calls == [0.75]
    assert stub_channels == [channel, channel]
    channel.close.assert_called_once()


@pytest.mark.asyncio
async def test_worker_never_replaces_caller_owned_channel_during_transport_failures(monkeypatch):
    provided_channel = MagicMock(name="provided-channel")
    worker = TaskHubGrpcWorker(
        channel=provided_channel,
        resiliency_options=GrpcWorkerResiliencyOptions(channel_recreate_failure_threshold=1),
    )
    worker._async_worker_manager = DummyWorkerManager()
    monkeypatch.setattr(worker._shutdown, "wait", lambda timeout: False)

    stub_channels = []
    stubs = [
        MagicMock(GetWorkItems=MagicMock(return_value=FakeResponseStream(error=FakeRpcError(
            grpc.StatusCode.UNAVAILABLE,
            "transport failure",
        )))),
        MagicMock(GetWorkItems=MagicMock(side_effect=FakeRpcError(
            grpc.StatusCode.CANCELLED,
            "stop",
        ))),
    ]

    def create_stub(channel):
        stub_channels.append(channel)
        return stubs.pop(0)

    monkeypatch.setattr(
        "durabletask.worker.shared.get_grpc_channel",
        lambda *args, **kwargs: pytest.fail("SDK channel factory should not run for caller-owned channels"),
    )
    monkeypatch.setattr("durabletask.worker.stubs.TaskHubSidecarServiceStub", create_stub)

    await worker._async_run_loop()

    assert stub_channels == [provided_channel, provided_channel]
    provided_channel.close.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("work_item", "expected_submissions"),
    [
        (pb.WorkItem(healthPing=pb.HealthPing()), 0),
        (_make_activity_work_item(), 1),
    ],
)
async def test_worker_received_messages_reset_failure_tracker(monkeypatch, work_item, expected_submissions):
    worker = TaskHubGrpcWorker(
        resiliency_options=GrpcWorkerResiliencyOptions(
            channel_recreate_failure_threshold=2,
            silent_disconnect_timeout_seconds=5.0,
        )
    )
    worker_manager = DummyWorkerManager()
    worker._async_worker_manager = worker_manager
    monkeypatch.setattr(worker._shutdown, "wait", lambda timeout: False)

    created_channels = []

    def get_grpc_channel(*args, **kwargs):
        channel = MagicMock(name=f"channel-{len(created_channels) + 1}")
        created_channels.append(channel)
        return channel

    stubs = [
        MagicMock(GetWorkItems=MagicMock(return_value=FakeResponseStream(error=FakeRpcError(
            grpc.StatusCode.UNAVAILABLE,
            "first transport failure",
        )))),
        MagicMock(GetWorkItems=MagicMock(return_value=FakeResponseStream(
            items=[work_item],
            error=FakeRpcError(grpc.StatusCode.UNAVAILABLE, "second transport failure"),
        ))),
        MagicMock(GetWorkItems=MagicMock(side_effect=FakeRpcError(
            grpc.StatusCode.CANCELLED,
            "stop",
        ))),
    ]

    monkeypatch.setattr("durabletask.worker.shared.get_grpc_channel", get_grpc_channel)
    monkeypatch.setattr(
        "durabletask.worker.stubs.TaskHubSidecarServiceStub",
        lambda channel: stubs.pop(0),
    )

    await worker._async_run_loop()

    assert len(created_channels) == 1
    assert len(worker_manager.submissions) == expected_submissions
