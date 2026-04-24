import asyncio
import grpc
from unittest.mock import MagicMock

import pytest

from durabletask.grpc_options import GrpcWorkerResiliencyOptions
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.worker import TaskHubGrpcWorker, _WorkItemStreamOutcome


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


class DummyWorkerManager:
    def __init__(self):
        self._shutdown_event = asyncio.Event()
        self.submissions: list[tuple[str, tuple]] = []

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


def _make_activity_work_item() -> pb.WorkItem:
    return pb.WorkItem(
        activityRequest=pb.ActivityRequest(
            name="test_activity",
            taskId=1,
            orchestrationInstance=pb.OrchestrationInstance(instanceId="instance-id"),
        ),
        completionToken="token",
    )


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
