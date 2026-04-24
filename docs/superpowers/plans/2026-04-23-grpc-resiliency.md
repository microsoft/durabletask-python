# gRPC Resiliency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement automatic healing of stale gRPC worker streams and client channels in `durabletask-python`, aligned with the behavior added in `durabletask-dotnet` PR 708.

**Architecture:** Add explicit public resiliency option types plus a shared internal transport helper module, then wire those pieces into the worker loop and the sync and async clients. SDK-owned channels will be recreated after repeated transport failures, while caller-owned channels keep their existing ownership model and are only observed and logged.

**Tech Stack:** Python 3.10+, grpc, grpc.aio, pytest, unittest.mock, flake8

---

## File map

- `durabletask/grpc_options.py` - public resiliency option dataclasses and validation
- `durabletask/internal/grpc_resiliency.py` - shared backoff, failure tracking, and transport-failure classification helpers
- `durabletask/client.py` - sync and async client transport state, unary invocation helpers, and channel recreation logic
- `durabletask/worker.py` - hello timeout, stream-outcome classification, worker reconnect policy, and SDK-owned channel recreation
- `durabletask-azuremanaged/durabletask/azuremanaged/client.py` - pass `resiliency_options` through Azure Managed client wrappers
- `durabletask-azuremanaged/durabletask/azuremanaged/worker.py` - pass `resiliency_options` through Azure Managed worker wrapper
- `tests/durabletask/test_grpc_resiliency.py` - option validation and shared helper tests
- `tests/durabletask/test_worker_resiliency.py` - worker stream monitoring and reconnect behavior
- `tests/durabletask/test_client.py` - sync and async client constructor and channel recreation tests
- `tests/durabletask-azuremanaged/test_grpc_resiliency.py` - wrapper pass-through tests for the new option surfaces
- `CHANGELOG.md` - user-facing changelog entry for the core SDK
- `durabletask-azuremanaged/CHANGELOG.md` - user-facing changelog entry for Azure Managed wrappers

### Task 1: Add public resiliency option types

**Files:**
- Modify: `durabletask/grpc_options.py`
- Create: `tests/durabletask/test_grpc_resiliency.py`

- [x] **Step 1: Write the failing option tests**

```python
import pytest

from durabletask.grpc_options import (
    GrpcClientResiliencyOptions,
    GrpcWorkerResiliencyOptions,
)


def test_worker_resiliency_defaults_are_enabled():
    options = GrpcWorkerResiliencyOptions()
    assert options.hello_timeout_seconds == 30.0
    assert options.silent_disconnect_timeout_seconds == 120.0
    assert options.channel_recreate_failure_threshold == 5
    assert options.reconnect_backoff_base_seconds == 1.0
    assert options.reconnect_backoff_cap_seconds == 30.0


def test_worker_resiliency_allows_disabling_timeout_and_threshold():
    options = GrpcWorkerResiliencyOptions(
        silent_disconnect_timeout_seconds=0.0,
        channel_recreate_failure_threshold=0,
    )
    assert options.silent_disconnect_timeout_seconds == 0.0
    assert options.channel_recreate_failure_threshold == 0


def test_worker_resiliency_rejects_invalid_durations():
    with pytest.raises(ValueError, match="hello_timeout_seconds must be > 0"):
        GrpcWorkerResiliencyOptions(hello_timeout_seconds=0.0)
    with pytest.raises(ValueError, match="reconnect_backoff_cap_seconds must be >= reconnect_backoff_base_seconds"):
        GrpcWorkerResiliencyOptions(
            reconnect_backoff_base_seconds=5.0,
            reconnect_backoff_cap_seconds=1.0,
        )


def test_client_resiliency_defaults_are_enabled():
    options = GrpcClientResiliencyOptions()
    assert options.channel_recreate_failure_threshold == 5
    assert options.min_recreate_interval_seconds == 30.0


def test_client_resiliency_rejects_negative_cooldown():
    with pytest.raises(ValueError, match="min_recreate_interval_seconds must be >= 0"):
        GrpcClientResiliencyOptions(min_recreate_interval_seconds=-1.0)
```

- [x] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/durabletask/test_grpc_resiliency.py -v`

Expected: FAIL with `ImportError` or `AttributeError` because the new option classes do not exist yet.

- [x] **Step 3: Write the minimal implementation**

```python
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class GrpcWorkerResiliencyOptions:
    hello_timeout_seconds: float = 30.0
    silent_disconnect_timeout_seconds: float = 120.0
    channel_recreate_failure_threshold: int = 5
    reconnect_backoff_base_seconds: float = 1.0
    reconnect_backoff_cap_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.hello_timeout_seconds <= 0:
            raise ValueError("hello_timeout_seconds must be > 0")
        if self.silent_disconnect_timeout_seconds < 0:
            raise ValueError("silent_disconnect_timeout_seconds must be >= 0")
        if self.channel_recreate_failure_threshold < 0:
            raise ValueError("channel_recreate_failure_threshold must be >= 0")
        if self.reconnect_backoff_base_seconds <= 0:
            raise ValueError("reconnect_backoff_base_seconds must be > 0")
        if self.reconnect_backoff_cap_seconds <= 0:
            raise ValueError("reconnect_backoff_cap_seconds must be > 0")
        if self.reconnect_backoff_cap_seconds < self.reconnect_backoff_base_seconds:
            raise ValueError(
                "reconnect_backoff_cap_seconds must be >= reconnect_backoff_base_seconds"
            )


@dataclass
class GrpcClientResiliencyOptions:
    channel_recreate_failure_threshold: int = 5
    min_recreate_interval_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.channel_recreate_failure_threshold < 0:
            raise ValueError("channel_recreate_failure_threshold must be >= 0")
        if self.min_recreate_interval_seconds < 0:
            raise ValueError("min_recreate_interval_seconds must be >= 0")
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest tests/durabletask/test_grpc_resiliency.py -v`

Expected: PASS for the new option validation tests.

- [x] **Step 5: Commit**

```bash
git add durabletask/grpc_options.py tests/durabletask/test_grpc_resiliency.py
git commit -m "Add gRPC resiliency option types"
```

### Task 2: Thread resiliency options through constructors

**Files:**
- Modify: `durabletask/client.py`
- Modify: `durabletask/worker.py`
- Modify: `durabletask-azuremanaged/durabletask/azuremanaged/client.py`
- Modify: `durabletask-azuremanaged/durabletask/azuremanaged/worker.py`
- Modify: `tests/durabletask/test_client.py`
- Create: `tests/durabletask-azuremanaged/test_grpc_resiliency.py`

- [x] **Step 1: Write the failing constructor and wrapper tests**

```python
from unittest.mock import MagicMock, patch

from durabletask.client import AsyncTaskHubGrpcClient, TaskHubGrpcClient
from durabletask.grpc_options import (
    GrpcClientResiliencyOptions,
    GrpcWorkerResiliencyOptions,
)
from durabletask.worker import TaskHubGrpcWorker
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker


def test_client_stores_resiliency_options_for_recreation():
    resiliency = GrpcClientResiliencyOptions(channel_recreate_failure_threshold=7)
    with patch("durabletask.client.shared.get_grpc_channel", return_value=MagicMock()), patch(
        "durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=MagicMock()
    ):
        client = TaskHubGrpcClient(
            host_address="localhost:4001",
            resiliency_options=resiliency,
        )
    assert client._resiliency_options is resiliency
    assert client._host_address == "localhost:4001"


def test_async_client_stores_resolved_transport_inputs():
    resiliency = GrpcClientResiliencyOptions()
    with patch("durabletask.client.shared.get_async_grpc_channel", return_value=MagicMock()), patch(
        "durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=MagicMock()
    ):
        client = AsyncTaskHubGrpcClient(
            host_address="localhost:4001",
            resiliency_options=resiliency,
        )
    assert client._resiliency_options is resiliency
    assert client._host_address == "localhost:4001"


def test_worker_stores_resiliency_options():
    resiliency = GrpcWorkerResiliencyOptions(channel_recreate_failure_threshold=9)
    worker = TaskHubGrpcWorker(resiliency_options=resiliency)
    assert worker._resiliency_options is resiliency


def test_dts_client_passes_resiliency_options_to_base_client():
    resiliency = GrpcClientResiliencyOptions()
    with patch("durabletask.azuremanaged.client.TaskHubGrpcClient.__init__", return_value=None) as mock_init:
        DurableTaskSchedulerClient(
            host_address="localhost:4001",
            taskhub="hub",
            token_credential=None,
            resiliency_options=resiliency,
        )
    assert mock_init.call_args.kwargs["resiliency_options"] is resiliency


def test_dts_worker_passes_resiliency_options_to_base_worker():
    resiliency = GrpcWorkerResiliencyOptions()
    with patch("durabletask.azuremanaged.worker.TaskHubGrpcWorker.__init__", return_value=None) as mock_init:
        DurableTaskSchedulerWorker(
            host_address="localhost:4001",
            taskhub="hub",
            token_credential=None,
            resiliency_options=resiliency,
        )
    assert mock_init.call_args.kwargs["resiliency_options"] is resiliency
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/durabletask/test_client.py tests/durabletask-azuremanaged/test_grpc_resiliency.py -v`

Expected: FAIL because the constructors do not accept `resiliency_options` yet and do not retain enough transport state for later recreation.

- [x] **Step 3: Write the minimal implementation**

```python
self._host_address = host_address if host_address else shared.get_default_host_address()
self._secure_channel = secure_channel
self._channel_options = channel_options
self._resiliency_options = (
    resiliency_options if resiliency_options is not None else GrpcClientResiliencyOptions()
)
resolved_interceptors = (
    prepare_sync_interceptors(metadata, interceptors) if channel is None else interceptors
)
self._interceptors = list(resolved_interceptors) if resolved_interceptors is not None else None

self._resiliency_options = (
    resiliency_options if resiliency_options is not None else GrpcWorkerResiliencyOptions()
)

super().__init__(
    host_address=host_address,
    channel=channel,
    secure_channel=secure_channel,
    metadata=None,
    log_handler=log_handler,
    log_formatter=log_formatter,
    interceptors=resolved_interceptors,
    channel_options=channel_options,
    resiliency_options=resiliency_options,
    default_version=default_version,
    payload_store=payload_store,
)

super().__init__(
    host_address=host_address,
    channel=channel,
    secure_channel=secure_channel,
    metadata=None,
    log_handler=log_handler,
    log_formatter=log_formatter,
    interceptors=resolved_interceptors,
    channel_options=channel_options,
    resiliency_options=resiliency_options,
    concurrency_options=concurrency_options,
    maximum_timer_interval=None,
    payload_store=payload_store,
)
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest tests/durabletask/test_client.py tests/durabletask-azuremanaged/test_grpc_resiliency.py -v`

Expected: PASS for the new constructor and wrapper pass-through tests.

- [x] **Step 5: Commit**

```bash
git add durabletask/client.py durabletask/worker.py durabletask-azuremanaged/durabletask/azuremanaged/client.py durabletask-azuremanaged/durabletask/azuremanaged/worker.py tests/durabletask/test_client.py tests/durabletask-azuremanaged/test_grpc_resiliency.py
git commit -m "Thread gRPC resiliency options through constructors"
```

### Task 3: Add shared internal resiliency helpers

**Files:**
- Create: `durabletask/internal/grpc_resiliency.py`
- Modify: `tests/durabletask/test_grpc_resiliency.py`

- [x] **Step 1: Write the failing helper tests**

```python
import grpc
import pytest

from durabletask.internal.grpc_resiliency import (
    FailureTracker,
    get_full_jitter_delay_seconds,
    is_client_transport_failure,
    is_worker_transport_failure,
)


def test_full_jitter_delay_is_capped(monkeypatch):
    monkeypatch.setattr("durabletask.internal.grpc_resiliency.random.random", lambda: 1.0)
    delay = get_full_jitter_delay_seconds(10, base_seconds=1.0, cap_seconds=30.0)
    assert delay == 30.0


def test_failure_tracker_trips_at_threshold():
    tracker = FailureTracker(threshold=3)
    assert tracker.record_failure() is False
    assert tracker.record_failure() is False
    assert tracker.record_failure() is True
    tracker.record_success()
    assert tracker.consecutive_failures == 0


def test_client_transport_failure_ignores_long_poll_deadlines():
    assert is_client_transport_failure("WaitForInstanceStart", grpc.StatusCode.DEADLINE_EXCEEDED) is False
    assert is_client_transport_failure("StartInstance", grpc.StatusCode.DEADLINE_EXCEEDED) is True
    assert is_client_transport_failure("GetInstance", grpc.StatusCode.UNAVAILABLE) is True


def test_worker_transport_failure_filters_application_errors():
    assert is_worker_transport_failure(grpc.StatusCode.UNAVAILABLE) is True
    assert is_worker_transport_failure(grpc.StatusCode.DEADLINE_EXCEEDED) is True
    assert is_worker_transport_failure(grpc.StatusCode.UNAUTHENTICATED) is False
    assert is_worker_transport_failure(grpc.StatusCode.NOT_FOUND) is False
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/durabletask/test_grpc_resiliency.py -k "jitter or tracker or transport_failure" -v`

Expected: FAIL because the shared helper module and helper functions do not exist yet.

- [x] **Step 3: Write the minimal implementation**

```python
import random
from dataclasses import dataclass

import grpc


LONG_POLL_METHODS = {"WaitForInstanceStart", "WaitForInstanceCompletion"}


def get_full_jitter_delay_seconds(
    attempt: int,
    *,
    base_seconds: float,
    cap_seconds: float,
) -> float:
    capped_attempt = min(attempt, 30)
    upper_bound = min(cap_seconds, base_seconds * (2 ** capped_attempt))
    return random.random() * upper_bound


@dataclass
class FailureTracker:
    threshold: int
    consecutive_failures: int = 0

    def record_failure(self) -> bool:
        if self.threshold <= 0:
            return False
        self.consecutive_failures += 1
        return self.consecutive_failures >= self.threshold

    def record_success(self) -> None:
        self.consecutive_failures = 0


def is_client_transport_failure(method_name: str, status_code: grpc.StatusCode) -> bool:
    if status_code == grpc.StatusCode.UNAVAILABLE:
        return True
    if status_code == grpc.StatusCode.DEADLINE_EXCEEDED:
        return method_name not in LONG_POLL_METHODS
    return False


def is_worker_transport_failure(status_code: grpc.StatusCode) -> bool:
    return status_code in {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    }
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest tests/durabletask/test_grpc_resiliency.py -v`

Expected: PASS for the helper and option tests together.

- [x] **Step 5: Commit**

```bash
git add durabletask/internal/grpc_resiliency.py tests/durabletask/test_grpc_resiliency.py
git commit -m "Add shared gRPC resiliency helpers"
```

### Task 4: Harden the worker stream lifecycle

**Files:**
- Modify: `durabletask/worker.py`
- Create: `tests/durabletask/test_worker_resiliency.py`

- [x] **Step 1: Write the failing worker resiliency tests**

```python
import grpc
from unittest.mock import MagicMock

from durabletask.grpc_options import GrpcWorkerResiliencyOptions
from durabletask.worker import TaskHubGrpcWorker, _WorkItemStreamOutcome


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


def test_worker_counts_only_transport_failures_for_recreation():
    worker = TaskHubGrpcWorker(
        resiliency_options=GrpcWorkerResiliencyOptions(channel_recreate_failure_threshold=2)
    )
    assert worker._should_count_worker_failure(grpc.StatusCode.UNAVAILABLE) is True
    assert worker._should_count_worker_failure(grpc.StatusCode.UNAUTHENTICATED) is False


def test_worker_does_not_recreate_caller_owned_channel():
    worker = TaskHubGrpcWorker(channel=MagicMock())
    assert worker._can_recreate_channel() is False
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/durabletask/test_worker_resiliency.py -v`

Expected: FAIL because the worker does not expose explicit stream-outcome helpers yet and still uses ad hoc reconnect bookkeeping.

- [x] **Step 3: Write the minimal implementation**

```python
class _WorkItemStreamOutcome(Enum):
    SHUTDOWN = "shutdown"
    GRACEFUL_CLOSE_BEFORE_FIRST_MESSAGE = "graceful_close_before_first_message"
    GRACEFUL_CLOSE_AFTER_MESSAGE = "graceful_close_after_message"
    SILENT_DISCONNECT = "silent_disconnect"


def _classify_stream_outcome(self, *, saw_message: bool, timed_out: bool) -> _WorkItemStreamOutcome:
    if timed_out:
        return _WorkItemStreamOutcome.SILENT_DISCONNECT
    if saw_message:
        return _WorkItemStreamOutcome.GRACEFUL_CLOSE_AFTER_MESSAGE
    return _WorkItemStreamOutcome.GRACEFUL_CLOSE_BEFORE_FIRST_MESSAGE


def _should_count_worker_failure(self, status_code: grpc.StatusCode) -> bool:
    return is_worker_transport_failure(status_code)


def _can_recreate_channel(self) -> bool:
    return self._channel is None


hello_timeout = self._resiliency_options.hello_timeout_seconds
current_stub.Hello(empty_pb2.Empty(), timeout=hello_timeout)

queue_timeout = self._resiliency_options.silent_disconnect_timeout_seconds or None
work_item = await asyncio.wait_for(
    loop.run_in_executor(None, work_item_queue.get),
    timeout=queue_timeout,
)

delay = get_full_jitter_delay_seconds(
    conn_retry_count,
    base_seconds=self._resiliency_options.reconnect_backoff_base_seconds,
    cap_seconds=self._resiliency_options.reconnect_backoff_cap_seconds,
)

if work_item.HasField("healthPing"):
    failure_tracker.record_success()
    continue
```

- [x] **Step 4: Run the worker tests**

Run: `python -m pytest tests/durabletask/test_worker_resiliency.py -v`

Expected: PASS for the worker classification and ownership tests.

- [x] **Step 5: Commit**

```bash
git add durabletask/worker.py tests/durabletask/test_worker_resiliency.py
git commit -m "Harden worker gRPC stream reconnect behavior"
```

### Task 5: Add sync client channel recreation

**Files:**
- Modify: `durabletask/client.py`
- Modify: `tests/durabletask/test_client.py`

- [x] **Step 1: Write the failing sync client recreation tests**

```python
import grpc
import pytest
from unittest.mock import MagicMock, patch

from durabletask.client import TaskHubGrpcClient
from durabletask.grpc_options import GrpcClientResiliencyOptions


def test_sync_client_recreates_sdk_owned_channel_after_repeated_unavailable():
    first_channel = MagicMock(name="first-channel")
    second_channel = MagicMock(name="second-channel")
    first_stub = MagicMock()
    first_stub.GetInstance.side_effect = grpc.RpcError()
    second_stub = MagicMock()
    second_stub.GetInstance.return_value = MagicMock(exists=False)

    rpc_error = MagicMock(spec=grpc.RpcError)
    rpc_error.code.return_value = grpc.StatusCode.UNAVAILABLE
    first_stub.GetInstance.side_effect = rpc_error

    with patch("durabletask.client.shared.get_grpc_channel", side_effect=[first_channel, second_channel]), patch(
        "durabletask.client.stubs.TaskHubSidecarServiceStub", side_effect=[first_stub, second_stub]
    ):
        client = TaskHubGrpcClient(
            host_address="localhost:4001",
            resiliency_options=GrpcClientResiliencyOptions(
                channel_recreate_failure_threshold=1,
                min_recreate_interval_seconds=0.0,
            ),
        )
        with pytest.raises(grpc.RpcError):
            client.get_orchestration_state("abc")
        client.get_orchestration_state("abc")

    assert client._channel is second_channel


def test_sync_client_does_not_count_long_poll_deadline():
    rpc_error = MagicMock(spec=grpc.RpcError)
    rpc_error.code.return_value = grpc.StatusCode.DEADLINE_EXCEEDED
    stub = MagicMock()
    stub.WaitForInstanceStart.side_effect = rpc_error

    with patch("durabletask.client.shared.get_grpc_channel", return_value=MagicMock()), patch(
        "durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub
    ):
        client = TaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(channel_recreate_failure_threshold=1)
        )
        with pytest.raises(TimeoutError):
            client.wait_for_orchestration_start("abc")
        assert client._client_failure_tracker.consecutive_failures == 0
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/durabletask/test_client.py -k "recreates_sdk_owned_channel or long_poll_deadline" -v`

Expected: FAIL because client calls still go directly through the stub and the client has no failure tracker or channel recreation path.

- [x] **Step 3: Write the minimal implementation**

```python
self._client_failure_tracker = FailureTracker(
    self._resiliency_options.channel_recreate_failure_threshold
)
self._last_recreate_time = 0.0
self._recreate_lock = threading.Lock()

def _invoke_unary(self, method_name: str, request: Any, *, timeout: Optional[int] = None):
    method = getattr(self._stub, method_name)
    try:
        if timeout is None:
            response = method(request)
        else:
            response = method(request, timeout=timeout)
    except grpc.RpcError as rpc_error:
        if is_client_transport_failure(method_name, rpc_error.code()):
            should_recreate = self._client_failure_tracker.record_failure()
            if should_recreate:
                self._maybe_recreate_channel()
        else:
            self._client_failure_tracker.record_success()
        raise
    else:
        self._client_failure_tracker.record_success()
        return response

def _maybe_recreate_channel(self) -> None:
    if not self._owns_channel:
        return
    with self._recreate_lock:
        now = time.monotonic()
        if now - self._last_recreate_time < self._resiliency_options.min_recreate_interval_seconds:
            return
        old_channel = self._channel
        self._channel = shared.get_grpc_channel(
            host_address=self._host_address,
            secure_channel=self._secure_channel,
            interceptors=self._interceptors,
            channel_options=self._channel_options,
        )
        self._stub = stubs.TaskHubSidecarServiceStub(self._channel)
        self._last_recreate_time = now
        self._client_failure_tracker.record_success()
        threading.Timer(30.0, old_channel.close).start()
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest tests/durabletask/test_client.py -k "recreates_sdk_owned_channel or long_poll_deadline" -v`

Expected: PASS for both new sync client tests and no regressions in the existing client construction tests.

- [x] **Step 5: Commit**

```bash
git add durabletask/client.py tests/durabletask/test_client.py
git commit -m "Add sync client gRPC channel recreation"
```

### Task 6: Add async client channel recreation

**Files:**
- Modify: `durabletask/client.py`
- Modify: `tests/durabletask/test_client.py`

- [x] **Step 1: Write the failing async client recreation tests**

```python
import grpc
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from durabletask.client import AsyncTaskHubGrpcClient
from durabletask.grpc_options import GrpcClientResiliencyOptions


@pytest.mark.asyncio
async def test_async_client_recreates_sdk_owned_channel_after_unavailable():
    rpc_error = MagicMock(spec=grpc.aio.AioRpcError)
    rpc_error.code.return_value = grpc.StatusCode.UNAVAILABLE

    first_stub = MagicMock()
    first_stub.GetInstance = AsyncMock(side_effect=rpc_error)
    second_stub = MagicMock()
    second_stub.GetInstance = AsyncMock(return_value=MagicMock(exists=False))

    with patch("durabletask.client.shared.get_async_grpc_channel", side_effect=[MagicMock(), MagicMock()]), patch(
        "durabletask.client.stubs.TaskHubSidecarServiceStub", side_effect=[first_stub, second_stub]
    ):
        client = AsyncTaskHubGrpcClient(
            host_address="localhost:4001",
            resiliency_options=GrpcClientResiliencyOptions(
                channel_recreate_failure_threshold=1,
                min_recreate_interval_seconds=0.0,
            ),
        )
        with pytest.raises(grpc.aio.AioRpcError):
            await client.get_orchestration_state("abc")
        await client.get_orchestration_state("abc")


@pytest.mark.asyncio
async def test_async_client_does_not_count_wait_for_orchestration_deadline():
    rpc_error = MagicMock(spec=grpc.aio.AioRpcError)
    rpc_error.code.return_value = grpc.StatusCode.DEADLINE_EXCEEDED
    stub = MagicMock()
    stub.WaitForInstanceCompletion = AsyncMock(side_effect=rpc_error)

    with patch("durabletask.client.shared.get_async_grpc_channel", return_value=MagicMock()), patch(
        "durabletask.client.stubs.TaskHubSidecarServiceStub", return_value=stub
    ):
        client = AsyncTaskHubGrpcClient(
            resiliency_options=GrpcClientResiliencyOptions(channel_recreate_failure_threshold=1)
        )
        with pytest.raises(TimeoutError):
            await client.wait_for_orchestration_completion("abc")
        assert client._client_failure_tracker.consecutive_failures == 0
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/durabletask/test_client.py -k "async_client_recreates_sdk_owned_channel or async_client_does_not_count" -v`

Expected: FAIL because the async client still awaits stub methods directly and has no async-safe recreation path.

- [x] **Step 3: Write the minimal implementation**

```python
self._client_failure_tracker = FailureTracker(
    self._resiliency_options.channel_recreate_failure_threshold
)
self._recreate_lock = asyncio.Lock()
self._last_recreate_time = 0.0

async def _invoke_unary(self, method_name: str, request: Any, *, timeout: Optional[int] = None):
    method = getattr(self._stub, method_name)
    try:
        if timeout is None:
            response = await method(request)
        else:
            response = await method(request, timeout=timeout)
    except grpc.aio.AioRpcError as rpc_error:
        if is_client_transport_failure(method_name, rpc_error.code()):
            should_recreate = self._client_failure_tracker.record_failure()
            if should_recreate:
                await self._maybe_recreate_channel()
        else:
            self._client_failure_tracker.record_success()
        raise
    else:
        self._client_failure_tracker.record_success()
        return response

async def _maybe_recreate_channel(self) -> None:
    if not self._owns_channel:
        return
    async with self._recreate_lock:
        now = time.monotonic()
        if now - self._last_recreate_time < self._resiliency_options.min_recreate_interval_seconds:
            return
        old_channel = self._channel
        self._channel = shared.get_async_grpc_channel(
            host_address=self._host_address,
            secure_channel=self._secure_channel,
            interceptors=self._interceptors,
            channel_options=self._channel_options,
        )
        self._stub = stubs.TaskHubSidecarServiceStub(self._channel)
        self._last_recreate_time = now
        self._client_failure_tracker.record_success()
        asyncio.create_task(self._close_retired_channel(old_channel))


async def _close_retired_channel(self, channel: grpc.aio.Channel) -> None:
    await asyncio.sleep(30.0)
    await channel.close()
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest tests/durabletask/test_client.py -k "async_client_recreates_sdk_owned_channel or async_client_does_not_count" -v`

Expected: PASS for the async recreation tests and no regressions in the existing async client construction tests.

- [x] **Step 5: Commit**

```bash
git add durabletask/client.py tests/durabletask/test_client.py
git commit -m "Add async client gRPC channel recreation"
```

### Task 7: Update changelogs and run final verification

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `durabletask-azuremanaged/CHANGELOG.md`
- Modify: `docs/superpowers/specs/2026-04-23-grpc-resiliency-design.md` (only if the implementation changed the agreed design)
- Modify: `docs/superpowers/plans/2026-04-23-grpc-resiliency.md` (check off completed steps only after execution)

- [x] **Step 1: Add the changelog entries**

```markdown
## Unreleased

### Added

- Added automatic gRPC channel healing for SDK-owned clients and workers, with new resiliency option types for tuning hello deadlines, silent-disconnect detection, recreate thresholds, and recreate cooldowns.
```

```markdown
## Unreleased

### Added

- Added pass-through support for the new gRPC resiliency option types on Azure Managed clients and workers.
```

- [x] **Step 2: Run the focused tests**

Run:

```bash
python -m pytest tests/durabletask/test_grpc_resiliency.py tests/durabletask/test_worker_resiliency.py tests/durabletask/test_client.py tests/durabletask-azuremanaged/test_grpc_resiliency.py -v
```

Expected: PASS for all new and touched unit tests.

- [x] **Step 3: Run lint on the changed Python files**

Run:

```bash
python -m flake8 durabletask/grpc_options.py durabletask/internal/grpc_resiliency.py durabletask/client.py durabletask/worker.py durabletask-azuremanaged/durabletask/azuremanaged/client.py durabletask-azuremanaged/durabletask/azuremanaged/worker.py tests/durabletask/test_grpc_resiliency.py tests/durabletask/test_worker_resiliency.py tests/durabletask/test_client.py tests/durabletask-azuremanaged/test_grpc_resiliency.py
```

Expected: no output

- [x] **Step 4: Run the full test suite**

Run:

```bash
python -m pytest
```

Expected: PASS across the repository, including the existing orchestration and Azure Managed test suites.

- [x] **Step 5: Commit**

```bash
git add CHANGELOG.md durabletask-azuremanaged/CHANGELOG.md durabletask/grpc_options.py durabletask/internal/grpc_resiliency.py durabletask/client.py durabletask/worker.py durabletask-azuremanaged/durabletask/azuremanaged/client.py durabletask-azuremanaged/durabletask/azuremanaged/worker.py tests/durabletask/test_grpc_resiliency.py tests/durabletask/test_worker_resiliency.py tests/durabletask/test_client.py tests/durabletask-azuremanaged/test_grpc_resiliency.py
git commit -m "Add gRPC connection resiliency"
```
