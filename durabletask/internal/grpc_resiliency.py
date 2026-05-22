# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import inspect
import random
import threading
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional, Union

import grpc
import grpc.aio

# Fully qualified sidecar RPC method paths (as exposed by the gRPC client call
# details) that legitimately block on the server until an instance reaches a
# terminal state. ``DEADLINE_EXCEEDED`` on these is the caller's chosen timeout
# expiring rather than a transport failure, so we do not treat it as one.
LONG_POLL_METHODS = {
    "/TaskHubSidecarService/WaitForInstanceStart",
    "/TaskHubSidecarService/WaitForInstanceCompletion",
}

# Cap the attempt number fed into ``2 ** attempt`` to keep the jitter calculation
# bounded for callers that retry indefinitely; once we hit the cap, the upper
# bound is fully governed by ``cap_seconds``.
_MAX_JITTER_ATTEMPT_EXPONENT = 30


def get_full_jitter_delay_seconds(
        attempt: int,
        *,
        base_seconds: float,
        cap_seconds: float,
) -> float:
    capped_attempt = min(attempt, _MAX_JITTER_ATTEMPT_EXPONENT)
    upper_bound = min(cap_seconds, base_seconds * (2 ** capped_attempt))
    return random.random() * upper_bound


@dataclass
class FailureTracker:
    """Counts consecutive transport failures with thread-safe mutation.

    The sync ``TaskHubGrpcClient`` is commonly invoked from multiple worker
    threads, so ``record_failure``/``record_success`` need a lock to keep the
    increment-and-compare atomic. The async client only mutates this from a
    single event loop, but the extra lock has negligible cost on that path.
    """

    threshold: int
    consecutive_failures: int = 0
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False, compare=False
    )

    def record_failure(self) -> bool:
        if self.threshold <= 0:
            return False
        with self._lock:
            self.consecutive_failures += 1
            return self.consecutive_failures >= self.threshold

    def record_success(self) -> None:
        with self._lock:
            self.consecutive_failures = 0


def is_client_transport_failure(method: str, status_code: grpc.StatusCode) -> bool:
    """Classify a unary RPC failure.

    ``method`` is the fully qualified gRPC method path (e.g.
    ``/TaskHubSidecarService/WaitForInstanceStart``) so that long-poll RPCs can
    be looked up against :data:`LONG_POLL_METHODS` directly.
    """
    if status_code == grpc.StatusCode.UNAVAILABLE:
        return True
    if status_code == grpc.StatusCode.DEADLINE_EXCEEDED:
        return method not in LONG_POLL_METHODS
    return False


def is_worker_transport_failure(status_code: grpc.StatusCode) -> bool:
    return status_code in {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    }


class ClientResiliencyInterceptor(grpc.UnaryUnaryClientInterceptor):
    """Tracks unary call outcomes and triggers channel recreation.

    Centralising the resiliency logic here means the client itself does not
    need to wrap every stub call: any unary RPC sent through the intercepted
    channel automatically participates in failure tracking and channel
    recreation, including future RPCs that are added to the service stub.
    """

    def __init__(
            self,
            failure_tracker: FailureTracker,
            on_recreate: Callable[[], None],
    ):
        self._failure_tracker = failure_tracker
        self._on_recreate = on_recreate

    def intercept_unary_unary(self, continuation, client_call_details, request):
        response = continuation(client_call_details, request)
        error = response.exception()
        self._record_outcome(client_call_details.method, error)
        return response

    def _record_outcome(self, method: str, error: Optional[BaseException]) -> None:
        if error is None:
            self._failure_tracker.record_success()
            return
        status_code = getattr(error, "code", lambda: None)()
        if status_code is not None and is_client_transport_failure(method, status_code):
            if self._failure_tracker.record_failure():
                self._on_recreate()
        else:
            self._failure_tracker.record_success()


class AsyncClientResiliencyInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    """Async counterpart of :class:`ClientResiliencyInterceptor`."""

    def __init__(
            self,
            failure_tracker: FailureTracker,
            on_recreate: Callable[[], Union[None, Awaitable[object]]],
    ):
        self._failure_tracker = failure_tracker
        self._on_recreate = on_recreate

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        try:
            response = await continuation(client_call_details, request)
        except grpc.aio.AioRpcError as rpc_error:
            await self._record_outcome(client_call_details.method, rpc_error)
            raise
        await self._record_outcome(client_call_details.method, None)
        return response

    async def _record_outcome(self, method: str, error: Optional[BaseException]) -> None:
        if error is None:
            self._failure_tracker.record_success()
            return
        status_code = getattr(error, "code", lambda: None)()
        if status_code is not None and is_client_transport_failure(method, status_code):
            if self._failure_tracker.record_failure():
                result = self._on_recreate()
                if inspect.isawaitable(result):
                    await result
        else:
            self._failure_tracker.record_success()
