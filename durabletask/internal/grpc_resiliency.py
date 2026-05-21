# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import random
import threading
from dataclasses import dataclass, field

import grpc

# Sidecar RPCs that legitimately block on the server until an instance reaches a
# terminal state. ``DEADLINE_EXCEEDED`` on these is the caller's chosen timeout
# expiring rather than a transport failure, so we do not treat it as one.
LONG_POLL_METHODS = {"WaitForInstanceStart", "WaitForInstanceCompletion"}

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
