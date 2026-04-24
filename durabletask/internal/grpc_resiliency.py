# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import random
from dataclasses import dataclass

import grpc

LONG_POLL_METHODS = {"WaitForInstanceStart", "WaitForInstanceCompletion"}


def get_full_jitter_delay_seconds(
        attempt: int,
        *,
        base_seconds: float,
        cap_seconds: float) -> float:
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
