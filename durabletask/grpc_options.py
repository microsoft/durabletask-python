# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any, Optional


@dataclass
class GrpcRetryPolicyOptions:
    """Configuration for transport-level gRPC retries."""

    max_attempts: int = 4
    initial_backoff_seconds: float = 0.05
    max_backoff_seconds: float = 0.25
    backoff_multiplier: float = 2.0
    retryable_status_codes: list[str] = field(default_factory=lambda: ["UNAVAILABLE"])

    def __post_init__(self) -> None:
        if self.max_attempts < 2:
            raise ValueError("max_attempts must be >= 2")
        if self.initial_backoff_seconds <= 0:
            raise ValueError("initial_backoff_seconds must be > 0")
        if self.max_backoff_seconds <= 0:
            raise ValueError("max_backoff_seconds must be > 0")
        if self.backoff_multiplier <= 0:
            raise ValueError("backoff_multiplier must be > 0")
        if self.max_backoff_seconds < self.initial_backoff_seconds:
            raise ValueError("max_backoff_seconds must be >= initial_backoff_seconds")
        if len(self.retryable_status_codes) == 0:
            raise ValueError("retryable_status_codes cannot be empty")
        # Validate that backoff values are representable as non-zero gRPC duration strings.
        self._format_duration(self.initial_backoff_seconds)
        self._format_duration(self.max_backoff_seconds)

    @staticmethod
    def _format_duration(seconds: float) -> str:
        formatted = f"{seconds:.9f}".rstrip('0')
        if formatted.endswith('.'):
            formatted += '0'
        if float(formatted) == 0:
            raise ValueError(
                f"Duration {seconds!r} rounds to zero; use a value large enough to "
                "produce a non-zero gRPC duration string."
            )
        return f"{formatted}s"

    def to_service_config(self) -> dict[str, Any]:
        return {
            "methodConfig": [
                {
                    "name": [{}],
                    "retryPolicy": {
                        "maxAttempts": self.max_attempts,
                        "initialBackoff": self._format_duration(self.initial_backoff_seconds),
                        "maxBackoff": self._format_duration(self.max_backoff_seconds),
                        "backoffMultiplier": self.backoff_multiplier,
                        "retryableStatusCodes": self.retryable_status_codes,
                    },
                }
            ]
        }


@dataclass
class GrpcChannelOptions:
    """Configuration for transport-level gRPC channel behavior."""

    max_receive_message_length: Optional[int] = None
    max_send_message_length: Optional[int] = None
    keepalive_time_ms: Optional[int] = None
    keepalive_timeout_ms: Optional[int] = None
    keepalive_permit_without_calls: Optional[bool] = None
    retry_policy: Optional[GrpcRetryPolicyOptions] = None
    raw_options: list[tuple[str, Any]] = field(default_factory=list)

    def to_grpc_options(self) -> list[tuple[str, Any]]:
        options = list(self.raw_options)

        if self.max_receive_message_length is not None:
            options.append(("grpc.max_receive_message_length", self.max_receive_message_length))
        if self.max_send_message_length is not None:
            options.append(("grpc.max_send_message_length", self.max_send_message_length))
        if self.keepalive_time_ms is not None:
            options.append(("grpc.keepalive_time_ms", self.keepalive_time_ms))
        if self.keepalive_timeout_ms is not None:
            options.append(("grpc.keepalive_timeout_ms", self.keepalive_timeout_ms))
        if self.keepalive_permit_without_calls is not None:
            options.append(
                (
                    "grpc.keepalive_permit_without_calls",
                    1 if self.keepalive_permit_without_calls else 0,
                )
            )

        if self.retry_policy is not None:
            options.append(("grpc.enable_retries", 1))
            options.append(("grpc.service_config", json.dumps(self.retry_policy.to_service_config())))

        return options
