# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Activity functions for the durabletask-native-style sample app (blueprint).

Activities are ordinary Azure Functions dispatched by the host (single-argument
input), shared by both authoring styles. Includes the failure/flaky activities
used to exercise error propagation and retry policies.
"""

import azure.durable_functions as df

bp = df.Blueprint()

# In-process attempt counters keyed by a caller-supplied token; see the v1
# sample's activities module for the rationale.
_ATTEMPTS: dict[str, int] = {}


@bp.activity_trigger(input_name="name")
def say_hello(name: str) -> str:
    return f"Hello {name}!"


@bp.activity_trigger(input_name="n")
def square(n: int) -> int:
    return n * n


@bp.activity_trigger(input_name="reason")
def always_fail(reason: str) -> str:
    raise ValueError(reason or "activity failed on purpose")


@bp.activity_trigger(input_name="payload")
def flaky(payload: dict) -> dict:
    """Fail until ``threshold`` attempts have been made, then succeed."""
    key = payload["key"]
    threshold = int(payload["threshold"])
    _ATTEMPTS[key] = _ATTEMPTS.get(key, 0) + 1
    attempts = _ATTEMPTS[key]
    if attempts < threshold:
        raise ValueError(f"flaky failure {attempts}/{threshold}")
    return {"attempts": attempts}
