# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Activity functions for the V1-style sample app (blueprint).

Activities are ordinary Azure Functions dispatched by the host. This blueprint
also provides the failure/flaky activities used to exercise error propagation
and the activity retry policies.
"""

import azure.durable_functions as df

bp = df.Blueprint()

# Module-global attempt counters keyed by a caller-supplied token. Activity
# retries re-invoke the same function in the same worker process, so this
# in-process state lets ``flaky`` fail a fixed number of times before
# succeeding -- exercising ``call_activity_with_retry`` end to end.
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
    """Fail until ``threshold`` attempts have been made, then succeed.

    The worker rejects parameterized generic and ``Optional`` annotations, so
    the parameter and return type must be the plain ``dict`` type.
    """
    key = payload["key"]
    threshold = int(payload["threshold"])
    _ATTEMPTS[key] = _ATTEMPTS.get(key, 0) + 1
    attempts = _ATTEMPTS[key]
    if attempts < threshold:
        raise ValueError(f"flaky failure {attempts}/{threshold}")
    return {"attempts": attempts}
