# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Durable HTTP orchestrators for the V1-style sample app (blueprint).

Durable HTTP (``context.call_http``) is a v1-only feature reconstructed on top
of durabletask primitives. These orchestrators cover the happy path (GET), a
request with content (POST), and the non-2xx path (the response is returned to
the orchestrator rather than raising).
"""

import azure.durable_functions as df

bp = df.Blueprint()


@bp.orchestration_trigger(context_name="context")
def http_call(context: df.DurableOrchestrationContext):
    url = context.get_input()
    response = yield context.call_http("GET", url)
    return {"status_code": response.status_code, "content": response.content}


@bp.orchestration_trigger(context_name="context")
def http_post(context: df.DurableOrchestrationContext):
    payload = context.get_input()
    response = yield context.call_http("POST", payload["url"], content=payload["content"])
    return {"status_code": response.status_code, "content": response.content}
