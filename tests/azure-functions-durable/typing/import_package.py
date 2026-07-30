# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from datetime import timedelta

import azure.durable_functions as df


def create_app() -> df.DFApp:
    return df.DFApp()


def create_retry_policy() -> df.RetryPolicy:
    return df.RetryPolicy(
        first_retry_interval=timedelta(seconds=1),
        max_number_of_attempts=3,
    )


def activity_is_complete(context: df.DurableOrchestrationContext) -> bool:
    return context.call_activity("activity").is_complete
