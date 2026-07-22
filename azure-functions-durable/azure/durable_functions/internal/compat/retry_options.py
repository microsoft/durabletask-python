# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from datetime import timedelta

from warnings import deprecated

from durabletask.task import RetryPolicy


@deprecated(
    "RetryOptions is deprecated; use durabletask.task.RetryPolicy with "
    "timedelta values instead.")
class RetryOptions(RetryPolicy):
    """Backwards-compatible shim for the v1 ``RetryOptions`` class.

    This maps the v1 millisecond-based constructor onto the durabletask
    :class:`~durabletask.task.RetryPolicy`, which uses ``timedelta`` values.
    New code should use ``RetryPolicy`` directly.
    """

    def __init__(
            self,
            first_retry_interval_in_milliseconds: int,
            max_number_of_attempts: int):
        """Create a new RetryOptions instance.

        Args:
            first_retry_interval_in_milliseconds (int): The retry interval, in
                milliseconds, to use for the first retry attempt. Must be
                greater than 0.
            max_number_of_attempts (int): The maximum number of retry attempts.
        """
        if first_retry_interval_in_milliseconds <= 0:
            raise ValueError(
                "first_retry_interval_in_milliseconds value must be greater than 0.")

        super().__init__(
            first_retry_interval=timedelta(
                milliseconds=first_retry_interval_in_milliseconds),
            max_number_of_attempts=max_number_of_attempts)

    @property
    def first_retry_interval_in_milliseconds(self) -> int:
        """Get the first retry interval, in milliseconds."""
        return int(self.first_retry_interval / timedelta(milliseconds=1))

    def to_json(self) -> dict[str, int]:
        """Return the v1 JSON representation of these retry options."""
        return {
            "firstRetryIntervalInMilliseconds": self.first_retry_interval_in_milliseconds,
            "maxNumberOfAttempts": self.max_number_of_attempts,
        }
