# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Durable HTTP request/response models (v1-compatible).

These mirror the v1 ``DurableHttpRequest`` and the ``DurableHttpResponse`` shape
returned by ``context.call_http``. In v1 the Durable Functions host extension
executed the request natively; here the request is carried to a built-in
activity that performs the call, so these models double as the (JSON) wire
payload exchanged with that activity.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Optional, cast

from durabletask.task import RetryPolicy

from ..internal.compat.token_source import TokenSource


class HttpRetryOptions(RetryPolicy):
    """Durable failure retries for the initial HTTP request, not Location polls.

    Durations use ``timedelta``, as in :class:`durabletask.task.RetryPolicy`.
    ``max_number_of_attempts`` includes the initial attempt. Backoff defaults to
    1, the maximum retry interval to six days, and the retry timeout to unbounded.
    An omitted or empty ``status_codes_to_retry`` selects every non-2xx
    response; a nonempty list selects exactly those codes (even 202).
    Transport and other activity failures are retried regardless of this list.
    Exhaustion fails ``call_http`` rather than returning a selected response.
    """

    def __init__(self, *,
                 first_retry_interval: timedelta,
                 max_number_of_attempts: int,
                 backoff_coefficient: float | None = 1.0,
                 max_retry_interval: timedelta | None = timedelta(days=6),
                 retry_timeout: timedelta | None = None,
                 status_codes_to_retry: Optional[list[int]] = None) -> None:
        super().__init__(
            first_retry_interval=first_retry_interval,
            max_number_of_attempts=max_number_of_attempts,
            backoff_coefficient=backoff_coefficient,
            max_retry_interval=max_retry_interval,
            retry_timeout=retry_timeout)
        self._status_codes_to_retry = (
            list(status_codes_to_retry) if status_codes_to_retry is not None else None)

    @property
    def status_codes_to_retry(self) -> Optional[list[int]]:
        """Return a copy of the selected response codes, or ``None``."""
        return (list(self._status_codes_to_retry)
                if self._status_codes_to_retry is not None else None)

    def to_json(self) -> dict[str, Any]:
        """Snapshot the policy into a JSON-serializable wire payload."""
        millisecond = timedelta(milliseconds=1)
        return {
            "firstRetryIntervalInMilliseconds": self.first_retry_interval / millisecond,
            "maxNumberOfAttempts": self.max_number_of_attempts,
            "backoffCoefficient": self.backoff_coefficient,
            "maxRetryIntervalInMilliseconds": (
                self.max_retry_interval / millisecond if self.max_retry_interval is not None else None),
            "retryTimeoutInMilliseconds": (
                self.retry_timeout / millisecond if self.retry_timeout is not None else None),
            "statusCodesToRetry": self.status_codes_to_retry,
        }

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> "HttpRetryOptions":
        """Reconstruct a snapshotted policy from its wire payload."""
        max_interval = value["maxRetryIntervalInMilliseconds"]
        timeout = value["retryTimeoutInMilliseconds"]
        return cls(
            first_retry_interval=timedelta(milliseconds=value["firstRetryIntervalInMilliseconds"]),
            max_number_of_attempts=value["maxNumberOfAttempts"],
            backoff_coefficient=value["backoffCoefficient"],
            max_retry_interval=timedelta(milliseconds=max_interval) if max_interval is not None else None,
            retry_timeout=timedelta(milliseconds=timeout) if timeout is not None else None,
            status_codes_to_retry=value.get("statusCodesToRetry"))


class DurableHttpRequest:
    """Data structure representing a durable HTTP request."""

    def __init__(self,
                 method: str,
                 uri: str,
                 content: Optional[str] = None,
                 headers: Optional[dict[str, str]] = None,
                 token_source: Optional[TokenSource] = None,
                 retry_options: Optional[HttpRetryOptions] = None):
        self._method = method
        self._uri = uri
        self._content = content
        self._headers = headers
        self._token_source = token_source
        self._retry_options = retry_options.to_json() if retry_options is not None else None

    @property
    def method(self) -> str:
        """Get the HTTP request method."""
        return self._method

    @property
    def uri(self) -> str:
        """Get the HTTP request uri."""
        return self._uri

    @property
    def content(self) -> Optional[str]:
        """Get the HTTP request content."""
        return self._content

    @property
    def headers(self) -> Optional[dict[str, str]]:
        """Get the HTTP request headers."""
        return self._headers

    @property
    def token_source(self) -> Optional[TokenSource]:
        """Get the source of the OAuth token to add to the request."""
        return self._token_source

    @property
    def retry_options(self) -> Optional[HttpRetryOptions]:
        """Get a copy of the policy captured when this request was created."""
        return (HttpRetryOptions.from_json(self._retry_options)
                if self._retry_options is not None else None)

    def to_json(self) -> dict[str, Any]:
        """Convert this request into a JSON-serializable dictionary."""
        json_dict: dict[str, Any] = {"method": self._method, "uri": self._uri}
        if self._content is not None:
            json_dict["content"] = self._content
        if self._headers is not None:
            json_dict["headers"] = dict(self._headers)
        if self._token_source is not None:
            json_dict["tokenSource"] = self._token_source.to_json()
        retry_options = self.retry_options
        if retry_options is not None:
            json_dict["retryOptions"] = retry_options.to_json()
        return json_dict


class DurableHttpResponse:
    """Data structure representing a durable HTTP response.

    Returned from ``context.call_http``. Exposes ``status_code``, ``headers``
    and ``content`` as attributes, matching the v1 access pattern
    (``response.status_code`` / ``response.content``).
    """

    def __init__(self,
                 status_code: int,
                 headers: Optional[dict[str, str]] = None,
                 content: Optional[str] = None):
        self._status_code = status_code
        self._headers = headers or {}
        self._content = content

    @property
    def status_code(self) -> int:
        """Get the HTTP response status code."""
        return self._status_code

    @property
    def headers(self) -> dict[str, str]:
        """Get the HTTP response headers."""
        return self._headers

    @property
    def content(self) -> Optional[str]:
        """Get the HTTP response content."""
        return self._content

    def to_json(self) -> dict[str, Any]:
        """Convert this response into a JSON-serializable dictionary."""
        return {
            "status_code": self._status_code,
            "headers": dict(self._headers),
            "content": self._content,
        }

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> "DurableHttpResponse":
        """Reconstruct a ``DurableHttpResponse`` from its dictionary payload.

        Accepts both the snake_case wire format produced by :meth:`to_json` and
        the camelCase ``statusCode`` key for defensiveness.
        """
        status_code = value.get("status_code", value.get("statusCode", 0))
        headers = cast("dict[str, str]", value.get("headers") or {})
        content = value.get("content")
        return cls(status_code=int(status_code), headers=headers, content=content)
