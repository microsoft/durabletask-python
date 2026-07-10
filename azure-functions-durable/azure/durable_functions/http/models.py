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

from typing import Any, Optional, cast

from ..internal.compat.token_source import TokenSource


class DurableHttpRequest:
    """Data structure representing a durable HTTP request."""

    def __init__(self,
                 method: str,
                 uri: str,
                 content: Optional[str] = None,
                 headers: Optional[dict[str, str]] = None,
                 token_source: Optional[TokenSource] = None):
        self._method = method
        self._uri = uri
        self._content = content
        self._headers = headers
        self._token_source = token_source

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

    def to_json(self) -> dict[str, Any]:
        """Convert this request into a JSON-serializable dictionary."""
        json_dict: dict[str, Any] = {"method": self._method, "uri": self._uri}
        if self._content is not None:
            json_dict["content"] = self._content
        if self._headers is not None:
            json_dict["headers"] = dict(self._headers)
        if self._token_source is not None:
            # TokenSource exposes ``to_json`` (e.g. ManagedIdentityTokenSource).
            json_dict["tokenSource"] = self._token_source.to_json()  # type: ignore[attr-defined]  # noqa: E501
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
