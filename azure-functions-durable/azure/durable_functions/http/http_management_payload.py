# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
from typing import Any, Mapping
from urllib.parse import quote, urlsplit, urlunsplit


_INSTANCE_ID_PLACEHOLDER = "INSTANCEID"


class HttpManagementPayload(dict[str, str]):
    """A class representing the HTTP management payload for a Durable Function orchestration instance.

    Contains URLs for managing the instance, such as querying status,
    sending events, terminating, restarting, etc.

    Subclasses ``dict`` for backwards compatibility with the v1 API, which
    returned a plain ``dict``. As a result the payload supports mapping-style
    access (``payload["statusQueryGetUri"]``, iteration, ``in``,
    ``.keys()``/``.items()``/``.values()``) and is directly JSON-serializable
    via ``json.dumps(payload)``.
    """

    def __init__(
            self,
            instance_id: str,
            instance_status_url: str,
            required_query_string_parameters: str,
            *,
            management_urls: Mapping[str, str] | None = None,
            request_origin: str | None = None):
        """Initializes the HttpManagementPayload with the necessary URLs.

        Args:
            instance_id (str): The ID of the Durable Function instance.
            instance_status_url (str): The base URL for the instance status.
            required_query_string_parameters (str): The required URL parameters provided by the Durable extension.
            management_urls (Mapping[str, str] | None): Canonical URL templates
                provided by the Durable extension.
            request_origin (str | None): Externally visible request origin used
                to replace the templates' internal origin.
        """
        fallback_urls = {
            'purgeHistoryDeleteUri': instance_status_url + "?" + required_query_string_parameters,
            'restartPostUri': instance_status_url + "/restart?" + required_query_string_parameters,
            'sendEventPostUri': instance_status_url + "/raiseEvent/{eventName}?" + required_query_string_parameters,
            'statusQueryGetUri': instance_status_url + "?" + required_query_string_parameters,
            'terminatePostUri': instance_status_url + "/terminate?reason={text}&" + required_query_string_parameters,
            'rewindPostUri': instance_status_url + "/rewind?reason={text}&" + required_query_string_parameters,
            'resumePostUri': instance_status_url + "/resume?reason={text}&" + required_query_string_parameters,
            'suspendPostUri': instance_status_url + "/suspend?reason={text}&" + required_query_string_parameters,
        }
        templates = management_urls or {}
        placeholder = templates.get("id") or _INSTANCE_ID_PLACEHOLDER
        encoded_instance_id = quote(instance_id, safe="")

        urls = {'id': instance_id}
        for name, fallback_url in fallback_urls.items():
            template = templates.get(name)
            if not template:
                urls[name] = fallback_url
                continue

            url = template.replace(placeholder, encoded_instance_id)
            if placeholder != _INSTANCE_ID_PLACEHOLDER:
                url = url.replace(_INSTANCE_ID_PLACEHOLDER, encoded_instance_id)
            urls[name] = replace_url_origin(url, request_origin)

        super().__init__(urls)

    def __str__(self) -> str:
        return json.dumps(self)

    @property
    def urls(self) -> dict[str, Any]:
        """Return the management URLs as a plain ``dict`` (v1 compatibility)."""
        return dict(self)

    def to_json(self) -> dict[str, Any]:
        """Return the management URLs as a plain ``dict``."""
        return dict(self)


def replace_url_origin(url: str, request_origin: str | None) -> str:
    if request_origin is None:
        return url

    parsed_url = urlsplit(url)
    parsed_origin = urlsplit(request_origin)
    if not parsed_origin.scheme or not parsed_origin.netloc:
        raise ValueError(
            "request_origin must include both a scheme and an authority")
    if not parsed_url.scheme or not parsed_url.netloc:
        return url

    return urlunsplit((
        parsed_origin.scheme,
        parsed_origin.netloc,
        parsed_url.path,
        parsed_url.query,
        parsed_url.fragment,
    ))
