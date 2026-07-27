# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
from typing import Any


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

    def __init__(self, instance_id: str, instance_status_url: str, required_query_string_parameters: str):
        """Initializes the HttpManagementPayload with the necessary URLs.

        Args:
            instance_id (str): The ID of the Durable Function instance.
            instance_status_url (str): The base URL for the instance status.
            required_query_string_parameters (str): The required URL parameters provided by the Durable extension.
        """
        super().__init__({
            'id': instance_id,
            'purgeHistoryDeleteUri': instance_status_url + "?" + required_query_string_parameters,
            'restartPostUri': instance_status_url + "/restart?" + required_query_string_parameters,
            'sendEventPostUri': instance_status_url + "/raiseEvent/{eventName}?" + required_query_string_parameters,
            'statusQueryGetUri': instance_status_url + "?" + required_query_string_parameters,
            'terminatePostUri': instance_status_url + "/terminate?reason={text}&" + required_query_string_parameters,
            'resumePostUri': instance_status_url + "/resume?reason={text}&" + required_query_string_parameters,
            'suspendPostUri': instance_status_url + "/suspend?reason={text}&" + required_query_string_parameters
        })

    def __str__(self) -> str:
        return json.dumps(self)

    @property
    def urls(self) -> dict[str, Any]:
        """Return the management URLs as a plain ``dict`` (v1 compatibility)."""
        return dict(self)

    def to_json(self) -> dict[str, Any]:
        """Return the management URLs as a plain ``dict``."""
        return dict(self)
