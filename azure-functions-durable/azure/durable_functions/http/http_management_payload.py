# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
from collections.abc import Iterator


class HttpManagementPayload:
    """A class representing the HTTP management payload for a Durable Function orchestration instance.

    Contains URLs for managing the instance, such as querying status,
    sending events, terminating, restarting, etc.

    Supports mapping-style access (``payload["statusQueryGetUri"]``, iteration,
    ``in``, ``.keys()``/``.items()``/``.values()``) for backwards compatibility
    with the v1 API, which returned a plain ``dict``.
    """

    def __init__(self, instance_id: str, instance_status_url: str, required_query_string_parameters: str):
        """Initializes the HttpManagementPayload with the necessary URLs.

        Args:
            instance_id (str): The ID of the Durable Function instance.
            instance_status_url (str): The base URL for the instance status.
            required_query_string_parameters (str): The required URL parameters provided by the Durable extension.
        """
        self.urls = {
            'id': instance_id,
            'purgeHistoryDeleteUri': instance_status_url + "?" + required_query_string_parameters,
            'restartPostUri': instance_status_url + "/restart?" + required_query_string_parameters,
            'sendEventPostUri': instance_status_url + "/raiseEvent/{eventName}?" + required_query_string_parameters,
            'statusQueryGetUri': instance_status_url + "?" + required_query_string_parameters,
            'terminatePostUri': instance_status_url + "/terminate?reason={text}&" + required_query_string_parameters,
            'resumePostUri': instance_status_url + "/resume?reason={text}&" + required_query_string_parameters,
            'suspendPostUri': instance_status_url + "/suspend?reason={text}&" + required_query_string_parameters
        }

    def __str__(self):
        return json.dumps(self.urls)

    def __getitem__(self, key: str) -> str:
        return self.urls[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.urls)

    def __len__(self) -> int:
        return len(self.urls)

    def __contains__(self, key: object) -> bool:
        return key in self.urls

    def keys(self):
        """Return the management URL keys."""
        return self.urls.keys()

    def items(self):
        """Return the management URL (key, value) pairs."""
        return self.urls.items()

    def values(self):
        """Return the management URL values."""
        return self.urls.values()
