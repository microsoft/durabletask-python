# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from datetime import datetime, timedelta, timezone
from typing import Optional

from azure.core.credentials import AccessToken, TokenCredential

import durabletask.internal.shared as shared


# By default, when there's 10minutes left before the token expires, refresh the token
class AccessTokenManager:

    _token: Optional[AccessToken]

    def __init__(self, token_credential: Optional[TokenCredential], refresh_interval_seconds: int = 600):
        self._scope = "https://durabletask.io/.default"
        self._refresh_interval_seconds = refresh_interval_seconds
        self._logger = shared.get_logger("token_manager")

        self._credential = token_credential

        if self._credential is not None:
            self._token = self._credential.get_token(self._scope)
            self.expiry_time = datetime.fromtimestamp(self._token.expires_on, tz=timezone.utc)
        else:
            self._token = None
            self.expiry_time = None

    def get_access_token(self) -> Optional[AccessToken]:
        if self._token is None or self.is_token_expired():
            self.refresh_token()
        return self._token

    # Checks if the token is expired, or if it will expire in the next "refresh_interval_seconds" seconds.
    # For example, if the token is created to have a lifespan of 2 hours, and the refresh buffer is set to 30 minutes,
    # We will grab a new token when there're 30minutes left on the lifespan of the token
    def is_token_expired(self) -> bool:
        if self.expiry_time is None:
            return True
        return datetime.now(timezone.utc) >= (self.expiry_time - timedelta(seconds=self._refresh_interval_seconds))

    def refresh_token(self):
        if self._credential is not None:
            self._token = self._credential.get_token(self._scope)

            # Convert UNIX timestamp to timezone-aware datetime
            self.expiry_time = datetime.fromtimestamp(self._token.expires_on, tz=timezone.utc)
            self._logger.debug(f"Token refreshed. Expires at: {self.expiry_time}")
