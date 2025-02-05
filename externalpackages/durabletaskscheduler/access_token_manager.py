# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from datetime import datetime, timedelta, timezone
from typing import Optional
import durabletask.internal.shared as shared

# By default, when there's 10minutes left before the token expires, refresh the token
class AccessTokenManager:
    def __init__(self, refresh_buffer: int = 600, metadata: Optional[list[tuple[str, str]]] = None):
        self.scope = "https://durabletask.io/.default"
        self.refresh_buffer = refresh_buffer
        self._use_managed_identity = False
        self._metadata = metadata
        self._client_id = None
        self._logger = shared.get_logger("token_manager")

        if metadata:  # Ensure metadata is not None
            for key, value in metadata:
                if key == "use_managed_identity":
                    self._use_managed_identity = value.lower() == "true"  # Properly convert string to bool
                elif key == "client_id":
                    self._client_id = value  # Directly assign string

        # Choose the appropriate credential based on use_managed_identity
        if self._use_managed_identity:
            if not self._client_id:
                self._logger.debug("Using System Assigned Managed Identity for authentication.")
                self.credential = ManagedIdentityCredential()
            else:
                self._logger.debug("Using User Assigned Managed Identity for authentication.")
                self.credential = ManagedIdentityCredential(client_id=self._client_id)
        else:
            self.credential = DefaultAzureCredential()
            self._logger.debug("Using Default Azure Credentials for authentication.")
        
        self.token = None
        self.expiry_time = None

    def get_access_token(self) -> str:
        if self.token is None or self.is_token_expired():
            self.refresh_token()
        return self.token

    # Checks if the token is expired, or if it will expire in the next "refresh_buffer" seconds.
    # For example, if the token is created to have a lifespan of 2 hours, and the refresh buffer is set to 30 minutes,
    # We will grab a new token when there're 30minutes left on the lifespan of the token
    def is_token_expired(self) -> bool:
        if self.expiry_time is None:
            return True
        return datetime.now(timezone.utc) >= (self.expiry_time - timedelta(seconds=self.refresh_buffer))

    def refresh_token(self):
        new_token = self.credential.get_token(self.scope)
        self.token = f"Bearer {new_token.token}"
        
        # Convert UNIX timestamp to timezone-aware datetime
        self.expiry_time = datetime.fromtimestamp(new_token.expires_on, tz=timezone.utc)
        self._logger.debug(f"Token refreshed. Expires at: {self.expiry_time}")