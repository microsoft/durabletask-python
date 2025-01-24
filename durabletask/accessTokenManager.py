# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from azure.identity import DefaultAzureCredential
from datetime import datetime, timedelta

class AccessTokenManager:
    def __init__(self, scope: str, refresh_buffer: int = 60):
        self.scope = scope
        self.refresh_buffer = refresh_buffer
        self.credential = DefaultAzureCredential()
        self.token = None
        self.expiry_time = None

    def get_access_token(self) -> str:
        if self.token is None or self.is_token_expired():
            self.refresh_token()
        return self.token

    def is_token_expired(self) -> bool:
        if self.expiry_time is None:
            return True
        return datetime.utcnow() >= (self.expiry_time - timedelta(seconds=self.refresh_buffer))

    def refresh_token(self):
        new_token = self.credential.get_token(self.scope)
        self.token = f"Bearer {new_token.token}"
        self.expiry_time = datetime.utcnow() + timedelta(seconds=new_token.expires_on - int(datetime.utcnow().timestamp()))
        print(f"Token refreshed. Expires at: {self.expiry_time}")