# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from datetime import datetime, timedelta
from typing import Optional

class AccessTokenManager:
    def __init__(self, refresh_buffer: int = 60, use_managed_identity: bool = False, client_id: Optional[str] = None):
        self.scope = "https://durabletask.io/.default"
        self.refresh_buffer = refresh_buffer
        
        # Choose the appropriate credential based on use_managed_identity
        if use_managed_identity:
            if not client_id:
                print("Using System Assigned Managed Identity for authentication.")
                self.credential = ManagedIdentityCredential()
            else:
                print("Using User Assigned Managed Identity for authentication.")
                self.credential = ManagedIdentityCredential(client_id)
        else:
            self.credential = DefaultAzureCredential()
            print("Using Default Azure Credentials for authentication.")
        
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