# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
from datetime import datetime, timedelta, timezone
from threading import Lock

from azure.core.credentials import AccessToken, TokenCredential
from azure.core.credentials_async import AsyncTokenCredential

import durabletask.internal.shared as shared


# By default, when there's 10minutes left before the token expires, refresh the token
class AccessTokenManager:

    _token: AccessToken | None
    expiry_time: datetime | None

    def __init__(self, token_credential: TokenCredential | None, refresh_interval_seconds: int = 600):
        self._scope = "https://durabletask.io/.default"
        self._refresh_interval_seconds = refresh_interval_seconds
        self._logger = shared.get_logger("token_manager")

        self._credential = token_credential
        self._refresh_lock = Lock()

        # Token acquisition is deferred to the first get_access_token() call so that
        # constructing a client or worker does not perform a blocking credential round
        # trip. The deferred first acquisition still goes through the double-checked
        # refresh lock below, so it remains single-flight across threads.
        self._token = None
        self.expiry_time = None

    def get_access_token(self) -> AccessToken | None:
        if self._token is None or self.is_token_expired():
            with self._refresh_lock:
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


class AsyncAccessTokenManager:
    """Async version of AccessTokenManager that uses AsyncTokenCredential.

    This avoids blocking the event loop when acquiring or refreshing tokens."""

    _token: AccessToken | None

    def __init__(self, token_credential: AsyncTokenCredential | None,
                 refresh_interval_seconds: int = 600):
        self._scope = "https://durabletask.io/.default"
        self._refresh_interval_seconds = refresh_interval_seconds
        self._logger = shared.get_logger("async_token_manager")

        self._credential = token_credential
        self._token = None
        self.expiry_time = None

        # An asyncio.Lock binds itself to the event loop it is first used on, and this
        # manager may outlive a loop or be shared across loops. Locks are therefore
        # created lazily per running loop, guarded by a plain threading lock because
        # different loops may run on different threads.
        self._refresh_locks: dict[asyncio.AbstractEventLoop, asyncio.Lock] = {}
        self._refresh_locks_guard = Lock()

    def _get_refresh_lock(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        with self._refresh_locks_guard:
            lock = self._refresh_locks.get(loop)
            if lock is None:
                # Discard locks belonging to loops that are no longer usable so the
                # mapping does not grow without bound.
                stale_loops = [
                    existing for existing in self._refresh_locks if existing.is_closed()
                ]
                for stale_loop in stale_loops:
                    del self._refresh_locks[stale_loop]
                lock = asyncio.Lock()
                self._refresh_locks[loop] = lock
            return lock

    async def get_access_token(self) -> AccessToken | None:
        if self._token is None or self.is_token_expired():
            async with self._get_refresh_lock():
                # Re-check under the lock: a concurrent caller may have already
                # refreshed the token while this one was waiting, so only a single
                # credential request is made per refresh window.
                if self._token is None or self.is_token_expired():
                    await self.refresh_token()
        return self._token

    def is_token_expired(self) -> bool:
        if self.expiry_time is None:
            return True
        return datetime.now(timezone.utc) >= (
            self.expiry_time - timedelta(seconds=self._refresh_interval_seconds))

    async def refresh_token(self):
        if self._credential is not None:
            self._token = await self._credential.get_token(self._scope)

            # Convert UNIX timestamp to timezone-aware datetime
            self.expiry_time = datetime.fromtimestamp(self._token.expires_on, tz=timezone.utc)
            self._logger.debug(f"Token refreshed. Expires at: {self.expiry_time}")
