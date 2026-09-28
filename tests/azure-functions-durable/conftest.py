# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Fixtures for the independently runnable Functions provider test suite."""

import pytest

from durabletask.payload import LargePayloadStorageOptions, PayloadStore


class FakePayloadStore(PayloadStore):
    """In-memory storage with recognizable references and configurable limits."""

    def __init__(self, threshold_bytes: int = 100,
                 max_stored_payload_bytes: int = 10 * 1024 * 1024) -> None:
        self._options = LargePayloadStorageOptions(
            threshold_bytes=threshold_bytes,
            max_stored_payload_bytes=max_stored_payload_bytes,
            enable_compression=False,
        )
        self._blobs: dict[str, bytes] = {}

    @property
    def options(self) -> LargePayloadStorageOptions:
        return self._options

    def upload(self, data: bytes, *, instance_id: str | None = None) -> str:
        token = f"blob:v1:test-container:blob-{len(self._blobs)}"
        self._blobs[token] = data
        return token

    async def upload_async(self, data: bytes, *, instance_id: str | None = None) -> str:
        return self.upload(data, instance_id=instance_id)

    def download(self, token: str) -> bytes:
        return self._blobs[token]

    async def download_async(self, token: str) -> bytes:
        return self.download(token)

    def is_known_token(self, value: str) -> bool:
        return value.startswith("blob:v1:test-container:")


@pytest.fixture
def payload_store_factory() -> type[FakePayloadStore]:
    return FakePayloadStore
