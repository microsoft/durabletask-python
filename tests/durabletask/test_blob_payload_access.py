# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Offline tests of payload destinations through the real Azure Storage pipeline."""

import asyncio
import gzip
import time
from unittest.mock import Mock
from urllib.parse import quote, urlsplit

import pytest

pytest.importorskip("azure.storage.blob")

from azure.core.credentials import AccessToken, AzureNamedKeyCredential, AzureSasCredential  # noqa: E402
from azure.core.exceptions import HttpResponseError  # noqa: E402
from azure.core.pipeline.transport import (  # noqa: E402
    AioHttpTransport, AsyncHttpResponse, HttpResponse, RequestsTransport,
)
from requests import Request  # noqa: E402
from requests.structures import CaseInsensitiveDict  # noqa: E402

from durabletask.extensions.azure_blob_payloads import BlobPayloadStore, BlobPayloadStoreOptions  # noqa: E402
from durabletask.internal import history_helpers  # noqa: E402
from durabletask.internal import orchestrator_service_pb2 as pb  # noqa: E402
from durabletask.payload import deexternalize_payloads, deexternalize_payloads_async  # noqa: E402
from google.protobuf.wrappers_pb2 import StringValue  # noqa: E402


PRIMARY = "https://primary.blob.core.windows.net"
ADDITIONAL = "https://archive.blob.core.windows.net"


class RequestObserved(Exception):
    """Stops a real Azure pipeline at the HTTP transport without network I/O."""


class RecordingCredential:
    def __init__(self):
        self.calls = []

    def get_token(self, *scopes, **kwargs):
        self.calls.append(scopes)
        return AccessToken("synthetic-token", int(time.time()) + 3600)


@pytest.fixture
def offline_storage(monkeypatch):
    requests = []

    def record(request):
        prepared = Request(request.method, request.url, headers=dict(request.headers)).prepare()
        requests.append(prepared)
        raise RequestObserved()

    def send(self, request, **kwargs):
        return record(request)

    async def send_async(self, request, **kwargs):
        return record(request)

    monkeypatch.setattr(RequestsTransport, "send", send)
    monkeypatch.setattr(AioHttpTransport, "send", send_async)
    return requests


@pytest.fixture
def store_factory():
    stores = []

    def create(**kwargs):
        options = BlobPayloadStoreOptions(
            account_url=PRIMARY, credential=RecordingCredential(), enable_compression=False)
        for name, value in kwargs.items():
            setattr(options, name, value)
        store = BlobPayloadStore(options)
        stores.append(store)
        return store

    yield create
    for store in stores:
        store.close()
        asyncio.run(store.close_async())


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("account_wide", [False, True])
@pytest.mark.parametrize("additional", [False, True])
@pytest.mark.parametrize("reference,location", [
    ("blob:v1:durabletask-payloads:folder/a b%2Fc", "primary-container"),
    ("blob:v1:other-container:blob", "primary-other-container"),
    (f"blob:v2:{PRIMARY}/durabletask-payloads/blob", "primary-container"),
    (f"blob:v2:{PRIMARY}/other-container/blob", "primary-other-container"),
    (f"blob:v2:{ADDITIONAL}/durabletask-payloads/blob", "additional"),
    (f"blob:v2:{ADDITIONAL}/other-container/blob", "additional"),
    ("blob:v2:https://unlisted.blob.core.windows.net/other-container/blob", "unlisted"),
    ("blob:v2:https://primary.blob.core.windows.net:444/other-container/blob", "unlisted"),
])
async def test_access_scopes_before_http_and_credentials(
        offline_storage, store_factory, use_async, account_wide, additional, reference, location):
    store = store_factory(
        allow_all_containers_in_configured_account=account_wide,
        additional_account_urls=[ADDITIONAL] if additional else [],
    )
    credential = store.options.credential
    assert not credential.calls and not offline_storage
    assert store.is_known_token(reference)
    allowed = (location == "primary-container"
               or location == "primary-other-container" and account_wide
               or location == "additional" and additional)
    with pytest.raises(RequestObserved if allowed else ValueError):
        if use_async:
            await store.download_async(reference)
        else:
            store.download(reference)
    if allowed:
        assert credential.calls == [("https://storage.azure.com/.default",)]
        assert len(offline_storage) == 1
        request = offline_storage[0]
        assert request.headers["Authorization"] == "Bearer synthetic-token"
        expected = (f"{PRIMARY}/durabletask-payloads/folder/a%20b%252Fc" if reference.startswith(
            "blob:v1:durabletask-payloads:") else f"{PRIMARY}/other-container/blob"
            if reference.startswith("blob:v1:") else reference.removeprefix("blob:v2:"))
        assert request.url == expected
    else:
        assert not credential.calls and not offline_storage


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("account_wide", [False, True])
@pytest.mark.parametrize("reference,allowed", [
    ("https://PRIMARY.blob.core.windows.net:443/durabletask-payloads/a%20b%252Fc", True),
    ("https://primary.blob.core.windows.net/%64urabletask-payloads/blob", True),
    ("https://primary.blob.core.windows.net/durabletask-payloads/folder%2Fblob", True),
    ("https://primary.blob.core.windows.net/durabletask-payloads/literal/%252e%252e/blob", True),
    ("https://primary.blob.core.windows.net/durabletask-payloads/../other/blob", False),
    ("https://primary.blob.core.windows.net/durabletask-payloads/%2E%2e/other/blob", False),
    ("https://primary.blob.core.windows.net/durabletask-payloads/a%2F..%2F..%2Fother/blob", False),
    ("https://primary.blob.core.windows.net/durabletask-payloads/blob?sig=synthetic", True),
    ("https://primary.blob.core.windows.net/durabletask-payloads/blob#part", True),
    ("https://primary.blob.core.windows.net/durabletask-payloads/blob?", True),
    ("https://primary.blob.core.windows.net/durabletask-payloads/blob#", True),
    ("https://primary.blob.core.windows.net@other.invalid/durabletask-payloads/blob", False),
    ("https://primary.blob.core.windows.net%2Eother.invalid/durabletask-payloads/blob", False),
    ("https://primary.blob.core.windows.net.other.invalid/durabletask-payloads/blob", False),
    ("https://primary.blob.core.windows.net/durabletask-payloads%2ffolder/blob", False),
    ("http://primary.blob.core.windows.net/durabletask-payloads/blob", False),
    ("https://primary.blob.core.windows.net:444/durabletask-payloads/blob", False),
    ("https://primary.blob.core.windows.net/durabletask-payloads-other/blob", None),
])
async def test_url_boundaries(offline_storage, store_factory, use_async, account_wide, reference, allowed):
    store = store_factory(allow_all_containers_in_configured_account=account_wide, additional_account_urls=[ADDITIONAL])
    if allowed is None:
        allowed = account_wide
    with pytest.raises(RequestObserved if allowed else ValueError):
        if use_async:
            await store.download_async(f"blob:v2:{reference}")
        else:
            store.download(f"blob:v2:{reference}")
    assert bool(store.options.credential.calls) == allowed
    assert bool(offline_storage) == allowed


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("reference,allowed", [
    ("https://localhost:10443/primary/durabletask-payloads/blob", True),
    ("https://localhost:10443/primary/other-container/blob", True),
    ("https://localhost:10443/%70rimary/other-container/blob", True),
    ("https://localhost:10443/Primary/other-container/blob", False),
    ("https://localhost:10443/primary%2Fother-container/blob", False),
    ("https://localhost:10443/archive%2Fother-container/blob", False),
    ("https://localhost:10443/archive/other-container/blob", True),
    ("https://localhost:10443/archive-other/other-container/blob", False),
    ("https://localhost:10443/unlisted/other-container/blob", False),
    ("https://localhost:10444/archive/other-container/blob", False),
    ("http://localhost:10443/archive/other-container/blob", False),
    ("https://localhost:10443/primary/../unlisted/other-container/blob", False),
    ("https://localhost:10443/archive/%2e%2e/unlisted/other-container/blob", False),
])
async def test_path_style_account_boundaries(offline_storage, store_factory, use_async, reference, allowed):
    store = store_factory(
        account_url="https://localhost:10443/primary",
        allow_all_containers_in_configured_account=True,
        additional_account_urls=["https://localhost:10443/archive"],
    )
    with pytest.raises(RequestObserved if allowed else ValueError):
        if use_async:
            await store.download_async(f"blob:v2:{reference}")
        else:
            store.download(f"blob:v2:{reference}")
    assert bool(store.options.credential.calls) == allowed
    assert bool(offline_storage) == allowed
    if allowed:
        assert offline_storage[0].url == reference.replace("%70", "p")


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("auth", ["key", "sas", "connection-string", "anonymous"])
@pytest.mark.parametrize("version", ["v1", "v2"])
async def test_account_wide_retains_configured_authentication(
        offline_storage, store_factory, use_async, auth, version):
    credential = (AzureNamedKeyCredential("primary", "c3ludGhldGlj") if auth == "key"
                  else AzureSasCredential("sv=2025-01-05&sig=synthetic&sp=r") if auth == "sas" else None)
    store = store_factory(
        account_url=PRIMARY if auth != "connection-string" else None,
        credential=credential,
        connection_string=(f"BlobEndpoint={PRIMARY};AccountName=primary;AccountKey=c3ludGhldGlj"
                           if auth == "connection-string" else None),
        allow_all_containers_in_configured_account=True,
    )
    reference = ("blob:v1:other-container:blob" if version == "v1"
                 else f"blob:v2:{PRIMARY}/other-container/blob")
    with pytest.raises(RequestObserved):
        if use_async:
            await store.download_async(reference)
        else:
            store.download(reference)
    assert len(offline_storage) == 1
    request = offline_storage[0]
    if auth in ("key", "connection-string"):
        assert request.headers["Authorization"].startswith("SharedKey primary:")
    elif auth == "sas":
        assert "sig=synthetic" in request.url
        assert "Authorization" not in request.headers
    else:
        assert "Authorization" not in request.headers


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("auth", ["token", "key", "sas", "url-sas", "connection-string", "anonymous"])
@pytest.mark.parametrize("suffix", [
    "?sig=untrusted&snapshot=ignored&versionid=ignored#fragment",
    "#fragment?sig=untrusted",
])
async def test_legacy_v2_uses_only_configured_authentication(
        offline_storage, store_factory, use_async, auth, suffix):
    credential = (RecordingCredential() if auth == "token" else
                  AzureNamedKeyCredential("primary", "c3ludGhldGlj") if auth == "key" else
                  AzureSasCredential("sv=2025-01-05&sig=configured&sp=r") if auth == "sas" else None)
    endpoint = PRIMARY + ("?sv=2025-01-05&sig=configured&sp=r" if auth == "url-sas" else "")
    store = store_factory(
        account_url=None if auth == "connection-string" else endpoint,
        credential=credential,
        connection_string=(f"BlobEndpoint={PRIMARY};AccountName=primary;AccountKey=c3ludGhldGlj"
                           if auth == "connection-string" else None),
    )
    reference = f"blob:v2:{PRIMARY}/durabletask-payloads/blob{suffix}"
    assert store.is_known_token(reference)
    with pytest.raises(RequestObserved):
        if use_async:
            await store.download_async(reference)
        else:
            store.download(reference)
    assert len(offline_storage) == 1
    request = offline_storage[0]
    assert request.url.split("?")[0] == f"{PRIMARY}/durabletask-payloads/blob"
    assert all(value not in request.url for value in ("untrusted", "snapshot", "versionid", "#"))
    if auth in ("sas", "url-sas"):
        assert "sig=configured" in request.url and "Authorization" not in request.headers
    elif auth in ("key", "connection-string"):
        assert request.headers["Authorization"].startswith("SharedKey primary:")
    elif auth == "token":
        assert len(credential.calls) == 1 and "Authorization" in request.headers
    else:
        assert "Authorization" not in request.headers


@pytest.mark.parametrize("credential", [
    None, "c3ludGhldGlj", {"account_name": "primary", "account_key": "c3ludGhldGlj"},
    AzureNamedKeyCredential("primary", "c3ludGhldGlj"), AzureSasCredential("sig=synthetic"),
])
def test_additional_accounts_require_identity(offline_storage, store_factory, credential):
    with pytest.raises(ValueError, match="TokenCredential"):
        store_factory(credential=credential, additional_account_urls=[ADDITIONAL])
    assert not offline_storage


def test_connection_string_does_not_switch_to_credential(offline_storage, store_factory):
    credential = RecordingCredential()
    with pytest.raises(ValueError, match="connection strings"):
        store_factory(
            connection_string="UseDevelopmentStorage=true", credential=credential,
            additional_account_urls=[ADDITIONAL],
        )
    assert not offline_storage and not credential.calls


@pytest.mark.parametrize("kwargs", [
    {"allow_all_containers_in_configured_account": "False"},
    {"additional_account_urls": ADDITIONAL},
    {"additional_account_urls": None},
    {"additional_account_urls": (ADDITIONAL,)},
])
def test_scope_options_reject_ambiguous_types(offline_storage, store_factory, kwargs):
    with pytest.raises(ValueError):
        store_factory(**kwargs)
    assert not offline_storage


@pytest.mark.parametrize("url", [
    PRIMARY, PRIMARY.upper() + ":443/", "archive.blob.core.windows.net",
    "https://archive.blob.core.windows.net?sig=synthetic",
    "https://archive.blob.core.windows.net#fragment",
    "https://archive.blob.core.windows.net?", "https://archive.blob.core.windows.net#",
    "https://*.blob.core.windows.net", "*.blob.core.windows.net",
    "https://user@archive.blob.core.windows.net", "https://archive.blob.core.windows.net:0",
    "https://archive.blob.core.windows.net:65536",
    "https://archive.blob.core.windows.net/../account",
    "https://localhost:10443/account/%2e%2e/other",
    "https://localhost:10443", "https://127.0.0.1:10443", "https://[::1]:10443",
    "https://archive.blob.core.windows.net/path-account",
    "https://storage.example.net/account", "https://localhost:10443/account/container",
    "https://localhost:10443/account/container/blob", "https://[::1]:10443/account",
    "http://archive.blob.core.windows.net",
])
def test_invalid_additional_endpoint_configuration(offline_storage, store_factory, url):
    credential = RecordingCredential()
    with pytest.raises(ValueError):
        store_factory(credential=credential, additional_account_urls=[url])
    assert not offline_storage and not credential.calls


@pytest.mark.parametrize("connection_string", [False, True])
@pytest.mark.parametrize("endpoint", [
    f"{PRIMARY}/path-account", "https://storage.example.net/account",
    "https://localhost:10443/account/container", "https://[::1]:10443/account",
])
def test_configured_endpoint_path_must_survive_sdk_normalization(
        offline_storage, store_factory, connection_string, endpoint):
    credential = RecordingCredential()
    with pytest.raises(ValueError, match="endpoint"):
        store_factory(
            account_url=None if connection_string else endpoint,
            credential=credential,
            connection_string=(f"BlobEndpoint={endpoint};AccountName=primary;AccountKey=c3ludGhldGlj"
                               if connection_string else None),
        )
    assert not offline_storage and not credential.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_explicit_additional_port_is_not_a_hostname_allowlist(offline_storage, store_factory, use_async):
    store = store_factory(additional_account_urls=["https://primary.blob.core.windows.net:444"])
    with pytest.raises(ValueError):
        if use_async:
            await store.download_async("blob:v2:https://primary.blob.core.windows.net:445/other/blob")
        else:
            store.download("blob:v2:https://primary.blob.core.windows.net:445/other/blob")
    assert not offline_storage and not store.options.credential.calls
    with pytest.raises(RequestObserved):
        if use_async:
            await store.download_async("blob:v2:https://primary.blob.core.windows.net:444/other/blob")
        else:
            store.download("blob:v2:https://primary.blob.core.windows.net:444/other/blob")
    assert offline_storage[0].url == "https://primary.blob.core.windows.net:444/other/blob"


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("reference", [
    "blob:v1:../other:blob", "blob:v1:container/../../other:blob",
    "blob:v1:container%2Fother:blob",
    f"blob:v2:{PRIMARY}/%2fother/blob", f"blob:v2:{ADDITIONAL}/%2fother/blob",
])
async def test_container_boundaries_in_broader_scopes(offline_storage, store_factory, use_async, reference):
    store = store_factory(allow_all_containers_in_configured_account=True, additional_account_urls=[ADDITIONAL])
    with pytest.raises(ValueError):
        if use_async:
            await store.download_async(reference)
        else:
            store.download(reference)
    assert not offline_storage and not store.options.credential.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("operation", ["download", "upload"])
@pytest.mark.parametrize("mismatch", ["account", "container", "blob", "url"])
async def test_final_blob_client_is_checked_before_io(
        offline_storage, store_factory, monkeypatch, use_async, operation, mismatch):
    from azure.storage.blob import BlobClient
    from azure.storage.blob.aio import BlobClient as AsyncBlobClient

    store = store_factory()
    service = store._async_blob_service_client if use_async else store._blob_service_client
    container = service.get_container_client("durabletask-payloads")
    client_type = AsyncBlobClient if use_async else BlobClient
    clients = []

    def get_blob_client(name):
        blob = client_type(
            account_url=ADDITIONAL if mismatch == "account" else PRIMARY,
            container_name="other-container" if mismatch == "container" else "durabletask-payloads",
            blob_name="different" if mismatch == "blob" else name,
            credential=store.options.credential,
        )
        clients.append(blob)
        if mismatch == "url":
            monkeypatch.setattr(client_type, "url", property(
                lambda _: f"{ADDITIONAL}/durabletask-payloads/{name}"))
        return blob

    monkeypatch.setattr(container, "get_blob_client", get_blob_client)
    monkeypatch.setattr(service, "get_container_client", lambda _: container)
    try:
        with pytest.raises(ValueError, match="destination"):
            if operation == "upload":
                if use_async:
                    await store.upload_async(b"payload")
                else:
                    store.upload(b"payload")
            elif use_async:
                await store.download_async("blob:v1:durabletask-payloads:blob")
            else:
                store.download("blob:v1:durabletask-payloads:blob")
        assert not offline_storage and not store.options.credential.calls
    finally:
        for blob in clients:
            if use_async:
                await blob.close()
            else:
                blob.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("surface", ["input", "event", "history", "result"])
@pytest.mark.parametrize("suffix", ["", "?sig=ignored#fragment"])
async def test_transport_fields_use_scopes(offline_storage, store_factory, use_async, surface, suffix):
    reference = f"blob:v2:{ADDITIONAL}/other-container/blob{suffix}"
    if surface == "input":
        message = pb.CreateInstanceRequest(input=StringValue(value=reference))
    elif surface == "event":
        message = pb.HistoryEvent(eventRaised=pb.EventRaisedEvent(input=StringValue(value=reference)))
    elif surface == "history":
        message = pb.HistoryChunk(events=[
            pb.HistoryEvent(taskCompleted=pb.TaskCompletedEvent(result=StringValue(value=reference)))])
    else:
        message = pb.GetInstanceResponse(
            exists=True, orchestrationState=pb.OrchestrationState(output=StringValue(value=reference)))
    original = message.SerializeToString()
    store = store_factory()
    with pytest.raises(ValueError):
        if use_async:
            await deexternalize_payloads_async(message, store)
        else:
            deexternalize_payloads(message, store)
    assert message.SerializeToString() == original
    assert not offline_storage and not store.options.credential.calls

    allowed = store_factory(additional_account_urls=[ADDITIONAL])
    with pytest.raises(RequestObserved):
        if surface == "history":
            if use_async:
                async def chunks():
                    yield message
                await history_helpers.collect_history_events_async(chunks(), allowed)
            else:
                history_helpers.collect_history_events([message], allowed)
        elif use_async:
            await deexternalize_payloads_async(message, allowed)
        else:
            deexternalize_payloads(message, allowed)
    assert len(offline_storage) == 1


class RedirectResponse(HttpResponse):
    def __init__(self, request):
        super().__init__(request, None)
        self.status_code = 302
        self.headers = {"location": f"{ADDITIONAL}/other-container/blob", "content-length": "0"}
        self.reason = "Found"

    def body(self):
        return b""

    def read(self):
        return self.body()

    def close(self):
        pass


class AsyncRedirectResponse(AsyncHttpResponse):
    def __init__(self, request):
        super().__init__(request, None)
        self.status_code = 302
        self.headers = {"location": f"{ADDITIONAL}/other-container/blob", "content-length": "0"}
        self.reason = "Found"

    def body(self):
        return b""

    async def read(self):
        return self.body()

    async def close(self):
        pass


class PayloadResponseStream:
    def __init__(self, response):
        self.response = response
        self.content_length = int(response.headers["content-length"])

    def __iter__(self):
        yield self.response.body()

    async def __aiter__(self):
        yield self.response.body()


class PayloadResponse(RedirectResponse):
    def body(self):
        return self.internal_response

    def stream_download(self, pipeline, **kwargs):
        return PayloadResponseStream(self)


class AsyncPayloadResponse(AsyncRedirectResponse):
    def body(self):
        return self.internal_response

    def stream_download(self, pipeline, **kwargs):
        return PayloadResponseStream(self)


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("compression", [False, True])
@pytest.mark.parametrize("endpoint", [PRIMARY, "https://localhost:10443/primary"])
@pytest.mark.parametrize("instance_id", [
    "folder/nested",
    "folder/a b?query#fragment/\u96ea",
    "folder/%2e%2e/%2F/%252e%252e",
])
async def test_real_sdk_payload_round_trip(
        store_factory, monkeypatch, use_async, compression, endpoint, instance_id):
    requests = []
    stored = {}
    payload = b'{"value":"' + b"payload" * 100 + b'"}'

    def response(request, cls):
        prepared = Request(request.method, request.url, data=request.body).prepare()
        requests.append(prepared)
        if request.method == "PUT":
            stored[prepared.url] = prepared.body
            content, status = b"", 201
        else:
            assert request.method == "GET"
            content, status = stored[prepared.url], 206
        result = cls(request)
        result.internal_response = content
        result.status_code = status
        result.reason = "Created" if status == 201 else "Partial Content"
        result.headers = CaseInsensitiveDict({
            "etag": '"synthetic"', "content-length": str(len(content)),
            "x-ms-blob-type": "BlockBlob",
            "last-modified": "Tue, 03 Nov 2026 19:00:00 GMT",
        })
        if status == 206:
            result.headers["content-range"] = f"bytes 0-{len(content) - 1}/{len(content)}"
        return result

    def send(self, request, **kwargs):
        return response(request, PayloadResponse)

    async def send_async(self, request, **kwargs):
        return response(request, AsyncPayloadResponse)

    monkeypatch.setattr(RequestsTransport, "send", send)
    monkeypatch.setattr(AioHttpTransport, "send", send_async)
    store = store_factory(account_url=endpoint, enable_compression=compression)
    store._ensure_container_created = True
    token = (await store.upload_async(payload, instance_id=instance_id) if use_async
             else store.upload(payload, instance_id=instance_id))
    location = token.removeprefix("blob:v2:")
    blob_name = store._get_download_destination(token)[2]
    assert blob_name.startswith(instance_id + "/")
    assert location == f"{endpoint}/durabletask-payloads/{quote(blob_name, safe='/')}"
    assert (gzip.decompress(stored[location]) if compression else stored[location]) == payload
    for reference in (token, f"blob:v1:durabletask-payloads:{blob_name}"):
        actual = await store.download_async(reference) if use_async else store.download(reference)
        assert actual == payload
    assert [request.method for request in requests] == ["PUT", "GET", "GET"]
    assert all(request.url == location for request in requests)
    expected_path = f"{urlsplit(endpoint).path}/durabletask-payloads/{quote(blob_name, safe='/')}"
    assert all(urlsplit(request.url).path == expected_path for request in requests)


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("instance_id", [".", "..", "folder/../child", "folder/./child"])
async def test_invalid_upload_paths_fail_before_credentials_and_http(
        offline_storage, store_factory, use_async, instance_id):
    from durabletask.internal.exceptions import PayloadValidationError

    store = store_factory()
    with pytest.raises(PayloadValidationError, match="path segments") as failure:
        if use_async:
            await store.upload_async(b"payload", instance_id=instance_id)
        else:
            store.upload(b"payload", instance_id=instance_id)
    assert isinstance(failure.value, ValueError)
    assert not offline_storage and not store.options.credential.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("auth", ["token", "sas"])
async def test_uploads_remain_primary_and_emit_only_location(store_factory, monkeypatch, use_async, auth):
    requests = []

    def response(request, cls):
        requests.append(Request(request.method, request.url).prepare())
        result = cls(request)
        result.status_code, result.reason = 201, "Created"
        result.headers = {"etag": '"synthetic"', "content-length": "0"}
        return result

    def send(self, request, **kwargs):
        return response(request, RedirectResponse)

    async def send_async(self, request, **kwargs):
        return response(request, AsyncRedirectResponse)

    monkeypatch.setattr(AioHttpTransport, "send", send_async)
    monkeypatch.setattr(RequestsTransport, "send", send)
    store = store_factory(
        allow_all_containers_in_configured_account=True,
        additional_account_urls=[ADDITIONAL] if auth == "token" else [],
        credential=RecordingCredential() if auth == "token" else AzureSasCredential("sig=configured"),
    )
    store._ensure_container_created = True
    instance_id = "folder/a b%2Fc?query#fragment/\u96ea"
    token = (await store.upload_async(b"payload", instance_id=instance_id) if use_async
             else store.upload(b"payload", instance_id=instance_id))
    assert token.startswith(f"blob:v2:{PRIMARY}/durabletask-payloads/folder/")
    assert all(value not in token for value in ("?", "#", "@", "sig="))
    assert store._get_download_destination(token)[2].startswith(instance_id + "/")
    assert len(requests) == 1 and requests[0].method == "PUT"
    assert requests[0].url.startswith(token.removeprefix("blob:v2:"))
    assert ("sig=configured" in requests[0].url) == (auth == "sas")


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("status", [302, 403])
@pytest.mark.parametrize("endpoint", [PRIMARY, ADDITIONAL])
async def test_storage_responses_never_redirect_or_change_identity(
        store_factory, monkeypatch, use_async, status, endpoint):
    requests = []

    def response(request, cls):
        requests.append(request.url)
        result = cls(request)
        result.status_code = status
        if status == 403:
            result.reason = "Forbidden"
            result.headers["x-ms-error-code"] = "AuthorizationPermissionMismatch"
        return result

    def send(self, request, **kwargs):
        return response(request, RedirectResponse)

    async def send_async(self, request, **kwargs):
        return response(request, AsyncRedirectResponse)

    monkeypatch.setattr(AioHttpTransport, "send", send_async)
    monkeypatch.setattr(RequestsTransport, "send", send)
    store = store_factory(additional_account_urls=[ADDITIONAL])
    reference = f"blob:v2:{endpoint}/durabletask-payloads/blob"
    with pytest.raises(HttpResponseError):
        if use_async:
            await store.download_async(reference)
        else:
            store.download(reference)
    assert requests == [f"{endpoint}/durabletask-payloads/blob"]
    assert len(store.options.credential.calls) == 1


def test_access_scopes_are_store_local_snapshots(store_factory):
    first = store_factory()
    first.options.allow_all_containers_in_configured_account = True
    first.options.additional_account_urls.append(ADDITIONAL)
    second = store_factory()
    assert second.options.additional_account_urls == []
    for store in (first, second):
        with pytest.raises(ValueError):
            store._get_download_destination("blob:v1:other-container:blob")
        with pytest.raises(ValueError):
            store._get_download_destination(f"blob:v2:{ADDITIONAL}/other-container/blob")


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_selected_endpoints_and_authentication_are_snapshots(offline_storage, store_factory, use_async):
    credential = RecordingCredential()
    store = store_factory(credential=credential, additional_account_urls=[ADDITIONAL])
    store.options.account_url = "https://unlisted.blob.core.windows.net"
    store.options.container_name = "other-container"
    store.options.credential = RecordingCredential()
    store.options.additional_account_urls[:] = [store.options.account_url]
    with pytest.raises(ValueError, match="configured container"):
        store._get_download_destination("blob:v1:other-container:blob")
    reference = f"blob:v2:{ADDITIONAL}/other-container/blob"
    with pytest.raises(RequestObserved):
        if use_async:
            await store.download_async(reference)
        else:
            store.download(reference)
    assert len(credential.calls) == 1
    assert not store.options.credential.calls
    assert len(offline_storage) == 1 and offline_storage[0].url == reference.removeprefix("blob:v2:")


def test_additional_clients_are_closed_and_share_identity(store_factory):
    store = store_factory(additional_account_urls=[ADDITIONAL, ADDITIONAL.upper() + ":443/"])
    assert len(store._blob_service_clients) == len(store._async_blob_service_clients) == 2
    sync_close = []
    async_close = []
    from unittest.mock import AsyncMock
    for endpoint, service in store._blob_service_clients.items():
        assert service.credential is store.options.credential
        blob = service.get_blob_client("other-container", "blob")
        assert not blob._config.retry_policy.retry_to_secondary
        assert blob.secondary_hostname == endpoint[1].replace(".blob.", "-secondary.blob.")
        service.close = Mock()
        sync_close.append(service.close)
    for endpoint, service in store._async_blob_service_clients.items():
        assert service.credential is store.options.credential
        blob = service.get_blob_client("other-container", "blob")
        assert not blob._config.retry_policy.retry_to_secondary
        assert blob.secondary_hostname == endpoint[1].replace(".blob.", "-secondary.blob.")
        service.close = AsyncMock()
        async_close.append(service.close)
    store.close()
    asyncio.run(store.close_async())
    for close in sync_close:
        close.assert_called_once()
    for close in async_close:
        close.assert_awaited_once()
