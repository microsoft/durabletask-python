# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""GZIP interoperability through real Azure SDK HTTP transports."""

import asyncio
import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest

pytest.importorskip("azure.storage.blob")

from durabletask.extensions.azure_blob_payloads import BlobPayloadStore, BlobPayloadStoreOptions  # noqa: E402


@pytest.fixture
def blob_server():
    blobs = {}
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_PUT(self):
            requests.append((self.command, self.path))
            body = self.rfile.read(int(self.headers["Content-Length"]))
            blobs[self.path] = body, self.headers.get("x-ms-blob-content-encoding", "")
            self.send_response(201)
            self.send_header("Content-Length", "0")
            self.send_header("ETag", '"synthetic"')
            self.end_headers()

        def do_GET(self):
            requests.append((self.command, self.path))
            body, encoding = blobs[self.path]
            self.send_response(206)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Range", f"bytes 0-{len(body) - 1}/{len(body)}")
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("x-ms-blob-type", "BlockBlob")
            self.send_header("ETag", '"synthetic"')
            self.send_header("Last-Modified", "Tue, 03 Nov 2026 19:00:00 GMT")
            if encoding:
                self.send_header("Content-Encoding", encoding)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/account", blobs, requests
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


@pytest.fixture
def compression_store(blob_server):
    endpoint, _, _ = blob_server
    store = BlobPayloadStore(BlobPayloadStoreOptions(account_url=endpoint))
    store._ensure_container_created = True
    try:
        yield store
    finally:
        store.close()
        asyncio.run(store.close_async())


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("compression", [False, True])
@pytest.mark.parametrize("encoding", ["", "gzip", "GZip"])
async def test_real_transport_keeps_gzip_decoding_explicit(
        blob_server, compression_store, use_async, compression, encoding):
    endpoint, blobs, requests = blob_server
    store = compression_store
    store.options.enable_compression = compression
    payload = b'{"value":"' + b"payload" * 100 + b'"}'
    path = "/account/durabletask-payloads/blob"
    blobs[path] = (gzip.compress(payload) if compression or encoding else payload), encoding
    for token in ("blob:v1:durabletask-payloads:blob", f"blob:v2:{endpoint}/durabletask-payloads/blob"):
        actual = await store.download_async(token) if use_async else store.download(token)
        assert actual == payload
    assert requests == [("GET", path), ("GET", path)]


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("compression", [False, True])
async def test_real_transport_uploads_publish_gzip_encoding(
        blob_server, compression_store, use_async, compression):
    _, blobs, requests = blob_server
    store = compression_store
    store.options.enable_compression = compression
    payload = b'{"value":"' + b"payload" * 100 + b'"}'
    token = await store.upload_async(payload) if use_async else store.upload(payload)
    path = requests[0][1]
    body, encoding = blobs[path]
    assert encoding == ("gzip" if compression else "")
    assert (gzip.decompress(body) if encoding == "gzip" else body) == payload
    store.options.enable_compression = False
    actual = await store.download_async(token) if use_async else store.download(token)
    assert actual == payload
    assert requests == [("PUT", path), ("GET", path)]


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("compression", [False, True])
async def test_invalid_marked_gzip_does_not_fall_back_to_plain(
        blob_server, compression_store, use_async, compression):
    _, blobs, requests = blob_server
    blobs["/account/durabletask-payloads/blob"] = b"invalid gzip", "gzip"
    store = compression_store
    store.options.enable_compression = compression
    with pytest.raises(gzip.BadGzipFile):
        if use_async:
            await store.download_async("blob:v1:durabletask-payloads:blob")
        else:
            store.download("blob:v1:durabletask-payloads:blob")
    assert requests == [("GET", "/account/durabletask-payloads/blob")]
