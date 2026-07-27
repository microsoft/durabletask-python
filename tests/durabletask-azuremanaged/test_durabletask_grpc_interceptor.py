# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import asyncio
import unittest
from concurrent import futures
from datetime import datetime, timedelta, timezone
from importlib.metadata import version
import threading
import time
from typing import Any

import grpc
from azure.core.credentials import AccessToken

from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.internal.access_token_manager import (
    AccessTokenManager,
    AsyncAccessTokenManager,
)
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from durabletask.internal.grpc_interceptor import DefaultClientInterceptorImpl
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.internal import orchestrator_service_pb2_grpc as stubs


class MockTaskHubSidecarServiceServicer(stubs.TaskHubSidecarServiceServicer):
    """Mock implementation of the TaskHubSidecarService for testing."""

    def __init__(self):
        self.captured_metadata = {}
        self.requests_received = 0

    def GetInstance(self, request, context):
        """Implementation of GetInstance that captures the metadata."""
        # Store all metadata key-value pairs from the context
        for key, value in context.invocation_metadata():
            self.captured_metadata[key] = value

        self.requests_received += 1

        # Return a mock response
        response = pb.GetInstanceResponse(exists=False)
        return response


class _TestTokenCredential:
    """Minimal TokenCredential stub that counts how often a token is requested."""

    def __init__(self):
        self._lock = threading.Lock()
        self.calls = 0

    def get_token(self, *scopes: str, **kwargs: Any) -> AccessToken:
        with self._lock:
            self.calls += 1
            call_number = self.calls
        time.sleep(0.02)
        return AccessToken(f"token-{call_number}", int(time.time()) + 3600)


class TestDurableTaskGrpcInterceptor(unittest.TestCase):
    """Tests for the DTSDefaultClientInterceptorImpl class."""

    @classmethod
    def setUpClass(cls):
        # Start a real gRPC server on a free port
        cls.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        cls.port = cls.server.add_insecure_port('[::]:0')  # Bind to a random free port
        cls.server_address = f"localhost:{cls.port}"

        # Add our mock service implementation to the server
        cls.mock_servicer = MockTaskHubSidecarServiceServicer()
        stubs.add_TaskHubSidecarServiceServicer_to_server(cls.mock_servicer, cls.server)

        # Start the server in a background thread
        cls.server.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.stop(grace=None)

    def setUp(self):
        self.mock_servicer.captured_metadata = {}
        self.mock_servicer.requests_received = 0

    def test_user_agent_metadata_passed_in_request(self):
        """Test that the user agent metadata is correctly passed in gRPC requests."""
        # Create a client that connects to our mock server
        # Note: secure_channel is False and token_credential is None as specified
        task_hub_client = DurableTaskSchedulerClient(
            host_address=self.server_address,
            secure_channel=False,
            taskhub="test-taskhub",
            token_credential=None
        )

        # Make a client call that will trigger our interceptor
        task_hub_client.get_orchestration_state("test-instance-id")

        # Verify the request was received by our mock server
        self.assertEqual(1, self.mock_servicer.requests_received, "Expected one request to be received")

        # Check if our custom x-user-agent header was correctly set
        self.assertIn("x-user-agent", self.mock_servicer.captured_metadata, "x-user-agent header not found")

        # Get what we expect our user agent to be
        try:
            expected_version = version('durabletask-azuremanaged')
        except Exception:
            expected_version = "unknown"

        expected_user_agent = f"durabletask-python/{expected_version}"
        self.assertEqual(
            expected_user_agent,
            self.mock_servicer.captured_metadata["x-user-agent"],
            f"Expected x-user-agent header to be '{expected_user_agent}'"
        )

        # Check if the taskhub header was correctly set
        self.assertIn("taskhub", self.mock_servicer.captured_metadata, "taskhub header not found")
        self.assertEqual("test-taskhub", self.mock_servicer.captured_metadata["taskhub"])

        # Verify the standard gRPC user-agent is different from our custom one
        # Note: gRPC automatically adds its own "user-agent" header
        self.assertIn("user-agent", self.mock_servicer.captured_metadata, "gRPC user-agent header not found")
        self.assertNotEqual(
            self.mock_servicer.captured_metadata["user-agent"],
            self.mock_servicer.captured_metadata["x-user-agent"],
            "gRPC user-agent should be different from our custom x-user-agent"
        )
        self.assertNotIn("workerid", self.mock_servicer.captured_metadata)

    def test_custom_interceptor_is_combined_with_dts_interceptor(self):
        custom_interceptor = DefaultClientInterceptorImpl([("x-custom-header", "abc")])
        task_hub_client = DurableTaskSchedulerClient(
            host_address=self.server_address,
            secure_channel=False,
            taskhub="test-taskhub",
            token_credential=None,
            interceptors=[custom_interceptor],
        )

        task_hub_client.get_orchestration_state("test-instance-id")

        self.assertEqual(1, self.mock_servicer.requests_received)
        self.assertEqual("abc", self.mock_servicer.captured_metadata["x-custom-header"])
        self.assertEqual("test-taskhub", self.mock_servicer.captured_metadata["taskhub"])

    def test_worker_includes_workerid_header(self):
        worker = DurableTaskSchedulerWorker(
            host_address=self.server_address,
            secure_channel=False,
            taskhub="test-taskhub",
            token_credential=None,
        )

        interceptor = worker._interceptors[-1]
        metadata = dict(interceptor._metadata)
        self.assertIn("workerid", metadata)
        self.assertTrue(metadata["workerid"])

    def test_client_construction_does_not_acquire_token(self):
        """Token acquisition is deferred from construction to the first request."""
        credential = _TestTokenCredential()

        task_hub_client = DurableTaskSchedulerClient(
            host_address=self.server_address,
            secure_channel=False,
            taskhub="test-taskhub",
            token_credential=credential,
        )

        self.assertEqual(0, credential.calls, "Constructing a client must not acquire a token")

        task_hub_client.get_orchestration_state("test-instance-id")

        self.assertEqual(1, credential.calls, "The first request should acquire exactly one token")
        self.assertIn("authorization", self.mock_servicer.captured_metadata)

        task_hub_client.get_orchestration_state("test-instance-id")

        self.assertEqual(1, credential.calls, "A cached, unexpired token should be reused")
        self.assertEqual(2, self.mock_servicer.requests_received)

    def test_worker_construction_does_not_acquire_token(self):
        """Token acquisition is deferred from worker construction to the first request."""
        credential = _TestTokenCredential()

        DurableTaskSchedulerWorker(
            host_address=self.server_address,
            secure_channel=False,
            taskhub="test-taskhub",
            token_credential=credential,
        )

        self.assertEqual(0, credential.calls, "Constructing a worker must not acquire a token")


class TestAccessTokenManagerThreadSafety(unittest.TestCase):

    @staticmethod
    def _get_access_token_concurrently(manager: AccessTokenManager, thread_count: int = 8) -> None:
        threads = [threading.Thread(target=manager.get_access_token) for _ in range(thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def test_deferred_first_acquisition_performs_single_acquisition(self):
        credential = _TestTokenCredential()
        manager = AccessTokenManager(credential)

        self.assertEqual(0, credential.calls, "Constructing the manager must not acquire a token")

        self._get_access_token_concurrently(manager)

        self.assertEqual(1, credential.calls)
        token = manager.get_access_token()
        self.assertIsNotNone(token)
        self.assertEqual("token-1", token.token)  # type: ignore[union-attr]
        self.assertEqual(1, credential.calls, "A cached, unexpired token should be reused")

    def test_concurrent_refresh_performs_single_refresh(self):
        credential = _TestTokenCredential()
        manager = AccessTokenManager(credential)
        manager.get_access_token()
        self.assertEqual(1, credential.calls)

        manager.expiry_time = datetime.now(timezone.utc) - timedelta(seconds=1)

        self._get_access_token_concurrently(manager)

        self.assertEqual(2, credential.calls)


class _CredentialError(Exception):
    pass


class _TestAsyncTokenCredential:
    def __init__(self, delay: float = 0.02):
        self.calls = 0
        self._delay = delay

    async def get_token(self, _scope):
        # Counting before the await means every coroutine that reaches the credential
        # is recorded, which is what makes a thundering herd observable.
        self.calls += 1
        call_number = self.calls
        await asyncio.sleep(self._delay)
        return AccessToken(f"token-{call_number}", int(time.time()) + 3600)


class _FlakyAsyncTokenCredential:
    def __init__(self):
        self.calls = 0
        self.fail = True

    async def get_token(self, _scope):
        self.calls += 1
        await asyncio.sleep(0)
        if self.fail:
            raise _CredentialError("credential unavailable")
        return AccessToken("token-recovered", int(time.time()) + 3600)


class TestAsyncAccessTokenManagerSingleFlight(unittest.IsolatedAsyncioTestCase):
    async def test_deferred_first_acquisition_performs_single_acquisition(self):
        credential = _TestAsyncTokenCredential()
        manager = AsyncAccessTokenManager(credential)

        self.assertEqual(0, credential.calls, "Constructing the manager must not acquire a token")

        # The async manager has always deferred the first acquisition (a constructor
        # cannot await), so the cold-start burst goes through the same refresh guard
        # as a later refresh and must likewise be single-flight.
        tokens = await asyncio.gather(*(manager.get_access_token() for _ in range(16)))

        self.assertEqual(1, credential.calls)
        self.assertEqual({"token-1"}, {token.token for token in tokens})

    async def test_concurrent_refresh_performs_single_refresh(self):
        credential = _TestAsyncTokenCredential()
        manager = AsyncAccessTokenManager(credential)
        await manager.get_access_token()
        self.assertEqual(1, credential.calls)

        manager.expiry_time = datetime.now(timezone.utc) - timedelta(seconds=1)

        tokens = await asyncio.gather(*(manager.get_access_token() for _ in range(16)))

        self.assertEqual(2, credential.calls)
        self.assertEqual({"token-2"}, {token.token for token in tokens})

    async def test_valid_token_is_returned_without_contacting_credential(self):
        credential = _TestAsyncTokenCredential()
        manager = AsyncAccessTokenManager(credential)

        first = await manager.get_access_token()
        second = await manager.get_access_token()

        self.assertEqual(1, credential.calls)
        self.assertIs(first, second)

    async def test_credential_failure_propagates_and_does_not_deadlock(self):
        credential = _FlakyAsyncTokenCredential()
        manager = AsyncAccessTokenManager(credential)

        results = await asyncio.gather(
            *(manager.get_access_token() for _ in range(4)), return_exceptions=True)

        self.assertTrue(all(isinstance(result, _CredentialError) for result in results))

        # The guard must have been released on failure so a later attempt can succeed.
        credential.fail = False
        token = await manager.get_access_token()
        self.assertIsNotNone(token)
        self.assertEqual("token-recovered", token.token)


class TestAsyncAccessTokenManagerEventLoopBinding(unittest.TestCase):
    def test_manager_is_reusable_across_event_loops(self):
        credential = _TestAsyncTokenCredential(delay=0)
        manager = AsyncAccessTokenManager(credential)

        first = asyncio.run(manager.get_access_token())

        manager.expiry_time = datetime.now(timezone.utc) - timedelta(seconds=1)

        # The second run uses a distinct event loop; the refresh guard must not stay
        # bound to the first, now-closed loop.
        second = asyncio.run(manager.get_access_token())

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertEqual(2, credential.calls)


if __name__ == "__main__":
    unittest.main()
