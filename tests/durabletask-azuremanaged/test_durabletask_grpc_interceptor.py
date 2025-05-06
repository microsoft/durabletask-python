# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import threading
import unittest
from concurrent import futures
from importlib.metadata import version

import grpc

from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.internal.durabletask_grpc_interceptor import (
    DTSDefaultClientInterceptorImpl,
)
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


if __name__ == "__main__":
    unittest.main()
