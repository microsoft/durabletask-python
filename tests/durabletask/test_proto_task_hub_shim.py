# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.


import ast
import inspect
from typing import get_type_hints

from durabletask.internal.orchestrator_service_pb2_grpc import TaskHubSidecarServiceStub
from durabletask.internal.proto_task_hub_sidecar_service_stub import ProtoTaskHubSidecarServiceStub


def test_proto_task_hub_shim_is_compatible():
    """Test that ProtoTaskHubSidecarServiceStub is compatible with TaskHubSidecarServiceStub."""
    protocol_attrs = set(get_type_hints(ProtoTaskHubSidecarServiceStub).keys())

    # Instantiate TaskHubSidecarServiceStub with a dummy channel to get its attributes
    class TestChannel():
        def unary_unary(self, *args, **kwargs):
            pass

        def unary_stream(self, *args, **kwargs):
            pass
    impl_attrs = TaskHubSidecarServiceStub(TestChannel()).__dict__.keys()

    # Check missing
    assert protocol_attrs == impl_attrs
