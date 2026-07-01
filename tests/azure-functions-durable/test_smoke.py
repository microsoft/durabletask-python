# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import azure.durable_functions as df


def test_public_api_is_importable():
    """Smoke test: the package imports and exposes its public API.

    This is a no-op placeholder establishing the unit-test structure for the
    azure-functions-durable module. Real unit tests should be added alongside
    it; integration tests that require Azurite or the Azure Functions host
    emulator should be marked (e.g. ``azurite``) so they can be excluded on
    the network-isolated ADO build pool.
    """
    assert df.version
    assert df.DFApp is not None
    assert df.Blueprint is not None
    assert df.DurableFunctionsClient is not None
    assert df.Orchestrator is not None
