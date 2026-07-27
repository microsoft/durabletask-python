# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the durable binding converters and their registration.

The ``azure-functions`` SDK exposes ``register_converter`` so this package can
own and register the Durable Functions binding converters. These tests verify
that importing the package installs our converters over the SDK defaults, and
that the converters use the durabletask-based encodings the host expects.
"""

from unittest.mock import patch

import json

from azure.functions import meta
from azure.functions.meta import get_binding_registry

import azure.durable_functions  # noqa: F401 - import triggers registration
from azure.durable_functions.constants import (
    ACTIVITY_TRIGGER,
    DURABLE_CLIENT,
    ENTITY_TRIGGER,
    ORCHESTRATION_TRIGGER,
)
from azure.durable_functions.internal.converters import (
    ActivityTriggerConverter,
    DurableClientConverter,
    EntityTriggerConverter,
    OrchestrationTriggerConverter,
    register_durable_converters,
)


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def test_import_registers_our_converters_over_sdk_defaults():
    registry = get_binding_registry()
    assert registry.get(ORCHESTRATION_TRIGGER) is OrchestrationTriggerConverter
    assert registry.get(ENTITY_TRIGGER) is EntityTriggerConverter
    assert registry.get(ACTIVITY_TRIGGER) is ActivityTriggerConverter
    assert registry.get(DURABLE_CLIENT) is DurableClientConverter


def test_register_durable_converters_is_idempotent():
    # A second registration must not raise (it passes overwrite=True).
    register_durable_converters()
    registry = get_binding_registry()
    assert registry.get(ORCHESTRATION_TRIGGER) is OrchestrationTriggerConverter
    assert registry.get(DURABLE_CLIENT) is DurableClientConverter


# ---------------------------------------------------------------------------
# Orchestration / entity triggers
# ---------------------------------------------------------------------------

def test_orchestration_trigger_encodes_result_as_string():
    datum = OrchestrationTriggerConverter.encode("base64response", expected_type=None)
    assert datum.type == "string"
    assert datum.value == "base64response"


def test_entity_trigger_encodes_result_as_string():
    datum = EntityTriggerConverter.encode("base64response", expected_type=None)
    assert datum.type == "string"
    assert datum.value == "base64response"


def test_orchestration_trigger_decodes_to_context_wrapping_body():
    ctx = OrchestrationTriggerConverter.decode(
        meta.Datum(type="string", value="the-body"), trigger_metadata=None)
    assert ctx.body == "the-body"


def test_entity_trigger_decodes_to_context_wrapping_body():
    ctx = EntityTriggerConverter.decode(
        meta.Datum(type="string", value="the-body"), trigger_metadata=None)
    assert ctx.body == "the-body"


def test_triggers_have_implicit_output_and_trigger_support():
    for conv in (OrchestrationTriggerConverter, EntityTriggerConverter,
                 ActivityTriggerConverter):
        assert conv.has_implicit_output() is True
        assert conv.has_trigger_support() is True


# ---------------------------------------------------------------------------
# Activity trigger
# ---------------------------------------------------------------------------

def test_activity_trigger_round_trips_json_payload():
    payload = {"a": 1, "b": ["x", "y"]}
    encoded = ActivityTriggerConverter.encode(payload, expected_type=None)
    assert encoded.type == "json"
    decoded = ActivityTriggerConverter.decode(encoded, trigger_metadata=None)
    assert decoded == payload


def test_activity_trigger_decode_falls_back_to_raw_string():
    decoded = ActivityTriggerConverter.decode(
        meta.Datum(type="string", value="not-json"), trigger_metadata=None)
    assert decoded == "not-json"


# ---------------------------------------------------------------------------
# Durable client
# ---------------------------------------------------------------------------

def test_durable_client_accepts_client_and_string_annotations():
    from azure.durable_functions.client import DurableFunctionsClient
    assert DurableClientConverter.check_input_type_annotation(DurableFunctionsClient)
    assert DurableClientConverter.check_input_type_annotation(str)
    assert DurableClientConverter.check_input_type_annotation(bytes)
    assert not DurableClientConverter.check_input_type_annotation(int)


def test_durable_client_has_no_trigger_support_or_implicit_output():
    assert DurableClientConverter.has_trigger_support() is False
    assert DurableClientConverter.has_implicit_output() is False


def test_durable_client_decode_constructs_client_from_value():
    with patch(
        "azure.durable_functions.client.DurableFunctionsClient"
    ) as mock_client:
        result = DurableClientConverter.decode(
            meta.Datum(type="string", value="client-config"),
            trigger_metadata=None)
    mock_client.assert_called_once_with("client-config")
    assert result is mock_client.return_value


async def test_durable_client_decode_builds_working_client_from_config():
    # End-to-end: decode parses a host-style configuration string into a live
    # DurableFunctionsClient (the construction that previously lived in the
    # decorator's client middleware).
    from azure.durable_functions.client import DurableFunctionsClient

    config = json.dumps({
        "taskHubName": "TestHub",
        "requiredQueryStringParameters": "code=xyz",
        "baseUrl": "http://localhost:7071/runtime/webhooks/durabletask",
        "rpcBaseUrl": "http://localhost:8080/",
    })

    client = DurableClientConverter.decode(
        meta.Datum(type="string", value=config), trigger_metadata=None)
    assert isinstance(client, DurableFunctionsClient)
    try:
        assert client.taskHubName == "TestHub"
    finally:
        await client.close()
