# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Azure Functions binding converters for the durabletask-based runtime.

Historically the ``azure-functions`` SDK shipped and auto-registered the Durable
Functions binding converters itself, detecting the installed
``azure-durable_functions`` package version to decide which set to use. The SDK
now exposes :func:`azure.functions.register_converter`, letting this package own
and register the converters it needs instead. :func:`register_durable_converters`
installs them, overriding the SDK's built-in defaults for the four durable
binding types.

These converters differ from the SDK defaults in the ways the durabletask-based
runtime requires:

* the orchestration and entity triggers encode their result as ``string`` -- the
  base64-encoded protobuf response the Durable Functions host expects from this
  runtime -- rather than ``json``; and
* the durable client binding decodes to a
  :class:`~azure.durable_functions.client.DurableFunctionsClient` instance rather
  than the raw configuration string.
"""

from __future__ import annotations

import json
from typing import Any, Mapping, Optional

import azure.functions as func
from azure.functions import meta
from azure.functions._durable_functions import (
    EntityContext,
    OrchestrationContext,
    df_dumps,
    df_loads,
)

from ..constants import (
    ACTIVITY_TRIGGER,
    DURABLE_CLIENT,
    ENTITY_TRIGGER,
    ORCHESTRATION_TRIGGER,
)

_TriggerMetadata = Optional[Mapping[str, meta.Datum]]


class OrchestrationTriggerConverter(meta.InConverter,
                                    meta.OutConverter,
                                    binding=None,
                                    trigger=True):  # pyright: ignore[reportArgumentType]
    @classmethod
    def check_input_type_annotation(cls, pytype: type) -> bool:
        return issubclass(pytype, OrchestrationContext)

    @classmethod
    def check_output_type_annotation(cls, pytype: type) -> bool:
        # Implicit output should accept any return type
        return True

    @classmethod
    def decode(cls, data: meta.Datum, *,
               trigger_metadata: _TriggerMetadata) -> OrchestrationContext:
        return OrchestrationContext(data.value)

    @classmethod
    def encode(cls, obj: Any, *,
               expected_type: Optional[type]) -> meta.Datum:
        # The durabletask worker returns the base64-encoded protobuf response
        # the host expects, so it is emitted as a plain string.
        return meta.Datum(type='string', value=obj)

    @classmethod
    def has_implicit_output(cls) -> bool:
        return True


class EntityTriggerConverter(meta.InConverter,
                             meta.OutConverter,
                             binding=None,
                             trigger=True):  # pyright: ignore[reportArgumentType]
    @classmethod
    def check_input_type_annotation(cls, pytype: type) -> bool:
        return issubclass(pytype, EntityContext)

    @classmethod
    def check_output_type_annotation(cls, pytype: type) -> bool:
        # Implicit output should accept any return type
        return True

    @classmethod
    def decode(cls, data: meta.Datum, *,
               trigger_metadata: _TriggerMetadata) -> EntityContext:
        return EntityContext(data.value)

    @classmethod
    def encode(cls, obj: Any, *,
               expected_type: Optional[type]) -> meta.Datum:
        # The durabletask worker returns the base64-encoded protobuf response
        # the host expects, so it is emitted as a plain string.
        return meta.Datum(type='string', value=obj)

    @classmethod
    def has_implicit_output(cls) -> bool:
        return True


class ActivityTriggerConverter(meta.InConverter,
                               meta.OutConverter,
                               binding=None,
                               trigger=True):  # pyright: ignore[reportArgumentType]
    @classmethod
    def check_input_type_annotation(cls, pytype: type) -> bool:
        # Activity Trigger's arguments should accept any types
        return True

    @classmethod
    def check_output_type_annotation(cls, pytype: type) -> bool:
        # The activity trigger should accept any JSON serializable types
        return True

    @classmethod
    def decode(cls, data: meta.Datum, *,
               trigger_metadata: _TriggerMetadata) -> Any:
        data_type = data.type

        # Durable functions extension always returns a string of json.
        # See the durable functions library's call_activity_task docs.
        #
        # Strict-mode caveat: when the AZURE_FUNCTIONS_DURABLE_STRICT_TYPING
        # environment variable is set, df_loads requires an ``expected_type`` to
        # deserialize custom-object envelopes. The worker's converter dispatch
        # does not forward the activity function's parameter type annotation to
        # ``decode``, so there is nothing to pass here -- a strict-mode payload
        # carrying a custom-object envelope surfaces as TypeError below and is
        # re-raised as ValueError.
        if data_type in ['string', 'json']:
            try:
                result = df_loads(data.value)
            except json.JSONDecodeError:
                # String failover if the content is not json serializable
                result = data.value
            except Exception as e:
                raise ValueError(
                    'activity trigger input must be a string or a '
                    f'valid json serializable ({data.value})') from e
        else:
            raise NotImplementedError(
                f'unsupported activity trigger payload type: {data_type}')

        return result

    @classmethod
    def encode(cls, obj: Any, *,
               expected_type: Optional[type]) -> meta.Datum:
        try:
            result = df_dumps(obj)
        except TypeError as e:
            raise ValueError(
                f'activity trigger output must be json serializable ({obj})') from e

        return meta.Datum(type='json', value=result)

    @classmethod
    def has_implicit_output(cls) -> bool:
        return True


class DurableClientConverter(meta.InConverter,
                             meta.OutConverter,
                             binding=None):
    @classmethod
    def has_implicit_output(cls) -> bool:
        return False

    @classmethod
    def has_trigger_support(cls) -> bool:
        return False

    @classmethod
    def check_input_type_annotation(cls, pytype: type) -> bool:
        from ..client import DurableFunctionsClient
        return issubclass(pytype, (str, bytes, DurableFunctionsClient))

    @classmethod
    def check_output_type_annotation(cls, pytype: type) -> bool:
        return issubclass(pytype, (str, bytes, bytearray))

    @classmethod
    def encode(cls, obj: Any, *,
               expected_type: Optional[type]) -> meta.Datum:
        if isinstance(obj, str):
            return meta.Datum(type='string', value=obj)

        elif isinstance(obj, (bytes, bytearray)):
            return meta.Datum(type='bytes', value=bytes(obj))
        elif obj is None:
            return meta.Datum(type=None, value=obj)
        elif isinstance(obj, dict):
            return meta.Datum(type='dict', value=obj)
        elif isinstance(obj, list):
            return meta.Datum(type='list', value=obj)
        elif isinstance(obj, bool):
            return meta.Datum(type='bool', value=obj)
        elif isinstance(obj, int):
            return meta.Datum(type='int', value=obj)
        elif isinstance(obj, float):
            return meta.Datum(type='double', value=obj)
        else:
            raise NotImplementedError

    @classmethod
    def decode(cls, data: meta.Datum, *,
               trigger_metadata: _TriggerMetadata) -> Any:
        from ..client import DurableFunctionsClient
        return DurableFunctionsClient(data.value)


def register_durable_converters() -> None:
    """Register this package's durable binding converters with azure-functions.

    Overrides the SDK's built-in converters for the four durable binding types
    so the host uses the durabletask-based encodings and the durable-client
    binding is decoded to a :class:`DurableFunctionsClient`.
    """
    func.register_converter(
        ORCHESTRATION_TRIGGER, OrchestrationTriggerConverter, overwrite=True)
    func.register_converter(
        ENTITY_TRIGGER, EntityTriggerConverter, overwrite=True)
    func.register_converter(
        ACTIVITY_TRIGGER, ActivityTriggerConverter, overwrite=True)
    func.register_converter(
        DURABLE_CLIENT, DurableClientConverter, overwrite=True)
