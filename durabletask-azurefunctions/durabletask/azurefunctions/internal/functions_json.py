# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
from azure.functions._durable_functions import _serialize_custom_object, _deserialize_custom_object
from durabletask.internal import shared


shared.to_json = lambda obj: json.dumps(obj, default=_serialize_custom_object)
shared.from_json = lambda json_str: json.loads(json_str, object_hook=_deserialize_custom_object)