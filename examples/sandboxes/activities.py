# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from durabletask.azuremanaged.preview.sandboxes import SandboxActivity

# Python orchestrations currently call activities by name, so this sample uses
# an unversioned sandbox activity identity.
REMOTE_HELLO = SandboxActivity(name="remote_hello", version=None)
