# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Per-invocation lifecycle management for durable-client bindings.

Every ``durable_client_input`` decode builds a fresh
:class:`~azure.durable_functions.client.DurableFunctionsClient` that owns a
distinct async gRPC channel (see
:class:`~azure.durable_functions.internal.converters.DurableClientConverter`).
Nothing else closes that channel, so without a post-invocation hook every
invocation leaks one. The Azure Functions worker exposes an app-level
post-invocation extension hook for exactly this kind of cleanup; this module
registers one that closes each durable client the invocation was given.

The extension registers itself at import time via ``ExtensionMeta`` (the
metaclass behind :class:`azure.functions.AppExtensionBase`), so importing this
module is what installs the hook -- mirroring how importing the converters
module installs the binding converters.

> [!NOTE]
> The hook closes the async ``DurableFunctionsClient``. When a first-class
> synchronous durable-client binding is added (see
> https://github.com/microsoft/durabletask-python/issues/181), its client will
> also need closing here; a synchronous client's ``close()`` can be called
> inline rather than scheduled on an event loop.
"""

from __future__ import annotations

from logging import Logger
from typing import Any, Optional

from azure.functions import AppExtensionBase, Context

from ..client import DurableFunctionsClient


class _DurableClientLifecycleExtension(AppExtensionBase):
    """App-level extension that closes per-invocation durable-client channels."""

    @staticmethod
    def post_invocation_app_level(
            logger: Logger,
            context: Context,
            func_args: Optional[dict[str, object]] = None,
            func_ret: Optional[object] = None,
            *args: Any,
            **kwargs: Any) -> None:
        """Close any durable clients that were injected into the invocation.

        ``func_args`` maps each binding parameter name to its decoded value, so
        a durable-client input surfaces here as a ``DurableFunctionsClient``.
        Each such client owns its own channel and is closed exactly once.
        """
        for value in (func_args or {}).values():
            if isinstance(value, DurableFunctionsClient):
                value.schedule_close()


def register_durable_client_lifecycle() -> None:
    """Ensure the durable-client lifecycle extension is registered.

    Defining :class:`_DurableClientLifecycleExtension` above already registers
    it with the worker via ``ExtensionMeta`` at import time. This function
    exists so that registration is an explicit, discoverable step from the
    package ``__init__`` (and so linters do not flag the class as unused).
    """
    # The registration side effect happens on import; nothing more to do.
    _ = _DurableClientLifecycleExtension
