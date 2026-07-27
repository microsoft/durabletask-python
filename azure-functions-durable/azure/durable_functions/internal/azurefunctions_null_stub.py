# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Any, Callable


class AzureFunctionsNullStub:
    """A task hub sidecar stub whose every method is a no-op.

    Instances structurally satisfy the methods of
    ``ProtoTaskHubSidecarServiceStub`` without inheriting from that
    ``Protocol`` (a ``Protocol`` subclass cannot be instantiated). Any
    attribute access resolves to a callable that ignores its arguments and
    returns ``None``, which is sufficient because the Azure Functions worker
    replaces the relevant completion callbacks before invoking the base
    worker logic.
    """

    def __getattr__(self, name: str) -> Callable[..., None]:
        def _noop(*args: Any, **kwargs: Any) -> None:
            return None

        return _noop
