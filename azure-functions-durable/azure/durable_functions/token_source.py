# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from abc import ABC

from typing_extensions import deprecated


class TokenSource(ABC):
    """Token source abstract base class.

    Backwards-compatible shim for the v1 ``TokenSource``. Token sources are
    consumed only by the orchestrator ``call_http`` API, which has no
    durabletask equivalent yet — see
    :meth:`DurableOrchestrationContext.call_http`. Constructing a token source
    is harmless, but it cannot be used until ``call_http`` is supported.
    """

    def __init__(self):
        super().__init__()


@deprecated(
    "ManagedIdentityTokenSource is deprecated; it is only usable with the "
    "orchestrator call_http API, which is not yet available in durabletask.")
class ManagedIdentityTokenSource(TokenSource):
    """Returns a ``ManagedIdentityTokenSource`` object.

    Only meaningful when passed to ``call_http`` (not yet supported in
    durabletask). Constructing one is allowed for import/config compatibility.
    """

    def __init__(self, resource: str):
        """Create a ManagedIdentityTokenSource.

        Args:
            resource (str): The Azure Active Directory resource identifier of the
                web API being invoked.
        """
        super().__init__()
        self._resource: str = resource
        self._kind: str = "AzureManagedIdentity"

    @property
    def resource(self) -> str:
        """Get the Azure Active Directory resource identifier of the web API being invoked."""
        return self._resource

    def to_json(self) -> dict[str, str]:
        """Convert this object into a JSON-serializable dictionary."""
        return {"resource": self._resource, "kind": self._kind}
