# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from abc import ABC, abstractmethod


class TokenSource(ABC):
    """Token source abstract base class.

    A token source supplies an OAuth token that is attached to the request made
    by the orchestrator ``call_http`` API. See
    :meth:`DurableOrchestrationContext.call_http`.
    """

    def __init__(self):
        super().__init__()

    @abstractmethod
    def to_json(self) -> dict[str, str]:
        """Convert this token source into a JSON-serializable dictionary."""
        pass


class ManagedIdentityTokenSource(TokenSource):
    """Returns a ``ManagedIdentityTokenSource`` object.

    Pass an instance to ``call_http`` to have a Managed Identity bearer token
    for the given ``resource`` attached to the outbound request.
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
