# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Types for enriching task failure details."""

from collections.abc import Mapping
from typing import Any, Protocol


class ExceptionPropertiesProvider(Protocol):
    """Extract portable custom properties from an exception."""

    def get_exception_properties(self, exception: Exception) -> Mapping[str, Any] | None:
        """Return properties to include in the exception's failure details."""
        ...
