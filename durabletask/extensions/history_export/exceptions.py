# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Custom exception types raised by the history-export extension.

The hierarchy multiply-inherits from the closest matching built-in
exception so that existing ``except ValueError:`` / ``except
LookupError:`` clauses keep working — the export-specific subclasses
are an additive refinement, not a breaking rename.
"""

from __future__ import annotations

from typing import Optional


class ExportJobError(Exception):
    """Base class for all export-job specific errors."""

    def __init__(self, message: str, *, job_id: Optional[str] = None) -> None:
        super().__init__(message)
        self.job_id = job_id


class ExportJobInvalidTransitionError(ExportJobError, ValueError):
    """Raised when an entity operation would produce an invalid state transition."""

    def __init__(
        self,
        operation: str,
        from_status: Optional[str],
        to_status: Optional[str],
        *,
        job_id: Optional[str] = None,
    ) -> None:
        message = (
            f"Operation {operation!r} cannot transition export job "
            f"{job_id!r} from {from_status!r} to {to_status!r}"
        )
        super().__init__(message, job_id=job_id)
        self.operation = operation
        self.from_status = from_status
        self.to_status = to_status


class ExportJobNotFoundError(ExportJobError, LookupError):
    """Raised when an export job cannot be located by ID."""

    def __init__(self, job_id: str) -> None:
        super().__init__(f"Export job {job_id!r} was not found", job_id=job_id)


__all__ = [
    "ExportJobError",
    "ExportJobInvalidTransitionError",
    "ExportJobNotFoundError",
]
