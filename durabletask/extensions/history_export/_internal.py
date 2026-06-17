# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Package-internal helpers shared across the history-export modules.

Names here have no leading underscore so they can be imported by sibling
modules without tripping pyright's ``reportPrivateUsage`` check.  They
remain package-private by convention: nothing in this module is exported
from :mod:`durabletask.extensions.history_export.__init__`.
"""

from __future__ import annotations

from datetime import datetime, timezone


def dt_to_iso(value: datetime | None) -> str | None:
    """Normalize *value* to a UTC ISO-8601 string (or ``None``)."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat()


def dt_from_iso(value: str | None) -> datetime | None:
    """Parse *value* as an ISO-8601 timestamp, defaulting naive values to UTC."""
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


__all__ = ["dt_from_iso", "dt_to_iso"]
