# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Iterable, Optional


def normalize_required(value: Optional[str], message: str) -> str:
    if not value or not value.strip():
        raise ValueError(message)
    return value.strip()


def resolve_activity_names(activity_names: str | Iterable[str]) -> list[str]:
    resolved: list[str] = []
    seen: set[str] = set()
    names = [activity_names] if isinstance(activity_names, str) else activity_names
    for name in names:
        normalized = name.strip()
        if normalized and normalized not in seen:
            resolved.append(normalized)
            seen.add(normalized)
    return resolved
