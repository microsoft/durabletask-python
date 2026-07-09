# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from dataclasses import dataclass
from typing import Iterable, Optional


@dataclass(frozen=True)
class SandboxActivity:
    name: str
    version: Optional[str] = None


def normalize_required(value: Optional[str], message: str) -> str:
    if not value or not value.strip():
        raise ValueError(message)
    return value.strip()


def normalize_optional(value: Optional[str]) -> Optional[str]:
    if not value or not value.strip():
        return None
    return value.strip()


def resolve_activities(
        activities: Iterable[SandboxActivity]) -> list[SandboxActivity]:
    resolved: list[SandboxActivity] = []
    seen: set[tuple[str, Optional[str]]] = set()
    for activity in activities:
        normalized_name = activity.name.strip()
        if not normalized_name:
            continue

        normalized = SandboxActivity(
            name=normalized_name,
            version=normalize_optional(activity.version))
        key = (normalized.name.casefold(), normalized.version)
        if key not in seen:
            resolved.append(normalized)
            seen.add(key)
    return resolved


def activities_overlap(
        left: SandboxActivity,
        right: SandboxActivity) -> bool:
    return (
        left.name.casefold() == right.name.casefold()
        and (left.version is None or right.version is None or left.version == right.version))


def format_activity(activity: SandboxActivity) -> str:
    return activity.name if activity.version is None else f"{activity.name}@{activity.version}"
